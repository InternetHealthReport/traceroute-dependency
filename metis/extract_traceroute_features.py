import argparse
import configparser
import json
import logging
import os
import sys
from datetime import datetime, timezone

import lz4.frame
from confluent_kafka import OFFSET_BEGINNING, OFFSET_END

from iplookup.ip_lookup import IPLookup
from kafka_wrapper.kafka_reader import KafkaReader
from kafka_wrapper.kafka_writer import KafkaWriter
from utils import atlas_api_helper
from utils.helper_functions import parse_timestamp_argument

DATE_FMT = '%Y-%m-%dT%H:%M'
INPUT_FILE_ENDING = '.json.lz4'


def check_config(config_path: str) -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config.read(config_path)
    try:
        config.get('input', 'mode')
        config.get('output', 'kafka_topic')
        config.getint('kafka', 'retention_ms')
        if config.get('input', 'mode') == 'kafka':
            config.get('input', 'kafka_topic')
    except configparser.NoSectionError as e:
        logging.error(f'Missing section in config file: {e}')
        return configparser.ConfigParser()
    except configparser.NoOptionError as e:
        logging.error(f'Missing option in config file: {e}')
        return configparser.ConfigParser()
    return config


def generate_msg_from_file(msg_source: str,
                           start_ts: int,
                           stop_ts: int) -> dict:
    logging.info(f'Reading messages from file: {msg_source}')
    try:
        with lz4.frame.open(msg_source, 'r') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        logging.error(f'Failed to load input file: {e}')
        return
    in_order = [(msg['timestamp'], msg) for msg in data]
    in_order.sort(key=lambda t: t[0])
    for timestamp, msg in in_order:
        if start_ts != OFFSET_BEGINNING and timestamp < start_ts:
            continue
        if stop_ts != OFFSET_END and timestamp >= stop_ts:
            break
        yield msg


def msg_generator(msg_source, mode: str, start_ts: int, stop_ts: int) -> dict:
    if mode == 'file':
        if not isinstance(msg_source, str):
            logging.error(f'File-input mode requires path to file but {type(msg_source)} was provided.')
            return
        for msg in generate_msg_from_file(msg_source, start_ts, stop_ts):
            yield msg
    elif mode == 'file_list':
        if not isinstance(msg_source, str):
            logging.error(f'File-input mode requires path to file but {type(msg_source)} was provided.')
            return
        with open(msg_source, 'r') as f:
            for input_file in f:
                for msg in generate_msg_from_file(input_file.strip(),
                                                  start_ts,
                                                  stop_ts):
                    yield msg
    elif mode == 'kafka':
        if not isinstance(msg_source, KafkaReader):
            logging.error(f'"kafka" input mode requires KafkaReader message source, but {type(msg_source)} was '
                          'provided.')
            return
        with msg_source:
            for msg in msg_source.read():
                yield msg


def process_hop(hop: dict, seen_ips: set, lookup: IPLookup) -> dict:
    if 'error' in hop or 'hop' not in hop or hop['hop'] == 255:
        # Packet send failed or end of traceroute reached.
        return
    replies = hop['result']
    reply_addresses = set()
    for reply in replies:
        if 'error' in reply:
            # This seems to be a bug that happens if sending the packet only fails for
            # _some_ of the probes for a single hop.
            # Should be treated the same as 'error' in hop.
            continue
        if ('err' in reply
                and 'from' in reply
                and reply['from'] in seen_ips):
            # Reply received with ICMP error (e.g., network unreachable).
            # We allow this reply once, if the IP was not seen before.
            continue
        # Timeout or no reply address
        if 'x' in reply or 'from' not in reply:
            continue
        address = reply['from']
        # Result already seen for this hop
        if address in reply_addresses:
            continue
        reply_addresses.add(address)
        if address not in seen_ips:
            seen_ips.add(address)
        asn = lookup.ip2asn(address)
        reply_hop = {'hop': hop['hop'],
                     'ip': address,
                     'asn': asn}
        if 'rtt' in reply:
            reply_hop['rtt'] = reply['rtt']
        if 'err' in reply:
            reply_hop['err'] = reply['err']
        yield reply_hop
        ixp = lookup.ip2ixpid(address)
        if ixp != 0:
            ixp_hop = reply_hop.copy()
            # We represent IXPs with negative "AS numbers".
            ixp_hop['asn'] = str(ixp * -1)
            yield ixp_hop


def process_message(msg: dict, lookup: IPLookup) -> dict:
    dst_addr = atlas_api_helper.get_dst_addr(msg)
    if not dst_addr:
        return dict()
    dst_asn = lookup.ip2asn(dst_addr)
    if dst_asn == 0:
        return dict()
    if 'from' not in msg or not msg['from']:
        return dict()
    peer_asn = lookup.ip2asn(msg['from'])
    if peer_asn == 0:
        return dict()
    hops = msg['result']
    seen_ips = set()
    reply_hops = [reply_hop
                  for hop in hops
                  for reply_hop in process_hop(hop, seen_ips, lookup)]
    if not reply_hops:
        return dict()
    return {'prb_id': msg['prb_id'],
            'timestamp': msg['timestamp'],
            'src_ip': msg['from'],
            'src_asn': peer_asn,
            'dst_ip': dst_addr,
            'dst_asn': dst_asn,
            'hops': reply_hops}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('config')
    parser.add_argument('-s', '--start',
                        help='start timestamp (as UNIX epoch in seconds or milliseconds, or in YYYY-MM-DDThh:mm '
                             'format)')
    parser.add_argument('-e', '--stop',
                        help='stop timestamp (as UNIX epoch in seconds or milliseconds, or in YYYY-MM-DDThh:mm format)')
    parser.add_argument('-i', '--input',
                        help='specify input file if file-input mode is used')
    parser.add_argument('--input-list',
                        help='specify list of input files if file-input mode is used')
    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        filename='log/extract_traceroute_features.log',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logging.info(f'Started: {sys.argv}')

    args = parser.parse_args()

    config = configparser.ConfigParser()
    config.read(args.config)
    start_ts_argument = args.start
    start_ts = OFFSET_BEGINNING
    if start_ts_argument:
        start_ts = parse_timestamp_argument(start_ts_argument)
        if start_ts == 0:
            logging.error(f'Invalid start time specified: {start_ts_argument}')
            sys.exit(1)
    logging.info(f'Start timestamp: '
                 f'{datetime.fromtimestamp(start_ts, tz=timezone.utc).strftime(DATE_FMT)} {start_ts}')

    stop_ts_argument = args.stop
    stop_ts = OFFSET_END
    if stop_ts_argument:
        stop_ts = parse_timestamp_argument(stop_ts_argument)
        if stop_ts == 0:
            logging.error(f'Invalid stop time specified: {stop_ts_argument}')
            sys.exit(1)
    logging.info(f'Stop timestamp: '
                 f'{datetime.fromtimestamp(stop_ts, tz=timezone.utc).strftime(DATE_FMT)} {stop_ts}')

    bootstrap_servers = os.environ.get('KAFKA_HOST', str())
    if config.has_option('kafka', 'bootstrap_servers'):
        bootstrap_servers = config.get('kafka', 'bootstrap_servers')

    if not bootstrap_servers:
        logging.error('No Kafka bootstrap servers specified.')
        sys.exit(1)

    kafka_topic_retention_ms = config.getint('kafka', 'retention_ms')

    input_mode = config.get('input', 'mode')
    if input_mode == 'file':
        input_file = args.input
        input_file_list = args.input_list
        if input_file and input_file_list:
            logging.error('--input and --input-list arguments are exclusive.')
            sys.exit(1)
        if input_file is None and input_file_list is None:
            logging.error('--input/--input-list argument is required when using file-input mode.')
            sys.exit(1)
        if input_file:
            input_source = input_file
            if not input_source.endswith(INPUT_FILE_ENDING):
                logging.error(f'Expected {INPUT_FILE_ENDING} input file, but got: {input_source}')
                sys.exit(1)
        else:
            input_source = input_file_list
            input_mode = 'file_list'
    elif input_mode == 'kafka':
        input_topic = config.get('input', 'kafka_topic')
        if start_ts != OFFSET_BEGINNING:
            start_ts *= 1000
        if stop_ts != OFFSET_END:
            stop_ts *= 1000
        input_source = KafkaReader([input_topic],
                                   bootstrap_servers,
                                   start_ts,
                                   stop_ts)
    else:
        logging.error(f'Invalid input mode specified: {input_mode}')
        sys.exit(1)

    output_topic = config.get('output', 'kafka_topic')

    lookup = IPLookup(config)
    if not lookup.initialized:
        logging.error('Error during iplookup initialization.')
        sys.exit(1)
    writer = KafkaWriter(output_topic,
                         bootstrap_servers,
                         config={'retention.ms': kafka_topic_retention_ms})
    with writer:
        for msg in msg_generator(input_source, input_mode, start_ts, stop_ts):
            data = process_message(msg, lookup)
            if not data:
                continue
            key = data['prb_id']
            if isinstance(key, int):
                key = key.to_bytes(4, 'big')
            writer.write(key,
                         data,
                         data['timestamp'] * 1000)


if __name__ == '__main__':
    main()
    sys.exit(0)

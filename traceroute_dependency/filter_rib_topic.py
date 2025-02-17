import argparse
import configparser
import logging
import os
import sys
from datetime import datetime, timezone

from confluent_kafka import OFFSET_BEGINNING, OFFSET_END

from kafka_wrapper.kafka_reader import KafkaReader
from kafka_wrapper.kafka_writer import KafkaWriter
from utils.helper_functions import check_key, check_keys, parse_timestamp_argument

DATE_FMT = '%Y-%m-%dT%H:%M'


def parse_kv_csv(option: str) -> dict:
    ret = dict()
    for pair in option.split(','):
        key, value = pair.split(':')
        ret[key.strip()] = value.strip()
    return ret


def parse_csv(option: str) -> list:
    return [entry.strip() for entry in option.split(',')]


def check_config(config_path: str) -> configparser.ConfigParser:
    config = configparser.ConfigParser(converters={'kv_csv': parse_kv_csv,
                                                   'csv': parse_csv})
    config.read(config_path)
    try:
        config.get('input', 'collector')
        config.get('output', 'collector')
        config.getint('kafka', 'retention_ms')
        if config.get('filter', 'path_attributes', fallback=None):
            config.getkv_csv('filter', 'path_attributes')
        if config.get('filter', 'excluded_path_attributes', fallback=None):
            config.getcsv('filter', 'excluded_path_attributes')
    except configparser.NoSectionError as e:
        logging.error(f'Missing section in config file: {e}')
        return configparser.ConfigParser()
    except configparser.NoOptionError as e:
        logging.error(f'Missing option in config file: {e}')
        return configparser.ConfigParser()
    except ValueError as e:
        logging.error(f'Malformed option in config file: {e}')
        return configparser.ConfigParser()
    return config


def handle_filter_arg(arg: str, filter_type: str = str()) -> set:
    if os.path.isfile(arg):
        return read_filter_list(arg, filter_type)
    if not arg.isdigit():
        logging.error(f'{filter_type} filter is neither a file nor a number.')
        return set()
    return {arg}


def read_filter_list(filter_list: str, filter_type: str = str()) -> set:
    logging.info(f'Reading {filter_type} filter from: {filter_list}')
    with open(filter_list, 'r') as f:
        ret = {line.strip() for line in f}
    logging.info(f'Read {len(ret)} entries')
    return ret


def filter_msg(msg: dict,
               path_attributes: dict = None,
               excluded_path_attributes: set = None,
               asn_list: set = None,
               prb_list: set = None) -> (dict, int, int):
    if (check_keys(['rec', 'elements'], msg)
            or check_key('time', msg['rec'])):
        logging.error(f'Missing "rec", "time", or "elements" field in message: {msg}')
        return dict(), -1, None
    ret = msg.copy()
    timestamp = msg['rec']['time']
    prb_id = None
    ret['elements'] = list()
    for element in msg['elements']:
        if (check_keys(['peer_asn', 'fields'], element)
                or check_key('path-attributes', element['fields'])):
            logging.error(f'Missing "peer_asn", "fields" or "path-attributes" field in message: {msg}')
            continue

        peer_asn = element['peer_asn']
        if check_key('prb_id', element):
            logging.warning(f'Missing "prb_id" field in message: {msg}')
        else:
            if prb_id is not None:
                logging.warning(f'Multiple elements with different probe ids in message: {msg}')
            prb_id = element['prb_id']
        if asn_list and peer_asn not in asn_list:
            continue
        if prb_list and prb_id not in prb_list:
            continue

        msg_path_attributes = element['fields']['path-attributes']
        if (excluded_path_attributes
                and excluded_path_attributes.intersection(msg_path_attributes.keys())):
            continue
        if path_attributes:
            filtered = False
            for attribute, value in path_attributes.items():
                if attribute not in msg_path_attributes:
                    filtered = True
                    break
                if str(msg_path_attributes[attribute]) != value:
                    filtered = True
                    break
            if filtered:
                continue
        ret['elements'].append(element)
    if not ret['elements']:
        return dict(), -1, None
    return ret, timestamp, prb_id


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('config')
    parser.add_argument('--asn-filter',
                        help='single ASN or file containing a list of ASNs (takes precedence over config)')
    parser.add_argument('--probe-filter',
                        help='single probe ID or file containing a list of probe IDs (takes precedence over config)')
    read_group_desc = """By using the --start, --end, and --timestamp options, only a
                      specific range or an exact timestamp of TOPIC can be dumped.
                      Either or both range options can be specified, but are exclusive
                      with --timestamp. Timestamps can be specified as UNIX epoch in
                      (milli)seconds or in the format
                      '%Y-%m-%dT%H:%M'."""
    read_group = parser.add_argument_group('Interval specification', description=read_group_desc)
    read_group.add_argument('-st', '--start', help='start timestamp (default: read topic from beginning)')
    read_group.add_argument('-e', '--end', help='end timestamp (default: read topic to the end)')
    read_group.add_argument('-ts', '--timestamp', help='exact timestamp')
    args = parser.parse_args()

    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        filename='log/filter_rib_topic.log',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logging.info(f'Started: {sys.argv}')

    config = check_config(args.config)
    if not config.sections():
        sys.exit(1)

    if (args.start or args.end) and args.timestamp:
        logging.error('Range and exact timestamp arguments are exclusive.')
        sys.exit(1)

    start_ts = OFFSET_BEGINNING
    end_ts = OFFSET_END
    if args.timestamp:
        start_ts = parse_timestamp_argument(args.timestamp) * 1000
        if start_ts == 0:
            logging.error(f'Invalid timestamp specified: {args.timestamp}')
            sys.exit(1)
        end_ts = start_ts + 1
    if args.start:
        start_ts = parse_timestamp_argument(args.start) * 1000
        if start_ts == 0:
            logging.error(f'Invalid start timestamp specified: {args.start}')
            sys.exit(1)
    if args.end:
        end_ts = parse_timestamp_argument(args.end) * 1000
        if end_ts == 0:
            logging.error(f'Invalid end timestamp specified: {args.end}')
            sys.exit(1)
    if start_ts != OFFSET_BEGINNING:
        start_ts_dt = datetime.fromtimestamp(start_ts / 1000, tz=timezone.utc)
        logging.info(f'Start reading at {start_ts_dt.strftime(DATE_FMT)}')
    else:
        logging.info('Start reading at beginning of topic')
    if end_ts != OFFSET_END:
        end_ts_dt = datetime.fromtimestamp(end_ts / 1000, tz=timezone.utc)
        logging.info(f'Stop reading at {end_ts_dt.strftime(DATE_FMT)}')
    else:
        logging.info('Stop reading at end of topic')

    path_attributes = config.getkv_csv('filter', 'path_attributes', fallback=None)

    excluded_path_attributes = None
    if config.get('filter', 'excluded_path_attributes', fallback=None):
        excluded_path_attributes = set(config.getcsv('filter', 'excluded_path_attributes', fallback=None))

    asn_filter = None
    if args.asn_filter:
        asn_filter = handle_filter_arg(args.asn_filter, 'asn')
    elif asn_list := config.get('filter', 'asn_list', fallback=None):
        asn_filter = read_filter_list(asn_list, 'asn')
    if asn_filter is not None and len(asn_filter) == 0:
        logging.error('Empty ASN filter.')
        sys.exit(1)

    prb_filter = None
    if args.probe_filter:
        prb_filter = handle_filter_arg(args.probe_filter, 'probe')
    elif prb_list := config.get('filter', 'prb_list', fallback=None):
        prb_filter = read_filter_list(prb_list, 'probe')
    if prb_filter is not None and len(prb_filter) == 0:
        logging.error('Empty probe filter.')
        sys.exit(1)

    bootstrap_servers = os.environ.get('KAFKA_HOST', str())
    if config.has_option('kafka', 'bootstrap_servers'):
        bootstrap_servers = config.get('kafka', 'bootstrap_servers')

    if not bootstrap_servers:
        logging.error('No Kafka bootstrap servers specified.')
        sys.exit(1)

    kafka_topic_retention_ms = config.getint('kafka', 'retention_ms')

    input_rib_topic = f'ihr_bgp_{config.get("input", "collector")}_ribs'
    output_rib_topic = f'ihr_bgp_{config.get("output", "collector")}_ribs'
    logging.info(f'Reading from RIB topic {input_rib_topic}')
    logging.info(f'Writing to RIB topic {output_rib_topic}')
    rib_reader = KafkaReader([input_rib_topic],
                             bootstrap_servers,
                             start_ts,
                             end_ts)
    rib_writer = KafkaWriter(output_rib_topic,
                             bootstrap_servers,
                             config={'retention.ms': kafka_topic_retention_ms})
    with rib_reader, rib_writer:
        last_ts = -1
        for msg in rib_reader.read():
            filtered_msg, timestamp, prb_id = filter_msg(msg,
                                                         path_attributes,
                                                         excluded_path_attributes,
                                                         asn_filter,
                                                         prb_filter)
            if filtered_msg:
                if last_ts > timestamp:
                    logging.warning(f'Writing out-of-order message: {last_ts} > {timestamp}')
                key = None
                if prb_id is not None:
                    key = prb_id
                    if isinstance(prb_id, int):
                        key = prb_id.to_bytes(4, byteorder='big')
                rib_writer.write(key, filtered_msg, timestamp * 1000)
                last_ts = timestamp

    input_updates_topic = f'ihr_bgp_{config.get("input", "collector")}_updates'
    output_updates_topic = f'ihr_bgp_{config.get("output", "collector")}_updates'
    logging.info(f'Copying messages from updates topic {input_updates_topic} to {output_updates_topic}')
    if end_ts != OFFSET_END:
        # Updates topic message have an incremented timestamp.
        end_ts += 1000
    updates_reader = KafkaReader([input_updates_topic],
                                 bootstrap_servers,
                                 start_ts,
                                 end_ts)
    updates_writer = KafkaWriter(output_updates_topic,
                                 bootstrap_servers,
                                 config={'retention.ms': kafka_topic_retention_ms})
    update_messages = list()
    with updates_reader:
        for msg in updates_reader.read():
            if check_key('rec', msg) or check_key('time', msg['rec']):
                logging.error(f'Missing "rec" or "time" field in msg {msg}')
                continue
            timestamp = msg['rec']['time']
            update_messages.append((timestamp, msg))
    update_messages.sort(key=lambda t: t[0])
    last_ts = -1
    with updates_writer:
        for timestamp, msg in update_messages:
            if last_ts > timestamp:
                logging.warning(f'Writing out-of-order message: {last_ts} > {timestamp}')
            updates_writer.write(None, msg, timestamp * 1000)
            last_ts = timestamp


if __name__ == '__main__':
    main()

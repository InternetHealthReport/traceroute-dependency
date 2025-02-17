import argparse
import configparser
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone

from kafka_wrapper.kafka_reader import KafkaReader
from kafka_wrapper.kafka_writer import KafkaWriter
from metis.shared_extract_functions import (AS_HOPS_FEATURE, IP_HOPS_FEATURE, RTT_FEATURE, VALID_FEATURES, VALID_MODES,
                                            extract_as_hops, extract_ip_hops, extract_rtts)
from utils.helper_functions import parse_timestamp_argument


def parse_csv(value: str) -> list:
    return [entry.strip() for entry in value.split(',')]


def check_config(config_path: str) -> configparser.ConfigParser:
    config = configparser.ConfigParser(converters={'csv': parse_csv})
    config.read(config_path)
    try:
        config.get('input', 'kafka_topic')
        config.get('output', 'kafka_topic')
        enabled_features = config.getcsv('options', 'enabled_features')
        mode = config.get('options', 'mode')
        config.getint('kafka', 'retention_ms')
    except configparser.NoSectionError as e:
        logging.error(f'Missing section in config file: {e}')
        return configparser.ConfigParser()
    except configparser.NoOptionError as e:
        logging.error(f'Missing option in config file: {e}')
        return configparser.ConfigParser()
    for feature in enabled_features:
        if feature not in VALID_FEATURES:
            logging.error(f'Invalid feature specified: {feature}')
            return configparser.ConfigParser()
    if mode not in VALID_MODES:
        logging.error(f'Invalid mode specified: {mode}')
        return configparser.ConfigParser()
    return config


def generate_messages(interval_start: int,
                      interval_end: int,
                      feature_values: dict) -> dict:
    logging.info(f'Building ASN set for interval: {interval_start} - {interval_end}')
    asn_set = set()
    for feature in feature_values:
        for peer in feature_values[feature]:
            for dst in feature_values[feature][peer]:
                asn_set.add((peer, dst))
    logging.info(f'Found {len(asn_set)} ASN pairs.')
    for peer, dst in sorted(asn_set):
        msg = {'timestamp': interval_end,
               'interval_start': interval_start,
               'peer': peer,
               'dst': dst,
               'features': dict()}
        for feature, values in feature_values.items():
            if peer in values and dst in values[peer]:
                msg['features'][feature] = values[peer][dst]
        if not msg['features']:
            logging.error(f'No feature for pair {peer} {dst}')
        yield msg


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('config')
    parser.add_argument('start',
                        help='start timestamp (as UNIX epoch in seconds or milliseconds, or in YYYY-MM-DDThh:mm '
                             'format)')
    parser.add_argument('stop',
                        help='stop timestamp (as UNIX epoch in seconds or milliseconds, or in YYYY-MM-DDThh:mm format)')
    args = parser.parse_args()

    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        filename='log/aggregate_traceroute_features.log',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logging.info(f'Started: {sys.argv}')

    config = check_config(args.config)
    if not config.sections():
        sys.exit(1)

    start_ts_arg = args.start
    start_ts = parse_timestamp_argument(start_ts_arg)
    if start_ts == 0:
        logging.error(f'Invalid start timestamp specified: {start_ts_arg}')
        sys.exit(1)
    logging.info(f'Starting read at timestamp: {start_ts} '
                 f'{datetime.fromtimestamp(start_ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M")}')

    end_ts_arg = args.stop
    end_ts = parse_timestamp_argument(end_ts_arg)
    if end_ts == 0:
        logging.error(f'Invalid end timestamp specified: {end_ts_arg}')
        sys.exit(1)
    logging.info(f'Ending read at timestamp: {end_ts} '
                 f'{datetime.fromtimestamp(end_ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M")}')

    mode = config.get('options', 'mode')
    enabled_features = config.getcsv('options', 'enabled_features')
    feature_values = {feature: defaultdict(dict)
                      for feature in enabled_features}
    input_topic = config.get('input', 'kafka_topic')
    bootstrap_servers = os.environ.get('KAFKA_HOST', str())
    if config.has_option('kafka', 'bootstrap_servers'):
        bootstrap_servers = config.get('kafka', 'bootstrap_servers')

    if not bootstrap_servers:
        logging.error('No Kafka bootstrap servers specified.')
        sys.exit(1)

    kafka_topic_retention_ms = config.getint('kafka', 'retention_ms')

    reader = KafkaReader([input_topic],
                         bootstrap_servers,
                         start_ts * 1000,
                         end_ts * 1000)
    with reader:
        for msg in reader.read():
            if AS_HOPS_FEATURE in enabled_features:
                extract_as_hops(msg,
                                feature_values[AS_HOPS_FEATURE],
                                mode)
            if IP_HOPS_FEATURE in enabled_features:
                extract_ip_hops(msg,
                                feature_values[IP_HOPS_FEATURE],
                                mode)
            if RTT_FEATURE in enabled_features:
                extract_rtts(msg,
                             feature_values[RTT_FEATURE],
                             mode)

    output_topic = config.get('output', 'kafka_topic')
    writer = KafkaWriter(output_topic,
                         bootstrap_servers,
                         config={'retention.ms': kafka_topic_retention_ms})
    msg_count = 0
    with writer:
        for msg in generate_messages(start_ts, end_ts, feature_values):
            writer.write(msg['peer'],
                         msg,
                         end_ts * 1000)
            msg_count += 1
    logging.info(f'Generated {msg_count} messages.')


if __name__ == '__main__':
    main()
    sys.exit(0)

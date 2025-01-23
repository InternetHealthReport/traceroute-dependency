import argparse
import logging
import os
import sys

from kafka_wrapper.kafka_reader import KafkaReader
from utils.helper_functions import check_keys, parse_timestamp_argument

DEFAULT_BOOTSTRAP_SERVER = 'localhost:9092'


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('topic')
    parser.add_argument('output_file')
    parser.add_argument('timestamp',
                        help='read timestamp (as UNIX epoch in seconds or milliseconds, or in YYYY-MM-DDThh:mm format)')
    parser.add_argument('-n', '--num-asn', type=int,
                        help='Get first N ASes')
    parser.add_argument('-s', '--server')
    args = parser.parse_args()

    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        filename='log/get_as_rank.log',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logging.info(f'Started: {sys.argv}')

    timestamp_arg = args.timestamp
    start_ts = parse_timestamp_argument(timestamp_arg)
    if start_ts == 0:
        logging.error(f'Invalid timestamp specified: {timestamp_arg}')
        sys.exit(1)
    end_ts = start_ts + 1

    num_asn = args.num_asn

    if args.server:
        bootstrap_servers = args.server
    else:
        bootstrap_servers = os.environ.get('KAFKA_HOST', str())
    if not bootstrap_servers:
        bootstrap_servers = DEFAULT_BOOTSTRAP_SERVER

    reader = KafkaReader([args.topic],
                         bootstrap_servers,
                         start_ts * 1000,
                         end_ts * 1000)
    required_keys = ['timestamp', 'rank', 'asn']
    asns = list()
    with reader:
        for msg in reader.read():
            if check_keys(required_keys, msg):
                logging.warning(f'Missing one or more required keys {required_keys} in message: {msg}')
                continue
            if msg['timestamp'] != start_ts:
                logging.warning(f'Read message with unexpected timestamp. Expected: {start_ts} Got: {msg["timestamp"]}')
                continue
            asns.append((msg['rank'], msg['asn']))
    asns.sort()

    if num_asn:
        logging.info(f'Limiting to top {num_asn} ASes.')
        asns = asns[:num_asn]
    prev_rank = 0
    for rank, asn in asns:
        if rank != prev_rank + 1:
            logging.error(f'Gap in AS ranks. Expected: {prev_rank + 1} Got: {rank} (AS{asn})')
        prev_rank = rank

    output_file = args.output_file
    logging.info(f'Writing {len(asns)} ASes to file: {output_file}')
    with open(output_file, 'w') as f:
        for rank, asn in asns:
            f.write(f'{asn}\n')


if __name__ == '__main__':
    main()
    sys.exit(0)

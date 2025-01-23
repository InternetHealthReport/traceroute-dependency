import argparse
import configparser
import logging
import os
import sys
from concurrent.futures import as_completed
from datetime import datetime, timedelta, timezone

from requests.adapters import HTTPAdapter, Response
from requests_futures.sessions import FuturesSession
from urllib3.util.retry import Retry

from kafka_wrapper.kafka_writer import KafkaWriter
from utils.helper_functions import parse_timestamp_argument


def parse_intcsv(value: str):
    try:
        res = [int(e) for e in value.split(',')]
    except ValueError as e:
        logging.error(f'Invalid integer csv list in config: {value}: {e}')
        raise ValueError(f'Invalid integer csv list in config: {value}: {e}')
    return res


class AtlasDownloader:
    def __init__(self, max_workers: int) -> None:
        self.max_retries = 10
        self.__init_session(max_workers)
        self.url = 'https://atlas.ripe.net/api/v2/measurements/{msm_id}/results'

    def __init_session(self, max_workers: int):
        self.session = FuturesSession(max_workers=max_workers)
        retry = Retry(
            total=self.max_retries,
            status_forcelist=[500, 502, 503, 504],
            backoff_factor=0.1,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount('https://', adapter)

    def fetch(self, msm_ids: list, start: datetime, stop: datetime, probe_ids: list = list()) -> list:
        params = {
            'start': int(start.timestamp()),
            'stop': int(stop.timestamp()),
            'format': 'json',
        }
        if probe_ids:
            params['probe_ids'] = ','.join(map(str, probe_ids))

        results = list()

        # The session handles retries caused by HTTP errors, but not by aborted
        # transfers resulting in invalid JSON, so add an additional layer of retries.
        retries = 0
        unfinished_msm_ids = list(msm_ids)
        while unfinished_msm_ids and retries <= self.max_retries:
            retries += 1
            queries = list()
            for msm_id in unfinished_msm_ids:
                future = self.session.get(
                    self.url.format(msm_id=msm_id),
                    params=params,
                )
                future.msm_id = msm_id
                queries.append(future)
            unfinished_msm_ids = list()
            for future in as_completed(queries):
                try:
                    resp: Response = future.result()
                    resp.raise_for_status()
                    data = resp.json()
                except Exception as e:
                    logging.warning(f'Try {retries}: Failed to fetch data for measurement id {future.msm_id}: {e}')
                    unfinished_msm_ids.append(future.msm_id)
                    continue
                results.extend(data)
        if unfinished_msm_ids:
            logging.error(f'Failed to get data for measurement ids: {unfinished_msm_ids}')
            print(f'Failed to get data for measurement ids: {unfinished_msm_ids}', file=sys.stderr)
        return results


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('config', help='configuration file')
    parser.add_argument('start', help='start timestamp')
    parser.add_argument('end', help='end timestamp')

    log_fmt = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=log_fmt,
        filename='log/atlas-traceroute-download.log',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    logging.info(f'Started: {sys.argv}')

    args = parser.parse_args()

    start_ts = parse_timestamp_argument(args.start)
    if start_ts == 0:
        logging.error(f'Invalid start timestamp: {args.start}')
        raise ValueError(f'Invalid start timestamp: {args.start}')
    start_ts = datetime.fromtimestamp(start_ts, tz=timezone.utc)

    end_ts = parse_timestamp_argument(args.end)
    if end_ts == 0:
        logging.error(f'Invalid end timestamp: {args.end}')
        raise ValueError(f'Invalid end timestamp: {args.end}')
    end_ts = datetime.fromtimestamp(end_ts, tz=timezone.utc)

    config = configparser.ConfigParser(converters={'intcsv': parse_intcsv})
    config.read(args.config)

    msm_ids = config.getintcsv('io', 'msm_ids')
    probe_ids = config.getintcsv('io', 'probe_ids', fallback=list())
    chunk_size = config.getint('io', 'chunk_size')

    topic = config.get('kafka', 'output_topic')
    num_partitions = config.getint('kafka', 'num_partitions')
    replication_factor = config.getint('kafka', 'replication_factor')
    retention_ms = config.getint('kafka', 'retention_ms')

    bootstrap_servers = os.environ.get('KAFKA_HOST', str())
    if config.has_option('kafka', 'bootstrap_servers'):
        bootstrap_servers = config.get('kafka', 'bootstrap_servers')

    if not bootstrap_servers:
        logging.error('No Kafka bootstrap servers specified.')
        raise RuntimeError('No Kafka bootstrap servers specified.')

    writer = KafkaWriter(
        topic,
        bootstrap_servers,
        num_partitions,
        replication_factor,
        config={'retention.ms': retention_ms},
    )
    downloader = AtlasDownloader(max_workers=config.getint('io', 'workers'))
    with writer:
        current_chunk_start = start_ts
        while current_chunk_start < end_ts:
            current_chunk_end = current_chunk_start + timedelta(seconds=chunk_size)
            if current_chunk_end >= end_ts:
                current_chunk_end = end_ts - timedelta(seconds=1)
            data = downloader.fetch(msm_ids, current_chunk_start, current_chunk_end, probe_ids)
            current_chunk_start += timedelta(seconds=chunk_size)
            if not data:
                continue
            data.sort(key=lambda e: e['timestamp'])
            for entry in data:
                writer.write(
                    entry['msm_id'].to_bytes(8, byteorder='big'),
                    entry,
                    entry['timestamp'] * 1000
                )
    logging.info('Done.')


if __name__ == '__main__':
    main()
    sys.exit(0)

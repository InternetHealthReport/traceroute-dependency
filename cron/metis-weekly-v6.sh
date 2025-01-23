#!/bin/bash
set -euo pipefail

DATE_SHORT=$(date --utc +%Y%m%d)
PROBE_AS_FILE="/home/ihr/github/internet-stats/parsed/${DATE_SHORT}-probes-by-as-v6.csv"
if [ ! -f "${PROBE_AS_FILE}" ]; then
    echo "IPv6 probe AS file missing: ${PROBE_AS_FILE}"
    exit 1
fi

AGGREGATE_START_TS=$(date --utc +%Y-%m-%dT00:00 --date="-1 week")
RANK_START_TS=$(date --utc +%Y-%m-%dT00:00 --date="-4 weeks")
CANDIDATE_START_TS=$(date --utc +%Y-%m-%dT00:00 --date="-24 weeks")
STOP_TS=$(date --utc +%Y-%m-%dT00:00)
STOP_TS_PLUS_ONE=$(date --utc +%Y-%m-%dT00:01)

python3 -m metis.aggregate_traceroute_features metis/conf/aggregate_traceroutev6_features.ini "${AGGREGATE_START_TS}" "${STOP_TS}"
# We need stop timestamp plus one minute, else the last week of aggregation
# results is excluded.
python3 -m metis.compute_as_rank metis/conf/traceroutev6_as_rank_weekly.ini "${RANK_START_TS}" "${STOP_TS_PLUS_ONE}" "${PROBE_AS_FILE}" --output-timestamp "${STOP_TS}"
python3 -m metis.compute_candidates metis/conf/traceroutev6_candidates_weekly.ini "${CANDIDATE_START_TS}" "${STOP_TS_PLUS_ONE}" "${PROBE_AS_FILE}" --output-timestamp "${STOP_TS}"

python3 -m metis.get_as_rank ihr_traceroutev6_topology_as_rank_weekly_as_path_length latest-1000-asn-as-path-length-v6.txt "${STOP_TS}" -n 1000
python3 -m metis.get_as_rank ihr_traceroutev6_topology_as_rank_weekly_rtt latest-1000-asn-rtt-v6.txt "${STOP_TS}" -n 1000
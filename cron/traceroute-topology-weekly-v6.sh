#!/bin/bash
set -euo pipefail

START_TS=$(date --utc +%Y-%m-%dT00:00 --date="-1 week")
STOP_TS=$(date --utc +%Y-%m-%dT00:00)

echo "${STOP_TS}" >> log/topology-weekly-v6.log
python3 -m traceroute_dependency.traceroute_to_bgp traceroute_dependency/conf/traceroutev6_topology_weekly.ini "${START_TS}" "${STOP_TS}" >> log/topology-weekly-v6.log

python3 -m traceroute_dependency.filter_rib_topic traceroute_dependency/conf/filter_topic_traceroutev6_topology_weekly_rank_as_path_length.ini --timestamp "${STOP_TS}"
python3 -m traceroute_dependency.filter_rib_topic traceroute_dependency/conf/filter_topic_traceroutev6_topology_weekly_rank_rtt.ini --timestamp "${STOP_TS}"
#!/bin/bash
set -euo pipefail

START_TS=$(date --utc +%Y-%m-%dT00:00 --date="yesterday")
STOP_TS=$(date --utc +%Y-%m-%dT00:00)

python3 -m metis.extract_traceroute_features metis/conf/traceroutev6_topology_features.ini --start "${START_TS}" --stop "${STOP_TS}"
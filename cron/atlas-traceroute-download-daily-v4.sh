#!/bin/bash
set -euo pipefail

START_TS=$(date +%Y-%m-%dT00:00 -d "yesterday")
END_TS=$(date +%Y-%m-%dT00:00)

python3 -m atlas_traceroute.download atlas_traceroute/conf/ihr-atlas-traceroutev4-topology.conf "${START_TS}" "${END_TS}"
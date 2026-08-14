#!/usr/bin/env bash
# run_ingest.sh — DAILY pass. Cheap, API-free: fetch feeds, append new items to
# the archive. Run this often (daily) because RSS windows expire and anything a
# feed drops between runs is lost forever. Classification/rollup is separate
# (run_rollup.sh, weekly).
#
# Env:
#   PIPELINE_DATA_DIR   optional; where the archive lives (default: script dir)
#
# Usage:
#   ./run_ingest.sh
#   ./run_ingest.sh --states CO,OH,HI
set -euo pipefail
cd "$(dirname "$0")"

echo ">> ingest: fetch_feeds -> items_archive.jsonl"
python3 fetch_feeds.py "$@"
echo ">> ingest done."

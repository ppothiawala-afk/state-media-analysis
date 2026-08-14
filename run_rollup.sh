#!/usr/bin/env bash
# run_rollup.sh — WEEKLY pass. Classify the archive, append a coverage snapshot,
# verify. This is where the (optional) API cost lives. Ingestion already happened
# daily via run_ingest.sh; this stage only reads the archive, never fetches.
#
# Order matters: classify -> snapshot -> verify. verify exits non-zero on any
# FAIL, which reds the CI run.
#
# Env:
#   ANTHROPIC_API_KEY   optional; if set, classify uses the API. If absent, falls
#                       back to deterministic keyword classification (--offline).
#   PIPELINE_OFFLINE=1  force keyword-only classification even if a key exists.
#   PIPELINE_DATA_DIR   optional data dir (default: script dir)
#
# Usage:
#   ./run_rollup.sh
#   PIPELINE_OFFLINE=1 ./run_rollup.sh
set -euo pipefail
cd "$(dirname "$0")"

echo ">> [1/3] classify"
if [[ -n "${PIPELINE_OFFLINE:-}" || -z "${ANTHROPIC_API_KEY:-}" ]]; then
  echo "   (offline keyword classification — no API key in use)"
  python3 classify.py --offline
else
  python3 classify.py
fi

echo ">> [2/3] append_history (weekly snapshot)"
python3 append_history.py

echo ">> [3/3] verify_pipeline"
python3 verify_pipeline.py

echo ">> rollup done. Outputs: items_classified.json, media_history.json, verification_report.json"

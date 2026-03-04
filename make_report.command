#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG_FILE="$ROOT_DIR/projexcellent_config.json"
RUNNER="$ROOT_DIR/Code/run_report.py"
PYTHON_BIN="${PYTHON_BIN:-python3}"

if [[ ! -f "$RUNNER" ]]; then
  echo "ERROR: Missing $RUNNER"
  exit 1
fi

if [[ ! -f "$CONFIG_FILE" ]]; then
  echo "ERROR: Missing $CONFIG_FILE"
  exit 1
fi

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "ERROR: Python 3 not found (tried '$PYTHON_BIN')."
  exit 1
fi

"$PYTHON_BIN" "$RUNNER" --config "$CONFIG_FILE" "$@"

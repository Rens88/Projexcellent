#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG_FILE="$ROOT_DIR/projexcellent_config.json"
RUNNER="$ROOT_DIR/Code/new_project.py"
VENV_PY="$ROOT_DIR/Code/.venv/bin/python"
PYTHON_BIN="${PYTHON_BIN:-python3}"

pause_before_exit() {
  local exit_code=$?
  trap - EXIT
  if [[ -t 0 ]]; then
    echo
    read -r -p "Press Enter to close..." _ </dev/tty || true
  fi
  exit "$exit_code"
}
trap pause_before_exit EXIT

if [[ ! -f "$RUNNER" ]]; then
  echo "ERROR: Missing $RUNNER"
  exit 1
fi

if [[ ! -f "$CONFIG_FILE" ]]; then
  echo "ERROR: Missing $CONFIG_FILE"
  exit 1
fi

CMD_ARGS=()
if [[ "$#" -eq 0 ]]; then
  echo "No arguments supplied. Enter new project details."
  read -r -p "Slug (required, e.g. sleep_study): " NP_SLUG
  read -r -p "Project name (required, e.g. Sleep Study Pilot): " NP_PROJECT_NAME
  read -r -p "Year (optional [YYYY], e.g. 2026; default=current year): " NP_YEAR
  read -r -p "Programma (optional, default=Other): " NP_PROGRAMMA
  read -r -p "Theme (optional, default=General): " NP_THEME
  read -r -p "Owner (optional): " NP_OWNER
  read -r -p "Requester (optional, default=Unknown): " NP_REQUESTER
  read -r -p "Status [Proposed/Active/On-hold/Closed/Cancelled] (optional, e.g. active): " NP_STATUS
  read -r -p "Priority [Low/Medium/High/Critical] (optional, e.g. medium): " NP_PRIORITY

  if [[ -z "$NP_SLUG" || -z "$NP_PROJECT_NAME" ]]; then
    echo "ERROR: Slug and project name are required."
    exit 1
  fi

  CMD_ARGS=(--config "$CONFIG_FILE" --slug "$NP_SLUG" --project-name "$NP_PROJECT_NAME")
  [[ -n "$NP_YEAR" ]] && CMD_ARGS+=(--year "$NP_YEAR")
  [[ -n "$NP_PROGRAMMA" ]] && CMD_ARGS+=(--programma "$NP_PROGRAMMA")
  [[ -n "$NP_THEME" ]] && CMD_ARGS+=(--theme "$NP_THEME")
  [[ -n "$NP_OWNER" ]] && CMD_ARGS+=(--owner "$NP_OWNER")
  [[ -n "$NP_REQUESTER" ]] && CMD_ARGS+=(--requester "$NP_REQUESTER")
  [[ -n "$NP_STATUS" ]] && CMD_ARGS+=(--status "$NP_STATUS")
  [[ -n "$NP_PRIORITY" ]] && CMD_ARGS+=(--priority "$NP_PRIORITY")
else
  CMD_ARGS=(--config "$CONFIG_FILE" "$@")
fi

if [[ -x "$VENV_PY" ]]; then
  "$VENV_PY" "$RUNNER" "${CMD_ARGS[@]}"
  exit 0
fi

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "ERROR: Python 3 not found (tried '$PYTHON_BIN')."
  exit 1
fi

"$PYTHON_BIN" "$RUNNER" "${CMD_ARGS[@]}"

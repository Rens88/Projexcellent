#!/usr/bin/env python3
"""Cross-platform report launcher for Projexcellent."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import venv
from pathlib import Path
from typing import List

CODE_DIR = Path(__file__).resolve().parent
ROOT_DIR = CODE_DIR.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from projexcellent_config import DEFAULT_CONFIG_PATH, load_config, resolve_path

VENV_DIR = CODE_DIR / ".venv"
REQUIREMENTS = CODE_DIR / "requirements.txt"
REPORT_SCRIPT = CODE_DIR / "make_report.py"
VALID_REPORT_TYPES = {"combined", "yearly", "monthly", "biweekly", "weekly", "daily", "all"}


def venv_python_path() -> Path:
    if os.name == "nt":
        return VENV_DIR / "Scripts" / "python.exe"
    return VENV_DIR / "bin" / "python"


def run_or_exit(cmd: List[str], cwd: Path) -> None:
    completed = subprocess.run(cmd, cwd=str(cwd), check=False)
    if completed.returncode != 0:
        raise SystemExit(completed.returncode)


def ensure_venv() -> Path:
    python_path = venv_python_path()
    if python_path.exists():
        return python_path
    print(f"Creating virtual environment: {VENV_DIR}")
    venv.EnvBuilder(with_pip=True).create(str(VENV_DIR))
    if not python_path.exists():
        raise SystemExit(f"Failed to create virtual environment at {VENV_DIR}")
    return python_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Projexcellent report generation.")
    parser.add_argument(
        "--report-type",
        default=None,
        help="Report type to generate (overrides config runtime.default_report_type).",
    )
    parser.add_argument(
        "--asof",
        default=None,
        help="As-of date in YYYY-MM-DD (optional).",
    )
    parser.add_argument(
        "--skip-install",
        action="store_true",
        help="Skip dependency installation step.",
    )
    parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG_PATH,
        help="Path to config JSON (default: projexcellent_config.json).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    reports_dir = resolve_path(config, config.get("paths", {}).get("reports_dir", "Reports"))

    if not REPORT_SCRIPT.exists():
        raise SystemExit(f"Missing report script: {REPORT_SCRIPT}")
    if not REQUIREMENTS.exists():
        raise SystemExit(f"Missing requirements file: {REQUIREMENTS}")

    runtime_cfg = config.get("runtime", {})
    default_report_type = str(runtime_cfg.get("default_report_type", "all")).strip().lower()
    report_type = str(args.report_type or default_report_type).strip().lower()
    if report_type not in VALID_REPORT_TYPES:
        allowed = ", ".join(sorted(VALID_REPORT_TYPES))
        raise SystemExit(
            f"Invalid report type '{report_type}'. Set runtime.default_report_type in config or use --report-type. "
            f"Allowed values: {allowed}"
        )

    python_path = ensure_venv()

    install_dependencies = bool(runtime_cfg.get("install_dependencies", True))
    should_install = install_dependencies and not args.skip_install
    if should_install:
        print("Installing dependencies (quiet)...")
        run_or_exit(
            [str(python_path), "-m", "pip", "install", "-r", str(REQUIREMENTS), "--quiet"],
            cwd=ROOT_DIR,
        )

    cmd = [
        str(python_path),
        str(REPORT_SCRIPT),
        "--report-type",
        report_type,
        "--config",
        str(args.config),
    ]
    if args.asof:
        cmd.extend(["--asof", args.asof])

    print("Running report generation...")
    run_or_exit(cmd, cwd=ROOT_DIR)
    print(f"Done. See the reports folder for outputs: {reports_dir}")


if __name__ == "__main__":
    main()

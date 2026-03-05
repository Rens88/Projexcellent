#!/usr/bin/env python3
"""Cross-platform report launcher for Projexcellent."""

from __future__ import annotations

import argparse
import os
import shutil
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

LEGACY_VENV_DIR = CODE_DIR / ".venv"
WINDOWS_VENV_DIR = CODE_DIR / ".venv-win"
REQUIREMENTS = CODE_DIR / "requirements.txt"
REPORT_SCRIPT = CODE_DIR / "make_report.py"
VALID_REPORT_TYPES = {"combined", "yearly", "monthly", "biweekly", "weekly", "daily", "all"}


def venv_python_path(venv_dir: Path) -> Path:
    if os.name == "nt":
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"


def select_venv_dir() -> Path:
    if os.name != "nt":
        return LEGACY_VENV_DIR

    windows_python = venv_python_path(WINDOWS_VENV_DIR)
    if windows_python.exists():
        return WINDOWS_VENV_DIR

    legacy_python = venv_python_path(LEGACY_VENV_DIR)
    if legacy_python.exists():
        return LEGACY_VENV_DIR

    return WINDOWS_VENV_DIR


def run_or_exit(cmd: List[str], cwd: Path) -> None:
    completed = subprocess.run(cmd, cwd=str(cwd), check=False)
    if completed.returncode != 0:
        raise SystemExit(completed.returncode)


def pip_is_usable(python_path: Path) -> bool:
    completed = subprocess.run(
        [str(python_path), "-m", "pip", "--version"],
        cwd=str(ROOT_DIR),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return completed.returncode == 0


def create_venv(venv_dir: Path) -> Path:
    print(f"Creating virtual environment: {venv_dir}")
    venv.EnvBuilder(with_pip=True).create(str(venv_dir))
    python_path = venv_python_path(venv_dir)
    if not python_path.exists():
        raise SystemExit(f"Failed to create virtual environment at {venv_dir}")
    return python_path


def recreate_venv(venv_dir: Path) -> tuple[Path, Path]:
    if venv_dir.exists():
        print(f"Removing broken virtual environment: {venv_dir}")
        try:
            shutil.rmtree(venv_dir)
        except OSError as exc:
            if os.name == "nt" and venv_dir == LEGACY_VENV_DIR:
                print(f"Could not remove legacy virtual environment {venv_dir}: {exc}")
                print(f"Creating a Windows-specific virtual environment instead: {WINDOWS_VENV_DIR}")
                if WINDOWS_VENV_DIR.exists():
                    print(f"Removing stale virtual environment: {WINDOWS_VENV_DIR}")
                    try:
                        shutil.rmtree(WINDOWS_VENV_DIR)
                    except OSError as fallback_exc:
                        raise SystemExit(
                            f"Failed to remove fallback virtual environment at {WINDOWS_VENV_DIR}: {fallback_exc}"
                        ) from fallback_exc
                return WINDOWS_VENV_DIR, create_venv(WINDOWS_VENV_DIR)
            raise SystemExit(f"Failed to remove virtual environment at {venv_dir}: {exc}") from exc
    return venv_dir, create_venv(venv_dir)


def ensure_venv() -> tuple[Path, Path]:
    venv_dir = select_venv_dir()
    python_path = venv_python_path(venv_dir)
    if python_path.exists():
        return venv_dir, python_path
    return venv_dir, create_venv(venv_dir)


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

    venv_dir, python_path = ensure_venv()

    install_dependencies = bool(runtime_cfg.get("install_dependencies", True))
    should_install = install_dependencies and not args.skip_install
    if should_install:
        install_cmd = [str(python_path), "-m", "pip", "install", "-r", str(REQUIREMENTS), "--quiet"]
        print("Installing dependencies (quiet)...")
        try:
            run_or_exit(install_cmd, cwd=ROOT_DIR)
        except SystemExit:
            if pip_is_usable(python_path):
                raise
            print("Detected a broken pip in the current virtual environment. Recreating and retrying once...")
            venv_dir, python_path = recreate_venv(venv_dir)
            install_cmd[0] = str(python_path)
            print("Installing dependencies again (quiet)...")
            run_or_exit(install_cmd, cwd=ROOT_DIR)

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

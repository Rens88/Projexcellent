#!/usr/bin/env python3
"""Shared configuration loader for Projexcellent scripts."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any, Dict, List, Optional


CODE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CODE_DIR.parent
DEFAULT_CONFIG_PATH = str(PROJECT_ROOT / "projexcellent_config.json")

_DEFAULT_CONFIG: Dict[str, Any] = {
    "report_title": "Project Portfolio Overview",
    "person_name": "john doe",
    "company": {
        "name": "NOC*NSF",
        "abbreviation": "NN",
    },
    "paths": {
        "projects_dir": "Projecten",
        "dummy_projects_dir": "DummyProjecten",
        "reports_dir": "Reports",
        "templates_dir": "Templates",
        "assets_dir": "assets",
        "hours_remaining": {
            "excel_paths": [
                "Data/hours_remaining.xlsx",
                "Data/nn_maandelijks.xlsx",
                "Data/NN_maandelijks.xlsx",
            ],
            "sheet_name": "NN_maandelijks",
            "header_row": 2,
        },
        # Legacy path list is still accepted for backward compatibility.
        "hours_remaining_excel_paths": [
            "Data/nn_maandelijks.xlsx",
            "Data/NN_maandelijks.xlsx",
        ],
    },
    "branding": {
        "profile_photo": "assets/profile_photo.jpg",
        "logo": "assets/logo.png",
    },
    "hours": {
        # Optional: if set, this replaces Excel-based yearly workable-hours capacity.
        "workable_hours_per_year": None,
        # Optional preferred denominator for weekly percentage views.
        "workable_hours_per_week_reference_value": None,
    },
    "color_scheme": {
        "base_one": "#01378A",
        "base_2": "#E1011A",
        "base_3": "#EA6D08",
        "base_4": "#F4C300",
        "base_5": "#009F3D",
        "base_6": "#111111",
        # Sideways yearly plan colors
        "year_plan_completed": "#01378A",
        "year_plan_current_billed": "#01378A",
        "year_plan_current_combined": "#01378A",
        "year_plan_current_expected": "#E1011A",
        "year_plan_expected": "#F4C300",
    },
    "runtime": {
        "default_report_type": "all",
        "install_dependencies": True,
        "use_dummy_projects_when_projects_empty": True,
    },
}


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    merged: Dict[str, Any] = copy.deepcopy(base)
    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _normalize_config_path(config_path: Optional[str]) -> Path:
    raw_path = config_path or DEFAULT_CONFIG_PATH
    path = Path(raw_path).expanduser()
    if not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()
    return path


def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    path = _normalize_config_path(config_path)
    if not path.exists():
        raise SystemExit(f"Configuration file not found: {path}")

    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Invalid JSON in configuration file: {path} ({exc})") from exc
    except OSError as exc:
        raise SystemExit(f"Failed to read configuration file: {path} ({exc})") from exc

    if not isinstance(loaded, dict):
        raise SystemExit(f"Configuration root must be a JSON object: {path}")

    merged = _deep_merge(_DEFAULT_CONFIG, loaded)
    merged["__config_path"] = str(path)
    merged["__config_dir"] = str(path.parent)
    return merged


def resolve_path(config: Dict[str, Any], raw_path: Any) -> str:
    if raw_path is None:
        return ""
    path_text = str(raw_path).strip()
    if not path_text:
        return ""

    path = Path(path_text).expanduser()
    if not path.is_absolute():
        config_dir = Path(str(config.get("__config_dir", PROJECT_ROOT)))
        path = (config_dir / path).resolve()
    return str(path)


def resolve_path_list(config: Dict[str, Any], raw_values: Any) -> List[str]:
    if raw_values is None:
        return []
    if isinstance(raw_values, str):
        values = [raw_values]
    elif isinstance(raw_values, list):
        values = raw_values
    else:
        return []

    resolved: List[str] = []
    for value in values:
        resolved_value = resolve_path(config, value)
        if resolved_value and resolved_value not in resolved:
            resolved.append(resolved_value)
    return resolved

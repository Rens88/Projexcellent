#!/usr/bin/env python3
"""
Rapportage/make_report.py

What this script does
---------------------
1) Scans ../Projecten/ and treats EACH direct subfolder as one project.
2) Derives project_id from the folder name: YYYY_NNNN_<description> -> YYYY_NNNN
3) Reads project metadata from project_info.xlsx (sheet: ProjectInfo, columns: Key/Value).
4) Validates hygiene rules:
   - Folder name format is valid
   - project_info.xlsx exists
   - project_id in project_info.xlsx matches the derived project_id
   - If status == "Closed" then actual_end_date must be filled
   - time_log.xlsx metadata project_id (cell B1) matches derived project_id (if present)
5) Reads time spent from time_log.xlsx (sheet: TimeLog, rows under the header),
   aggregates hours per project and per programma/requester.
6) Creates a single HTML report (Plotly) with:
   - Tabs: Counts / Hours
   - Period switcher: 1-week / 2-weeks / month / year
   - (Optionally) single-period reports via `--report-type`
7) Exports:
   - Reports/project_report.html (combined; default)
   - Reports/Archive/*_generated_YYYY-MM-DD.html
   - Single-period exports also write a PNG (requires `pip install kaleido`)

Dependencies
------------
pip install pandas openpyxl plotly kaleido
"""

from __future__ import annotations

import argparse
import base64
import html
import os
import re
import shutil
import warnings
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots


# ----------------------------
# Paths (script lives in Rapportage/)
# ----------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECTEN_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "Projecten"))
DUMMY_PROJECTEN_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "DummyProjecten"))
NN_MAANDELIJKS_PATHS = [
    r"C:\Users\rmeer\Dropbox\Public\DataScienceAgency\Admin\Uren\2026_DSA_invulsheet_uren_en_declaraties.xlsx",
    "/mnt/c/Users/rmeer/Dropbox/Public/DataScienceAgency/Admin/Uren/2026_DSA_invulsheet_uren_en_declaraties.xlsx",
]

def has_subfolders(path: str) -> bool:
    if not os.path.isdir(path):
        return False
    return any(
        os.path.isdir(os.path.join(path, entry))
        for entry in os.listdir(path)
    )

if not has_subfolders(PROJECTEN_DIR):
    print('WARNING: No files found in "../Projecten/". Using DummyProjecten/ for testing purposes.')
    PROJECTEN_DIR = DUMMY_PROJECTEN_DIR


# ----------------------------
# Warnings configuration
# ----------------------------
warnings.filterwarnings(
    "ignore",
    category=UserWarning,
    message="Data Validation extension is not supported and will be removed",
)
# warnings.filterwarnings(
#     "ignore",
#     category=FutureWarning,
#     message="The behavior of array concatenation with empty entries is deprecated",
# )


# ----------------------------
# TeamNL palette
# ----------------------------
BASE_BLUE = "#01378A"
BASE_RED = "#E1011A"
BASE_ORANGE = "#EA6D08"

# Derived from TeamNL + Olympic palette in teamnl_logo_olympisch_rgb_witkader-website.jpg
# BASE_NAVY = "#0B2A72"
# BASE_SKY = "#1A76D2"
BASE_YELLOW = "#F4C300"
BASE_GREEN = "#009F3D"
BASE_BLACK = "#111111"

TEAMNL_BASE_COLORS = [
    BASE_BLUE,
    BASE_RED,
    BASE_ORANGE,
    BASE_YELLOW,
    BASE_GREEN,
    BASE_BLACK,
]

SHADE_STEPS = [0.0, -0.25, 0.25, -0.50, 0.5, 0.75, -0.75]


# ----------------------------
# Color helpers
# ----------------------------
def _clamp_channel(value: float) -> int:
    return int(max(0, min(255, round(value))))


def adjust_color_luminance(hex_color: str, factor: float) -> str:
    """
    Lightens (factor>0) or darkens (factor<0) a hex color by the given factor.
    """
    color = hex_color.lstrip("#")
    r, g, b = int(color[0:2], 16), int(color[2:4], 16), int(color[4:6], 16)

    if factor >= 0:
        r = _clamp_channel(r + (255 - r) * factor)
        g = _clamp_channel(g + (255 - g) * factor)
        b = _clamp_channel(b + (255 - b) * factor)
    else:
        r = _clamp_channel(r * (1 + factor))
        g = _clamp_channel(g * (1 + factor))
        b = _clamp_channel(b * (1 + factor))

    return f"#{r:02X}{g:02X}{b:02X}"


def hex_to_rgba(hex_color: str, alpha: float) -> str:
    color = hex_color.lstrip("#")
    if len(color) != 6:
        return f"rgba(0,0,0,{alpha})"
    r, g, b = int(color[0:2], 16), int(color[2:4], 16), int(color[4:6], 16)
    return f"rgba({r},{g},{b},{alpha})"


def build_color_maps(projects_df: pd.DataFrame) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Returns:
      - program_color_map: programma -> base color
      - project_color_map: project_id -> shaded color (based on the programma base color)
    """
    program_color_map: Dict[str, str] = {}
    project_color_map: Dict[str, str] = {}

    programs = sorted(projects_df["programma"].fillna("Unknown").replace("", "Unknown").unique().tolist())
    for idx, programma in enumerate(programs):
        program_color_map[programma] = TEAMNL_BASE_COLORS[idx % len(TEAMNL_BASE_COLORS)]

    for programma in programs:
        base = program_color_map[programma]
        mask = projects_df["programma"].fillna("Unknown").replace("", "Unknown") == programma
        projects_in_program = projects_df.loc[mask].sort_values("project_id")

        for shade_idx, (_, project_row) in enumerate(projects_in_program.iterrows()):
            shade = SHADE_STEPS[shade_idx % len(SHADE_STEPS)]
            project_id = str(project_row.get("project_id", "")).strip()
            if project_id:
                project_color_map[project_id] = adjust_color_luminance(base, shade)

    return program_color_map, project_color_map


# ----------------------------
# Project folder naming rules
# ----------------------------
def derive_project_id_from_folder(folder_name: str) -> str:
    """
    Expected folder name format:
        YYYY_NNNN_<description>
    Example:
        2026_0001_SSC_Fysiologie_ondersteuning

    Derived project_id is:
        YYYY_NNNN
    """
    parts = folder_name.split("_")
    if len(parts) < 3:
        raise ValueError(
            f"Invalid project folder name '{folder_name}'. "
            "Expected format: YYYY_NNNN_<description>"
        )

    year, counter = parts[0], parts[1]
    if not (year.isdigit() and len(year) == 4):
        raise ValueError(f"Invalid year in project folder '{folder_name}' (expected 4 digits).")
    if not counter.isdigit():
        raise ValueError(f"Invalid counter in project folder '{folder_name}' (expected digits).")

    return f"{year}_{counter}"


def discover_project_folders(projecten_dir: str) -> List[str]:
    """Each direct subfolder under Projecten/ is treated as a project."""
    if not os.path.isdir(projecten_dir):
        raise FileNotFoundError(f"Projecten folder not found: {projecten_dir}")

    folders: List[str] = []
    for name in sorted(os.listdir(projecten_dir)):
        path = os.path.join(projecten_dir, name)
        if os.path.isdir(path):
            folders.append(path)
    return folders


# ----------------------------
# Reading project_info.xlsx (key/value)
# ----------------------------
def read_project_info_kv_from_xlsx(path: str) -> Dict[str, Any]:
    """
    Reads Excel with:
      sheet: ProjectInfo
      row 1 headers: Key | Value
      rows 2..n: key/value
    """
    df = pd.read_excel(path, sheet_name="ProjectInfo", header=0, usecols=[0, 1])
    df.columns = ["key", "value"]
    df = df.dropna(subset=["key"]).copy()

    df["key"] = df["key"].astype(str).str.strip()
    df["value"] = df["value"].apply(lambda v: v.strip() if isinstance(v, str) else v)

    return dict(zip(df["key"], df["value"]))


def parse_date(value: Any) -> Optional[pd.Timestamp]:
    """Best-effort date parsing; returns pandas Timestamp or None."""
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if isinstance(value, str) and value.strip() == "":
        return None
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return None
    return ts


def _split_pipe_values(val: Any) -> List[str]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return []
    parts = str(val).split("|")
    return [p.strip() for p in parts if p and p.strip()]


def _clean_group_value(val: Any) -> Optional[str]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    if not s:
        return None
    return "Unknown" if s.lower() == "unknown" else s


def extract_group_values(row: pd.Series, base_col: str) -> List[str]:
    """
    Returns ordered unique values for a base column that may have numbered variants (e.g., programma, programma02).
    Skips empty entries and prefers real values over 'Unknown'.
    """
    matches: List[Tuple[int, str]] = []
    base_len = len(base_col)
    for col in row.index:
        if col == base_col:
            matches.append((1, col))
        elif col.startswith(base_col) and col[base_len:].isdigit():
            matches.append((int(col[base_len:]), col))

    values: List[str] = []
    saw_unknown = False
    for _, col in sorted(matches, key=lambda x: x[0]):
        val = _clean_group_value(row.get(col))
        if val is None:
            continue
        if val == "Unknown":
            saw_unknown = True
            continue
        if val not in values:
            values.append(val)

    if values:
        return values
    return ["Unknown"] if saw_unknown else []


def _split_pipe_values(val: Any) -> List[str]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return []
    parts = str(val).split("|")
    return [p.strip() for p in parts if p and p.strip()]


# ----------------------------
# Reading time_log.xlsx
# ----------------------------
TIMELOG_SHEET_NAME = "TimeLog"
TIMELOG_HEADER_ROW_1BASED = 6  # row 6 contains column headers in your template

TIMELOG_COLUMNS = [
    "Date*",
    "StartTime",
    "EndTime",
    "DurationMinutes*",
    "ActivityType*",
    "WhatIDid*",
    "OutputLink",
    "NextStep",
    "Tags",
    "Location",
]


def read_time_log_entries(time_log_path: str) -> pd.DataFrame:
    """
    Reads time_log.xlsx (sheet 'TimeLog') and returns a dataframe with computed duration_minutes.

    Rules:
    - Prefer DurationMinutes as the source of truth.
    - If DurationMinutes is empty, but StartTime and EndTime are present, compute minutes.
    - Ignore fully empty rows.
    """
    # Header row is 6 -> pandas header index 5
    df = pd.read_excel(
        time_log_path,
        sheet_name=TIMELOG_SHEET_NAME,
        header=TIMELOG_HEADER_ROW_1BASED - 1,
        usecols="A:J",
        engine="openpyxl",
    )

    missing = [c for c in TIMELOG_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(
            f"time_log.xlsx has unexpected columns in {time_log_path}. "
            f"Missing expected: {missing}. Found: {list(df.columns)}"
        )

    df = df.dropna(how="all").copy()
    if df.empty:
        df["duration_minutes"] = pd.Series(dtype="float")
        df["date"] = pd.Series(dtype="datetime64[ns]")
        return df

    df["date"] = pd.to_datetime(df["Date*"], errors="coerce")
    df["duration_minutes"] = pd.to_numeric(df["DurationMinutes*"], errors="coerce")

    def to_minutes_from_times(start, end) -> Optional[float]:
        if pd.isna(start) or pd.isna(end):
            return None
        start_dt = pd.to_datetime(str(start), errors="coerce")
        end_dt = pd.to_datetime(str(end), errors="coerce")
        if pd.isna(start_dt) or pd.isna(end_dt):
            return None
        minutes = (end_dt - start_dt).total_seconds() / 60.0
        if minutes <= 0:
            return None
        return minutes

    needs_compute = df["duration_minutes"].isna()
    if needs_compute.any():
        computed: List[Optional[float]] = []
        for s, e, need in zip(df["StartTime"], df["EndTime"], needs_compute):
            computed.append(to_minutes_from_times(s, e) if need else None)
        df.loc[needs_compute, "duration_minutes"] = computed

    # Keep rows with at least a date or a duration
    df = df[(~df["date"].isna()) | (~df["duration_minutes"].isna())].copy()
    return df


def read_time_log_project_metadata(time_log_path: str) -> Dict[str, str]:
    """
    Reads metadata at the top of the TimeLog sheet:
      B1: Project ID
      B2: Project Name
      B3: Programma
    """
    meta = {"project_id": "", "project_name": "", "programma": ""}
    try:
        raw = pd.read_excel(time_log_path, sheet_name=TIMELOG_SHEET_NAME, header=None, nrows=3, usecols="A:B")
        meta["project_id"] = str(raw.iloc[0, 1]).strip() if raw.shape[0] > 0 and not pd.isna(raw.iloc[0, 1]) else ""
        meta["project_name"] = str(raw.iloc[1, 1]).strip() if raw.shape[0] > 1 and not pd.isna(raw.iloc[1, 1]) else ""
        meta["programma"] = str(raw.iloc[2, 1]).strip() if raw.shape[0] > 2 and not pd.isna(raw.iloc[2, 1]) else ""
    except Exception:
        raise ValueError(f"Failed to read time_log.xlsx metadata from {time_log_path}")
    return meta


# ----------------------------
# Human-readable record objects
# ----------------------------
@dataclass
class ProjectRecord:
    folder_name: str
    folder_path: str
    project_id: str
    project_info_path: str
    time_log_path: str
    info: Dict[str, Any]


# ----------------------------
# Load + validate projects
# ----------------------------
def load_and_validate_projects(projecten_dir: str) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Dict[str, Any]]]:
    """
    Returns:
      projects_df: one row per project (metadata)
      time_entries_df: one row per time log entry (enriched with project fields)
      project_info_map: project_id -> raw project_info.xlsx key/value dict
    """
    project_rows: List[Dict[str, Any]] = []
    all_time_entries: List[pd.DataFrame] = []
    project_info_map: Dict[str, Dict[str, Any]] = {}

    for folder_path in discover_project_folders(projecten_dir):
        folder_name = os.path.basename(folder_path)
        derived_project_id = derive_project_id_from_folder(folder_name)

        project_info_path = os.path.join(folder_path, "project_info.xlsx")
        time_log_path = os.path.join(folder_path, "time_log.xlsx")

        if not os.path.exists(project_info_path):
            raise FileNotFoundError(f"Missing project_info.xlsx in project folder '{folder_name}'")

        info = read_project_info_kv_from_xlsx(project_info_path)
        project_info_map[derived_project_id] = dict(info)

        # Requirement: derived project id must match project_info.xlsx project_id
        info_project_id = str(info.get("project_id", "")).strip()
        if not info_project_id:
            raise ValueError(f"'project_id' missing or empty in project_info.xlsx for '{folder_name}'")
        if info_project_id != derived_project_id:
            raise ValueError(
                f"Project ID mismatch in folder '{folder_name}'. "
                f"Derived from folder: '{derived_project_id}', "
                f"but project_info.xlsx contains: '{info_project_id}'."
            )

        # Next step #1: closure hygiene check
        status = str(info.get("status", "")).strip()
        actual_end_date = parse_date(info.get("actual_end_date"))
        if status == "Closed" and actual_end_date is None:
            raise ValueError(
                f"Project '{folder_name}' is status=Closed but actual_end_date is missing in project_info.xlsx."
            )

        # Create a clean project row
        project_row = dict(info)
        project_row["project_id"] = derived_project_id  # authoritative
        project_row["__folder_name"] = folder_name
        project_row["__project_folder"] = os.path.relpath(folder_path, SCRIPT_DIR)
        project_row["__project_info_file"] = os.path.relpath(project_info_path, SCRIPT_DIR)

        project_row["start_date"] = parse_date(info.get("start_date"))
        project_row["target_end_date"] = parse_date(info.get("target_end_date"))
        project_row["actual_end_date"] = actual_end_date

        project_row['programma(s)'] = project_row.get("programma (if multiple, separate by |)") or project_row.get("programma")
        programma_values = _split_pipe_values(project_row.get("programma (if multiple, separate by |)") or project_row.get("programma"))
        if programma_values:
            project_row["programma"] = programma_values[0]
            for idx, extra in enumerate(programma_values[1:], start=2):
                project_row[f"programma{idx:02d}"] = extra

        project_row['theme(s)'] = project_row.get("theme (if multiple, separate by |)") or project_row.get("theme")
        theme_values = _split_pipe_values(project_row.get("theme (if multiple, separate by |)") or project_row.get("theme"))
        if theme_values:
            project_row["theme"] = theme_values[0]
            for idx, extra in enumerate(theme_values[1:], start=2):
                project_row[f"theme{idx:02d}"] = extra

        project_row['requester(s)'] = project_row.get("requester(s) (if multiple, separate by |)") or project_row.get("requester")
        requester_values = _split_pipe_values(project_row.get("requester(s) (if multiple, separate by |)") or project_row.get("requester"))
        if requester_values:
            project_row["requester"] = requester_values[0]
            for idx, extra in enumerate(requester_values[1:], start=2):
                project_row[f"requester{idx:02d}"] = extra

        # Next step #2: hours aggregation from time_log.xlsx
        if os.path.exists(time_log_path):
            meta = read_time_log_project_metadata(time_log_path)
            if meta.get("project_id") and meta["project_id"] != derived_project_id:
                raise ValueError(
                    f"time_log.xlsx metadata project_id mismatch in '{folder_name}'. "
                    f"Derived folder id: '{derived_project_id}', metadata says: '{meta['project_id']}'."
                )

            time_df = read_time_log_entries(time_log_path)
            if not time_df.empty:
                time_df = time_df.copy()
                time_df["project_id"] = derived_project_id
                time_df["programma"] = str(project_row.get("programma", "Unknown") or "Unknown")
                time_df["project_name"] = str(project_row.get("project_name", derived_project_id) or derived_project_id)
                time_df["__project_folder"] = project_row["__project_folder"]
                time_df["duration_hours"] = pd.to_numeric(time_df["duration_minutes"], errors="coerce") / 60.0
                all_time_entries.append(time_df)

        project_rows.append(project_row)

    projects_df = pd.DataFrame(project_rows)

    for col in ["programma", "requester", "status", "project_name", "theme"]:
        if col not in projects_df.columns:
            projects_df[col] = "Unknown"
        projects_df[col] = projects_df[col].fillna("Unknown").replace("", "Unknown")

    time_entries_df = pd.concat(all_time_entries, ignore_index=True) if all_time_entries else pd.DataFrame()

    projects_df = projects_df.sort_values(["created_at"]).reset_index(drop=True)
    if not time_entries_df.empty and "Date*" in time_entries_df.columns:
        time_entries_df = time_entries_df.sort_values(["Date*"]).reset_index(drop=True)
    return projects_df, time_entries_df, project_info_map


# ----------------------------
# Hover helper
# ----------------------------
HOVER_KEYS = [
    "project_id",
    "project_name",
    "programma(s)",
    "requester(s)",
    "owner",
    "status",
    "priority",
    "theme(s)",
    "start_date",
    "target_end_date",
    "actual_end_date",
]


def build_hover_text(project_row: pd.Series, extra: Optional[Dict[str, Any]] = None) -> str:
    parts = []
    for k in HOVER_KEYS:
        if k in project_row and pd.notna(project_row[k]) and str(project_row[k]).strip() != "":
            parts.append(f"<b>{k}</b>: {project_row[k]}")
    if "__project_folder" in project_row and pd.notna(project_row["__project_folder"]):
        parts.append(f"<b>folder</b>: {project_row['__project_folder']}")
    if extra:
        for k, v in extra.items():
            if v is not None and str(v).strip() != "":
                parts.append(f"<b>{k}</b>: {v}")
    return "<br>".join(parts)


# ----------------------------
# Period helpers
# ----------------------------
def _last_completed_month(asof_date: date) -> Tuple[date, date, str]:
    first_of_month = date(asof_date.year, asof_date.month, 1)
    last_day_prev = first_of_month - timedelta(days=1)
    period_start = date(last_day_prev.year, last_day_prev.month, 1)
    period_end = last_day_prev
    period_key = f"{period_end.year:04d}-{period_end.month:02d}"
    return period_start, period_end, period_key


def _last_completed_iso_week(asof_date: date) -> Tuple[date, date, str]:
    last_sunday = asof_date - timedelta(days=asof_date.weekday() + 1)
    period_start = last_sunday - timedelta(days=6)
    period_end = last_sunday
    iso = period_end.isocalendar()
    period_key = f"{iso.year}-W{iso.week:02d}"
    return period_start, period_end, period_key


def _last_completed_biweekly(asof_date: date) -> Tuple[date, date, str]:
    # Two full ISO weeks (Mon-Sun), ending on the last completed Sunday.
    last_sunday = asof_date - timedelta(days=asof_date.weekday() + 1)
    period_end = last_sunday
    period_start = period_end - timedelta(days=13)
    iso_start = period_start.isocalendar()
    iso_end = period_end.isocalendar()
    period_key = f"{iso_start.year}-W{iso_start.week:02d}_to_{iso_end.year}-W{iso_end.week:02d}"
    return period_start, period_end, period_key


def compute_report_periods(asof_date: date) -> Dict[str, Dict[str, Any]]:
    monthly_start, monthly_end, month_key = _last_completed_month(asof_date)
    weekly_start, weekly_end, week_key = _last_completed_iso_week(asof_date)
    biweekly_start, biweekly_end, biweek_key = _last_completed_biweekly(asof_date)
    yearly_start = date(asof_date.year, 1, 1)
    yearly_end = asof_date
    year_key = f"{asof_date.year:04d}"
    return {
        "weekly": dict(label="1-week", start=weekly_start, end=weekly_end, key=week_key),
        "biweekly": dict(label="2-weeks", start=biweekly_start, end=biweekly_end, key=biweek_key),
        "monthly": dict(label="Month", start=monthly_start, end=monthly_end, key=month_key),
        "yearly": dict(label="Year (to-date)", start=yearly_start, end=yearly_end, key=year_key),
    }


def list_completed_month_periods(asof_date: date, time_entries_df: Optional[pd.DataFrame] = None) -> List[Dict[str, Any]]:
    """
    Returns completed month periods (start/end/key/label), newest-first, ending at the last fully completed month
    before `asof_date`.

    If time_entries_df is provided, the earliest month is derived from the earliest available time entry date;
    otherwise only the last completed month is returned.
    """
    last_start, last_end, _ = _last_completed_month(asof_date)
    start_month = last_start

    if time_entries_df is not None and not time_entries_df.empty and "date" in time_entries_df.columns:
        dates = pd.to_datetime(time_entries_df["date"], errors="coerce").dropna()
        if not dates.empty:
            min_date = dates.min().date()
            if min_date <= last_end:
                start_month = date(min_date.year, min_date.month, 1)

    month_starts = pd.date_range(pd.Timestamp(start_month), pd.Timestamp(last_start), freq="MS")
    periods: List[Dict[str, Any]] = []
    for month_start in month_starts:
        ms = month_start.date()
        me = (month_start + pd.offsets.MonthEnd(0)).date()
        key = f"{ms.year:04d}-{ms.month:02d}"
        label = month_start.strftime("%b %Y")
        periods.append(dict(start=ms, end=me, key=key, label=label))

    periods.sort(key=lambda p: p["start"], reverse=True)
    return periods


def filter_time_entries_by_period(time_entries_df: pd.DataFrame, period_start: date, period_end: date) -> pd.DataFrame:
    if time_entries_df.empty or "date" not in time_entries_df.columns:
        return time_entries_df.copy()
    mask = (time_entries_df["date"] >= pd.Timestamp(period_start)) & (time_entries_df["date"] <= pd.Timestamp(period_end))
    return time_entries_df.loc[mask].copy()


def filter_projects_with_hours(
    projects_df: pd.DataFrame, time_entries_df_filtered: pd.DataFrame
) -> pd.DataFrame:
    if time_entries_df_filtered.empty or "project_id" not in time_entries_df_filtered.columns:
        return projects_df.iloc[0:0].copy()
    hours_by_project = (
        time_entries_df_filtered.groupby("project_id")["duration_hours"]
        .sum(min_count=1)
        .fillna(0.0)
    )
    valid_ids = {str(pid) for pid, hours in hours_by_project.items() if hours > 0}
    if not valid_ids:
        return projects_df.iloc[0:0].copy()
    mask = projects_df["project_id"].astype(str).isin(valid_ids)
    return projects_df.loc[mask].copy()


def build_year_week_grid(target_year: int) -> Tuple[pd.DatetimeIndex, pd.DatetimeIndex, pd.DatetimeIndex, float]:
    year_start = date(target_year, 1, 1)
    year_end = date(target_year, 12, 31)
    week_starts = pd.date_range(pd.Timestamp(year_start), pd.Timestamp(year_end), freq="W-MON")
    week_ends = week_starts + pd.Timedelta(days=6)
    bar_width = pd.Timedelta(days=7)
    half_bar = bar_width / 2
    week_positions = week_starts + half_bar
    bar_width_ms = bar_width / pd.Timedelta(milliseconds=1)
    return week_starts, week_ends, week_positions, bar_width_ms


def estimate_magnitude_weight(value: Any) -> int:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return 1
    if isinstance(value, (int, float)):
        hours = float(value)
    else:
        s = str(value).strip().lower()
        if not s:
            return 1
        if any(k in s for k in ["small", "klein"]):
            return 1
        if "medium" in s:
            return 3
        if "substantial" in s:
            return 8
        if "large" in s or "groot" in s:
            return 16
        if "enormous" in s or "enorm" in s:
            return 30
        try:
            hours = float(re.sub(r"[^\d.]+", "", s))
        except ValueError:
            return 1

    if hours < 6:
        return 1
    if hours < 24:
        return 3
    if hours < 160:
        return 8
    if hours < 320:
        return 16
    return 30


def build_period_week_grid(period_start: date, period_end: date) -> Tuple[pd.DatetimeIndex, pd.DatetimeIndex, pd.DatetimeIndex, float]:
    start_week = period_start - timedelta(days=period_start.weekday())
    end_week = period_end - timedelta(days=period_end.weekday())
    week_starts = pd.date_range(pd.Timestamp(start_week), pd.Timestamp(end_week), freq="W-MON")
    week_ends = week_starts + pd.Timedelta(days=6)
    bar_width = pd.Timedelta(days=7)
    half_bar = bar_width / 2
    week_positions = week_starts + half_bar
    bar_width_ms = bar_width / pd.Timedelta(milliseconds=1)
    return week_starts, week_ends, week_positions, bar_width_ms


# ----------------------------
# NN_maandelijks helpers
# ----------------------------
_MONTH_MAP = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}


def _clean_colname(name: str) -> str:
    return re.sub(r"\s+", " ", str(name)).strip().lower()


def _find_col(cols: List[str], include_tokens: List[str], exclude_tokens: Optional[List[str]] = None) -> Optional[str]:
    exclude_tokens = exclude_tokens or []
    for col in cols:
        norm = _clean_colname(col)
        if all(tok in norm for tok in include_tokens) and not any(tok in norm for tok in exclude_tokens):
            return col
    return None


def _to_float(val: Any) -> Optional[float]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _parse_month_label(val: Any) -> Optional[pd.Timestamp]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, (pd.Timestamp, datetime, date)):
        return pd.Timestamp(val.year, val.month, 1)
    s = str(val).strip()
    if not s:
        return None
    ts = pd.to_datetime(s, errors="coerce")
    if not pd.isna(ts):
        return pd.Timestamp(ts.year, ts.month, 1)
    cleaned = re.sub(r"[^A-Za-z0-9]", "", s).lower()
    m = re.match(r"([a-z]{3,9})(\d{4})", cleaned)
    if m and m.group(1)[:3] in _MONTH_MAP:
        return pd.Timestamp(int(m.group(2)), _MONTH_MAP[m.group(1)[:3]], 1)
    m = re.match(r"(\d{4})(\d{1,2})", cleaned)
    if m:
        year = int(m.group(1))
        month = int(m.group(2))
        if 1 <= month <= 12:
            return pd.Timestamp(year, month, 1)
    return None


def _find_nn_maandelijks_path() -> Optional[str]:
    for path in NN_MAANDELIJKS_PATHS:
        if os.path.exists(path):
            return path
    return None


def load_nn_maandelijks_df() -> Tuple[Optional[pd.DataFrame], Optional[str], str]:
    path = _find_nn_maandelijks_path()
    if not path:
        return None, None, "NN_maandelijks file not found; skipping NN summaries."
    try:
        df = pd.read_excel(path, sheet_name="NN_maandelijks", header=1)
    except Exception as exc:
        return None, path, f"Failed to read NN_maandelijks: {exc}"
    return df, path, f"NN_maandelijks loaded from {path}"


def compute_nn_summary(
    nn_df: Optional[pd.DataFrame],
    period_type: str,
    period_end: date,
    time_entries_df_filtered: pd.DataFrame,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    if nn_df is None or nn_df.empty:
        return None, "NN_maandelijks data not available."

    df = nn_df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    if df.empty:
        return None, "NN_maandelijks sheet is empty."

    month_col = 'Tabblad'
    if not month_col in df.columns:
        month_col = df.columns[0]

    df["__month"] = df[month_col].apply(_parse_month_label)
    df = df.dropna(subset=["__month"]).copy()
    if df.empty:
        return None, "No month rows found in NN_maandelijks."

    df = df.sort_values("__month")
    target_month = pd.Timestamp(period_end.year, period_end.month, 1)
    row = df.loc[df["__month"] == target_month]
    if row.empty:
        return None, f"No NN_maandelijks row found for {target_month.date().isoformat()}."
    row = row.iloc[0]

    cols = list(df.columns)
    billed_year_col = _find_col(cols, ["cumulatief"])
    billed_month_col = _find_col(cols, ["uren per maand"])
    remaining_col = _find_col(cols, ["resterend"])

    billed = None
    remaining = None

    if period_type == "monthly":
        if billed_month_col:
            billed = _to_float(row.get(billed_month_col))
        if remaining_col:
            remaining = _to_float(row.get(remaining_col))

    else:  # yearly
        if billed_year_col:
            billed = _to_float(row.get(billed_year_col))
        if remaining_col:
            remaining = _to_float(row.get(remaining_col))

    project_logged_hours = float(time_entries_df_filtered["duration_hours"].sum()) if not time_entries_df_filtered.empty else 0.0
    completeness_ratio = None
    if billed is not None and billed > 0:
        completeness_ratio = project_logged_hours / billed

    summary = dict(
        period_type=period_type,
        period_month=target_month,
        billed=billed,
        remaining=remaining,
        project_logged_hours=project_logged_hours,
        completeness_ratio=completeness_ratio,
    )
    return summary, None


def build_nn_pie_html(nn_summary: Optional[Dict[str, Any]], div_id: str = "nn-pie") -> str:
    if not nn_summary:
        return ""
    billed = nn_summary.get("billed")
    remaining = nn_summary.get("remaining")
    if billed is None or remaining is None:
        return ""
    total = billed + remaining
    if total <= 0:
        return ""
    fig = go.Figure(
        data=[
            go.Pie(
                labels=["Gefactureerd", "Resterend"],
                values=[billed, remaining],
                marker=dict(colors=[BASE_BLUE, BASE_YELLOW]),
                hole=0.45,
                pull=[0.2,0.0],
                texttemplate="%{value:.0f}<br>(%{percent})",
                textposition="inside",
            )
        ]
    )
    center_text = f"{billed:.0f} / {total:.0f}"
    fig.update_layout(
        margin=dict(l=0, r=0, t=0, b=0),
        height=140,
        width=140,
        showlegend=False,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        annotations=[
            dict(
                text=center_text,
                x=0.5,
                y=0.5,
                xref="paper",
                yref="paper",
                showarrow=False,
                font=dict(size=14, color=BASE_BLACK),
                xanchor="center",
                yanchor="middle",
            ),
        ],
    )
    return pio.to_html(fig, include_plotlyjs=False, full_html=False, div_id=div_id)


def build_nn_metrics_html(nn_summary: Optional[Dict[str, Any]], note: Optional[str]) -> str:
    if not nn_summary:
        note_text = note or "NN summary not available."
        return f"<div class='nn-note'>{html.escape(note_text)}</div>"

    billed = nn_summary.get("billed")
    remaining = nn_summary.get("remaining")
    logged = nn_summary.get("project_logged_hours")
    ratio = nn_summary.get("completeness_ratio")
    period_type = nn_summary.get("period_type")
    billed_label = "Billed (month)" if period_type == "monthly" else "Billed (ytd)"

    def fmt(val: Optional[float]) -> str:
        if val is None:
            return "n/a"
        return f"{val:.0f}"

    ratio_text = f"{ratio * 100:.0f}%" if ratio is not None else "n/a"
    return (
        "<div class='nn-metrics'>"
        "<div><b>" + html.escape(billed_label) + "</b>: " + html.escape(fmt(billed)) + "</div>"
        "<div><b>Remaining</b>: " + html.escape(fmt(remaining)) + "</div>"
        "<div><b>Project logged hours</b>: " + html.escape(fmt(logged)) + "</div>"
        "<div><b>Tracking completeness</b>: " + html.escape(ratio_text) + "</div>"
        "</div>"
    )


def _plotly_cdn_src() -> str:
    # Pin plotly.js to the plotly.py-bundled version when possible.
    # Some older/newer plotly.py versions expose this as `plotlyjs_version` (public)
    # instead of `_plotlyjs_version` (private). Falling back to `plotly-latest` can
    # cause subtle rendering regressions between `.show()` and exported HTML.
    for attr in ("plotlyjs_version", "_plotlyjs_version"):
        version = getattr(pio, attr, None)
        if isinstance(version, str) and version.strip():
            return f"https://cdn.plot.ly/plotly-{version}.min.js"
    return "https://cdn.plot.ly/plotly-latest.min.js"


def build_project_info_tables_html(
    projects_df: pd.DataFrame, project_info_map: Dict[str, Dict[str, Any]]
) -> str:
    cards: List[str] = []
    for _, row in projects_df.iterrows():
        project_id = str(row.get("project_id", "")).strip()
        project_name = str(row.get("project_name", project_id)).strip()
        info = project_info_map.get(project_id, {})

        rows: List[str] = []
        for key in sorted(info.keys()):
            val = info.get(key)
            if val is None or (isinstance(val, float) and pd.isna(val)) or str(val).strip() == "":
                continue
            rows.append(
                "<tr><td>" + html.escape(str(key)) + "</td><td>" + html.escape(str(val)) + "</td></tr>"
            )

        if not rows:
            rows_html = "<tr><td colspan='2'>No data</td></tr>"
        else:
            rows_html = "".join(rows)

        cards.append(
            "<div class='project-card'>"
            "<div class='project-card-header'>" + html.escape(project_id) + " — " + html.escape(project_name) + "</div>"
            "<table>" + rows_html + "</table>"
            "</div>"
        )

    return "<div class='project-cards'>" + "".join(cards) + "</div>"


def _escape_html_multiline(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value)
    escaped = html.escape(text)
    escaped = escaped.replace("\r\n", "\n").replace("\r", "\n")
    return escaped.replace("\n", "<br/>")


def _format_minutes_hhmm(total_minutes: Any) -> str:
    if total_minutes is None or (isinstance(total_minutes, float) and pd.isna(total_minutes)):
        return "00:00"
    try:
        minutes_int = int(round(float(total_minutes)))
    except (TypeError, ValueError):
        return "00:00"
    sign = "-" if minutes_int < 0 else ""
    minutes_int = abs(minutes_int)
    hours, minutes = divmod(minutes_int, 60)
    return f"{sign}{hours:02d}:{minutes:02d}"


def build_logged_hours_breakdown_html(
    time_entries_df_filtered: pd.DataFrame,
    title: str = "Logged hours (by project)",
    show_percentage: bool = False,
) -> str:
    if time_entries_df_filtered is None or time_entries_df_filtered.empty:
        return (
            "<div class='hours-breakdown'>"
            f"<div class='hours-breakdown-header'><h3>{html.escape(title)}</h3></div>"
            "<div class='hours-breakdown-note'>No time log entries in this period.</div>"
            "</div>"
        )

    required_cols = {"project_id", "duration_hours"}
    missing = [c for c in required_cols if c not in time_entries_df_filtered.columns]
    if missing:
        return (
            "<div class='hours-breakdown'>"
            f"<div class='hours-breakdown-header'><h3>{html.escape(title)}</h3></div>"
            "<div class='hours-breakdown-note'>"
            "Time log entries are missing expected columns: "
            + html.escape(", ".join(missing))
            + "</div>"
            "</div>"
        )

    entries = time_entries_df_filtered.copy()
    entries["project_id"] = entries["project_id"].astype(str).str.strip()

    if "project_name" not in entries.columns:
        entries["project_name"] = entries["project_id"]
    entries["project_name"] = entries["project_name"].fillna("").astype(str).str.strip()
    entries.loc[entries["project_name"] == "", "project_name"] = entries["project_id"]

    what_i_did_col: Optional[str] = None
    for candidate in ("WhatIDid*", "WhatIDid"):
        if candidate in entries.columns:
            what_i_did_col = candidate
            break

    entries["duration_hours"] = pd.to_numeric(entries["duration_hours"], errors="coerce")
    entries = entries.dropna(subset=["duration_hours"]).copy()
    entries = entries[entries["duration_hours"] > 0].copy()
    if "duration_minutes" in entries.columns:
        entries["duration_minutes"] = pd.to_numeric(entries["duration_minutes"], errors="coerce")
    else:
        entries["duration_minutes"] = entries["duration_hours"] * 60.0
    if entries.empty:
        return (
            "<div class='hours-breakdown'>"
            f"<div class='hours-breakdown-header'><h3>{html.escape(title)}</h3></div>"
            "<div class='hours-breakdown-note'>No logged hours in this period.</div>"
            "</div>"
        )

    totals = (
        entries.groupby(["project_id", "project_name"], as_index=False)["duration_minutes"]
        .sum(min_count=1)
        .rename(columns={"duration_minutes": "total_minutes"})
    )
    totals["total_minutes"] = pd.to_numeric(totals["total_minutes"], errors="coerce").fillna(0.0)
    totals = totals.sort_values(["total_minutes", "project_name", "project_id"], ascending=[False, True, True])

    total_period_minutes = float(entries["duration_minutes"].sum()) if show_percentage else 0.0
    if show_percentage and total_period_minutes <= 0:
        show_percentage = False

    def fmt_pct(minutes: Any) -> str:
        if not show_percentage:
            return ""
        try:
            m = float(minutes or 0.0)
        except (TypeError, ValueError):
            m = 0.0
        return f"{(m * 100.0 / total_period_minutes):.1f}%"

    projects_html: List[str] = []
    for _, row in totals.iterrows():
        project_id = str(row.get("project_id", "")).strip()
        project_name = str(row.get("project_name", project_id)).strip() or project_id
        total_minutes = float(row.get("total_minutes", 0.0) or 0.0)

        project_entries = entries.loc[entries["project_id"] == project_id].copy()
        project_entries = project_entries.sort_values(["duration_minutes"], ascending=False)

        entry_rows: List[str] = []
        for _, entry in project_entries.iterrows():
            dur_text = _format_minutes_hhmm(entry.get("duration_minutes"))
            pct_text = fmt_pct(entry.get("duration_minutes"))
            desc_val = entry.get(what_i_did_col) if what_i_did_col else ""
            desc_html = _escape_html_multiline(desc_val).strip()
            if not desc_html:
                desc_html = "<span class='hours-entry-empty'>(no details)</span>"
            pct_cell = (
                f"<td class='hours-entry-percent'>{html.escape(pct_text)}</td>"
                if show_percentage
                else ""
            )
            entry_rows.append(
                "<tr>"
                f"<td class='hours-entry-duration'>{html.escape(dur_text)}</td>"
                f"{pct_cell}"
                f"<td>{desc_html}</td>"
                "</tr>"
            )

        pct_summary = f" <span class='hours-project-percent'>({html.escape(fmt_pct(total_minutes))})</span>" if show_percentage else ""
        summary_html = (
            f"<span class='hours-project-total'>{html.escape(_format_minutes_hhmm(total_minutes))}</span>, "
            f"<span class='hours-project-name'>{html.escape(project_name)}</span>"
            f"{pct_summary}"
        )

        header_pct = "<th>Percent</th>" if show_percentage else ""
        projects_html.append(
            "<details class='hours-project'>"
            f"<summary>{summary_html}</summary>"
            "<div class='hours-project-entries'>"
            "<table class='hours-entry-table'>"
            f"<thead><tr><th>Duration</th>{header_pct}<th>Details</th></tr></thead>"
            "<tbody>"
            + "".join(entry_rows)
            + "</tbody>"
            "</table>"
            "</div>"
            "</details>"
        )

    note_text = "Percentages are of the total logged time in this period." if show_percentage else "Click a project to expand."
    return (
        "<div class='hours-breakdown'>"
        "<div class='hours-breakdown-header'>"
        f"<h3>{html.escape(title)}</h3>"
        f"<div class='hours-breakdown-note'>{html.escape(note_text)}</div>"
        "</div>"
        "<div class='hours-breakdown-list'>"
        + "".join(projects_html)
        + "</div>"
        "</div>"
    )


# ----------------------------
# Plot builders
# ----------------------------
def axis_domain_ref(axis_letter: str, subplot_row: int) -> str:
    # Plotly uses "x domain"/"y domain" for the first subplot, not "x1 domain"/"y1 domain"
    return f"{axis_letter} domain" if subplot_row == 1 else f"{axis_letter}{subplot_row} domain"


def apply_axis_style(fig: go.Figure, total_rows: int) -> None:
    axis_style = dict(
        showgrid=True,
        gridwidth=1,
        gridcolor="rgba(0,0,0,0.15)",
        showline=True,
        linewidth=1,
        linecolor="rgba(0,0,0,0.8)",
        mirror=True,
        ticks="outside",
        zeroline=True,
        zerolinewidth=1,
        zerolinecolor="rgba(0,0,0,0.15)",
        automargin=True,
    )
    for row in range(1, total_rows + 1):
        fig.update_xaxes(**axis_style, row=row, col=1)
        fig.update_yaxes(**axis_style, row=row, col=1, tickmode="auto")


def _find_asset(prefix: str, exts: Tuple[str, ...]) -> Optional[str]:
    asset_dir = os.path.join(SCRIPT_DIR, ".assets")
    if not os.path.isdir(asset_dir):
        return None
    for filename in os.listdir(asset_dir):
        lower = filename.lower()
        if lower.startswith(prefix) and lower.endswith(exts):
            return os.path.join(asset_dir, filename)
    return None


def _encode_image_to_data_uri(img_path: str) -> Optional[str]:
    if not img_path or not os.path.exists(img_path):
        return None
    mime = "image/png"
    lower = img_path.lower()
    if lower.endswith(".svg"):
        mime = "image/svg+xml"
    elif lower.endswith((".jpg", ".jpeg")):
        mime = "image/jpeg"
    elif lower.endswith(".webp"):
        mime = "image/webp"
    with open(img_path, "rb") as f:
        encoded = base64.b64encode(f.read()).decode("ascii")
    return f"data:{mime};base64,{encoded}"


def add_teamnl_logo(fig: go.Figure) -> None:
    logo_path = _find_asset("teamnl_sport_science", (".png", ".jpg", ".jpeg", ".svg", ".webp"))
    data_uri = _encode_image_to_data_uri(logo_path) if logo_path else None
    if not data_uri:
        print("teamnl_sport_science logo not found in .assets; add it to include in the report.")
        return

    fig.add_layout_image(
        dict(
            source=data_uri,
            xref="paper",
            yref="paper",
            x=1.02,
            y=1.14,
            sizex=0.24,
            sizey=0.12,
            xanchor="right",
            yanchor="top",
            layer="above",
        )
    )


def build_header_assets() -> Dict[str, Optional[str]]:
    profile_path = _find_asset("profielfotos_nocnsf_square", (".png", ".jpg", ".jpeg", ".svg", ".webp"))
    teamnl_path = _find_asset("teamnl_sport_science", (".png", ".jpg", ".jpeg", ".svg", ".webp"))
    return {
        "profile_data_uri": _encode_image_to_data_uri(profile_path) if profile_path else None,
        "teamnl_data_uri": _encode_image_to_data_uri(teamnl_path) if teamnl_path else None,
    }


def add_profile_picture(fig: go.Figure) -> None:
    profile_path = _find_asset("profielfotos_nocnsf_square", (".png", ".jpg", ".jpeg", ".svg", ".webp"))
    data_uri = _encode_image_to_data_uri(profile_path) if profile_path else None
    if not data_uri:
        print("Profile picture (profielfotos_nocnsf_square.jpg) not found in .assets; add it to include in the report.")
        return

    fig.add_layout_image(
        dict(
            source=data_uri,
            xref="paper",
            yref="paper",
            x=0.82,
            y=1.14,
            sizex=0.12,
            sizey=0.12,
            xanchor="right",
            yanchor="top",
            layer="above",
        )
    )


def add_stacked_project_count_bars(
    fig: go.Figure,
    projects_df: pd.DataFrame,
    group_col: str,
    subplot_row: int,
    title: str,
    project_color_map: Dict[str, str],
) -> None:
    all_groups: set[str] = set()
    project_groups: List[Tuple[pd.Series, List[str]]] = []
    group_counts: Dict[str, int] = {}

    for _, project in projects_df.iterrows():
        values = extract_group_values(project, group_col)
        if not values:
            values = ["Unknown"]
        values = list(dict.fromkeys(values))  # preserve order, drop dupes
        project_groups.append((project, values))
        all_groups.update(values)
        for group_val in values:
            group_counts[group_val] = group_counts.get(group_val, 0) + 1

    groups = sorted(all_groups, key=lambda g: (-group_counts.get(g, 0), str(g)))
    fig.update_xaxes(categoryorder="array", categoryarray=groups, row=subplot_row, col=1)

    for project, values in project_groups:
        hover = build_hover_text(project)
        project_id = project.get("project_id")
        bar_name = str(project.get("project_name", project.get("project_id", "project")))

        for group_val in values:
            fig.add_trace(
                go.Bar(
                    x=[group_val],
                    y=[1],
                    name=bar_name,
                    hovertemplate=hover + "<extra></extra>",
                    marker_color=project_color_map.get(project_id, BASE_BLACK),
                    showlegend=False,
                ),
                row=subplot_row,
                col=1,
            )

    fig.update_yaxes(title_text="Project count", row=subplot_row, col=1)
    fig.add_annotation(
        text=f"<b>{title}</b>",
        x=0,
        xref="x domain",
        y=1.12,
        yref=axis_domain_ref("y", subplot_row),
        showarrow=False,
        align="left",
        row=subplot_row,
        col=1,
    )    


def add_stacked_hours_bars(
    fig: go.Figure,
    projects_df: pd.DataFrame,
    time_entries_df: pd.DataFrame,
    group_col: str,
    subplot_row: int,
    title: str,
    project_color_map: Dict[str, str],
) -> None:
    if time_entries_df.empty:
        fig.add_trace(
            go.Bar(x=["(no time_log data found)"], y=[0], hovertemplate="No time entries found.<extra></extra>", showlegend=False),
            row=subplot_row, col=1
        )
        fig.update_yaxes(title_text="Hours", row=subplot_row, col=1)
        fig.add_annotation(
            text=f"<b>{title}</b>",
            x=0,
            xref="x domain",
            y=1.12,
            yref=axis_domain_ref("y", subplot_row),
            showarrow=False,
            align="left",
            row=subplot_row,
            col=1,
        )        
        return

    project_hours = (
        time_entries_df.groupby("project_id", as_index=False)["duration_hours"]
        .sum()
        .rename(columns={"duration_hours": "total_hours"})
    )
    merged = projects_df.merge(project_hours, on="project_id", how="left")
    merged["total_hours"] = merged["total_hours"].fillna(0)

    all_groups: set[str] = set()
    project_groups: List[Tuple[pd.Series, List[str]]] = []
    group_hours: Dict[str, float] = {}

    for _, project in merged.iterrows():
        values = extract_group_values(project, group_col)
        if not values:
            values = ["Unknown"]
        values = list(dict.fromkeys(values))
        project_groups.append((project, values))
        all_groups.update(values)
        hours = float(project.get("total_hours", 0.0))
        for group_val in values:
            group_hours[group_val] = group_hours.get(group_val, 0.0) + hours

    groups = sorted(all_groups, key=lambda g: (-group_hours.get(g, 0.0), str(g)))
    fig.update_xaxes(categoryorder="array", categoryarray=groups, row=subplot_row, col=1)

    for project, values in project_groups:
        hours = float(project.get("total_hours", 0.0))
        hover = build_hover_text(project, extra={"total_hours": f"{hours:.2f}"})
        project_id = project.get("project_id")

        for group_val in values:
            fig.add_trace(
                go.Bar(
                    x=[group_val],
                    y=[hours],
                    name=str(project.get("project_name", project.get("project_id", "project"))),
                    hovertemplate=hover + "<extra></extra>",
                    marker_color=project_color_map.get(project_id, BASE_BLACK),
                    showlegend=False,
                ),
                row=subplot_row,
                col=1,
            )

    fig.update_yaxes(title_text="Hours", row=subplot_row, col=1)
    fig.add_annotation(
        text=f"<b>{title}</b>",
        x=0,
        xref="x domain",
        y=1.12,
        yref=axis_domain_ref("y", subplot_row),
        showarrow=False,
        align="left",
        row=subplot_row,
        col=1,
    )

def add_trend_started_closed(
    fig: go.Figure,
    projects_df: pd.DataFrame,
    subplot_row: int,
    title: str,
    project_color_map: Optional[Dict[str, str]] = None,
    target_year: Optional[int] = None,
) -> None:
    if target_year is None:
        target_year = date.today().year

    year_start = date(target_year, 1, 1)
    year_end = date(target_year, 12, 31)
    week_starts, week_ends, week_positions, bar_width_ms = build_year_week_grid(target_year)
    x_positions = week_positions.to_pydatetime().tolist() if len(week_positions) else []
    if week_starts.empty:
        fig.add_trace(
            go.Bar(x=[], y=[], hovertemplate="No weekly activity data.<extra></extra>"),
            row=subplot_row,
            col=1,
        )
        fig.add_annotation(
            text=f"<b>{title}</b> (no weeks found)",
            x=0,
            xref="x domain",
            y=1.12,
            yref=f"y{subplot_row} domain",
            showarrow=False,
            align="left",
            row=subplot_row,
            col=1,
        )
        fig.update_yaxes(title_text="Active projects", row=subplot_row, col=1)
        return

    project_rows = projects_df.dropna(subset=["start_date"]).copy()
    if project_rows.empty:
        fig.add_trace(
            go.Bar(x=[], y=[], hovertemplate="No weekly activity data.<extra></extra>"),
            row=subplot_row,
            col=1,
        )
        fig.add_annotation(
            text=f"<b>{title}</b> (no project dates found)",
            x=0,
            xref="x domain",
            y=1.12,
            yref=f"y{subplot_row} domain",
            showarrow=False,
            align="left",
            row=subplot_row,
            col=1,
        )
        fig.update_yaxes(title_text="Active projects", row=subplot_row, col=1)
        return

    project_rows["project_id"] = project_rows["project_id"].astype(str)
    project_rows = project_rows.set_index("project_id")

    def _resolve_expected_end(row: pd.Series) -> Optional[pd.Timestamp]:
        expected = row.get("expected_end_date")
        expected = parse_date(expected) if expected is not None else None
        if expected is not None:
            return expected
        target_end = row.get("target_end_date")
        if pd.notna(target_end):
            return pd.Timestamp(target_end)
        return None

    def _project_sort_key(project_id: str) -> Tuple[int, str]:
        try:
            return int(project_id.replace("_", "")), project_id
        except (TypeError, ValueError):
            return float("inf"), project_id

    ordered_projects = sorted(project_rows.index.tolist(), key=_project_sort_key)
    color_map = project_color_map or {}

    for project_id in ordered_projects:
        row = project_rows.loc[project_id]
        project_start = row.get("start_date")
        if pd.isna(project_start):
            continue
        project_start = pd.Timestamp(project_start)

        status_val = str(row.get("status", "Active")).strip()
        if status_val == "Closed":
            project_end = row.get("actual_end_date")
            if pd.isna(project_end):
                continue
            project_end = pd.Timestamp(project_end)
            alpha = 0.18
        else:
            expected_end = _resolve_expected_end(row)
            project_end = expected_end if expected_end is not None else pd.Timestamp(year_end)
            alpha = 1.0 if status_val == "Active" else 0.25

        active = (week_ends >= project_start) & (week_starts <= project_end)
        if not active.any():
            continue
        y_vals = active.astype(int).tolist()

        base_color = color_map.get(project_id, BASE_BLACK)
        marker_color = base_color if alpha >= 1.0 else hex_to_rgba(base_color, alpha)
        tick_text = ["✓" if v == 1 else "" for v in y_vals] if status_val == "Closed" else None
        hover_text = build_hover_text(row)

        fig.add_trace(
            go.Bar(
                x=x_positions,
                y=y_vals,
                name=project_id,
                width=[bar_width_ms] * len(y_vals),
                marker_color=marker_color,
                text=tick_text,
                textposition="inside",
                textfont=dict(color=BASE_GREEN, size=14),
                hovertext=[hover_text] * len(y_vals),
                hovertemplate="%{hovertext}<br>Week starting %{x|%Y-%m-%d}<br>Active=%{y}<extra></extra>",
                showlegend=False,
            ),
            row=subplot_row,
            col=1,
        )

    fig.update_yaxes(title_text="Active projects", row=subplot_row, col=1)
    fig.update_xaxes(title_text="Week", row=subplot_row, col=1, type="date")
    fig.add_annotation(
        text=f"<b>{title}</b>",
        x=0,
        xref="x domain",
        y=1.12,
        yref=f"y{subplot_row} domain",
        showarrow=False,
        align="left",
        row=subplot_row,
        col=1,
    )


def add_hours_per_week(
    fig: go.Figure,
    projects_df: pd.DataFrame,
    time_entries_df: pd.DataFrame,
    subplot_row: int,
    title: str,
    project_color_map: Optional[Dict[str, str]] = None,
    display_start: Optional[date] = None,
    display_end: Optional[date] = None,
    data_start: Optional[date] = None,
    data_end: Optional[date] = None,
) -> None:
    if display_start is None or display_end is None:
        display_start = date.today().replace(month=1, day=1)
        display_end = date.today()
    if data_start is None:
        data_start = display_start
    if data_end is None:
        data_end = display_end

    week_starts, week_ends, week_positions, bar_width_ms = build_period_week_grid(display_start, display_end)
    x_positions = week_positions.to_pydatetime().tolist() if len(week_positions) else []
    if week_starts.empty:
        fig.add_trace(
            go.Bar(x=[], y=[], hovertemplate="No weekly hours data.<extra></extra>"),
            row=subplot_row,
            col=1,
        )
        fig.add_annotation(
            text=f"<b>{title}</b> (no weeks found)",
            x=0,
            xref="x domain",
            y=1.12,
            yref=f"y{subplot_row} domain",
            showarrow=False,
            align="left",
            row=subplot_row,
            col=1,
        )
        fig.update_yaxes(title_text="Hours", row=subplot_row, col=1)
        fig.update_xaxes(title_text="Week", row=subplot_row, col=1, type="date")
        return

    required_cols = {"project_id", "date", "duration_hours"}
    if time_entries_df.empty or not required_cols.issubset(time_entries_df.columns):
        fig.add_trace(
            go.Scatter(x=[x_positions[0]] if x_positions else [], y=[0], mode="markers", marker_opacity=0, showlegend=False),
            row=subplot_row,
            col=1,
        )
        fig.add_annotation(
            text=f"<b>{title}</b> (no time entries)",
            x=0,
            xref="x domain",
            y=1.12,
            yref=f"y{subplot_row} domain",
            showarrow=False,
            align="left",
            row=subplot_row,
            col=1,
        )
        fig.update_yaxes(title_text="Hours", row=subplot_row, col=1)
        fig.update_xaxes(title_text="Week", row=subplot_row, col=1, type="date")
        return

    entries = time_entries_df.dropna(subset=["project_id", "date", "duration_hours"]).copy()
    entries["project_id"] = entries["project_id"].astype(str)
    entries["duration_hours"] = pd.to_numeric(entries["duration_hours"], errors="coerce")
    entries = entries.dropna(subset=["duration_hours"])
    entries = entries[(entries["date"] >= pd.Timestamp(data_start)) & (entries["date"] <= pd.Timestamp(data_end))]
    if entries.empty:
        fig.add_trace(
            go.Scatter(x=[x_positions[0]] if x_positions else [], y=[0], mode="markers", marker_opacity=0, showlegend=False),
            row=subplot_row,
            col=1,
        )
        fig.add_annotation(
            text=f"<b>{title}</b> (no time entries)",
            x=0,
            xref="x domain",
            y=1.12,
            yref=f"y{subplot_row} domain",
            showarrow=False,
            align="left",
            row=subplot_row,
            col=1,
        )
        fig.update_yaxes(title_text="Hours", row=subplot_row, col=1)
        fig.update_xaxes(title_text="Week", row=subplot_row, col=1, type="date")
        return

    entries["__week_start"] = entries["date"].dt.normalize() - pd.to_timedelta(entries["date"].dt.weekday, unit="D")
    weekly_hours = (
        entries.groupby(["project_id", "__week_start"], as_index=False)["duration_hours"]
        .sum(min_count=1)
        .rename(columns={"duration_hours": "week_hours"})
    )
    weekly_hours["week_hours"] = pd.to_numeric(weekly_hours["week_hours"], errors="coerce").fillna(0.0)
    totals_by_project = (
        weekly_hours.groupby("project_id")["week_hours"]
        .sum(min_count=1)
        .fillna(0.0)
    )

    project_rows = projects_df.dropna(subset=["start_date"]).copy()

    project_rows["project_id"] = project_rows["project_id"].astype(str)
    project_rows = project_rows.set_index("project_id")

    def _resolve_expected_end(row: pd.Series) -> Optional[pd.Timestamp]:
        expected = row.get("expected_end_date")
        expected = parse_date(expected) if expected is not None else None
        if expected is not None:
            return expected
        target_end = row.get("target_end_date")
        if pd.notna(target_end):
            return pd.Timestamp(target_end)
        return None

    def _project_sort_key(project_id: str) -> Tuple[int, str]:
        try:
            return int(project_id.replace("_", "")), project_id
        except (TypeError, ValueError):
            return float("inf"), project_id

    ordered_projects = sorted(totals_by_project.index.tolist(), key=_project_sort_key)
    color_map = project_color_map or {}

    week_index_map = {pd.Timestamp(ws): idx for idx, ws in enumerate(week_starts)}
    added_trace = False

    for project_id in ordered_projects:
        total_hours = float(totals_by_project.get(project_id, 0.0) or 0.0)
        if total_hours <= 0:
            continue

        if project_id in project_rows.index:
            row = project_rows.loc[project_id]
        else:
            row = pd.Series({"project_id": project_id, "project_name": project_id})

        status_val = str(row.get("status", "Active")).strip()
        if status_val == "Closed":
            alpha = 0.18
        else:
            alpha = 1.0 if status_val == "Active" else 0.25

        y_vals = [0.0] * len(week_starts)
        project_weekly = weekly_hours.loc[weekly_hours["project_id"] == project_id]
        for _, wrow in project_weekly.iterrows():
            week_start = wrow.get("__week_start")
            if pd.isna(week_start):
                continue
            idx = week_index_map.get(pd.Timestamp(week_start))
            if idx is None:
                continue
            y_vals[idx] = float(wrow.get("week_hours", 0.0) or 0.0)

        if not any(v > 0 for v in y_vals):
            continue

        base_color = color_map.get(project_id, BASE_BLACK)
        marker_color = base_color if alpha >= 1.0 else hex_to_rgba(base_color, alpha)
        tick_text = ["✓" if v > 0 else "" for v in y_vals] if status_val == "Closed" else None
        hover_text = build_hover_text(row, extra={"period_hours": f"{total_hours:.2f}"})

        fig.add_trace(
            go.Bar(
                x=x_positions,
                y=y_vals,
                name=project_id,
                width=[bar_width_ms] * len(y_vals),
                marker_color=marker_color,
                text=tick_text,
                textposition="inside",
                textfont=dict(color=BASE_GREEN, size=14),
                hovertext=[hover_text] * len(y_vals),
                hovertemplate="%{hovertext}<br>Week starting %{x|%Y-%m-%d}<br>Hours=%{y:.2f}<extra></extra>",
                showlegend=False,
            ),
            row=subplot_row,
            col=1,
        )
        added_trace = True

    if not added_trace:
        fig.add_trace(
            go.Scatter(x=[x_positions[0]] if x_positions else [], y=[0], mode="markers", marker_opacity=0, showlegend=False),
            row=subplot_row,
            col=1,
        )
        fig.add_annotation(
            text="No reported hours data.",
            x=0.5,
            xref="x domain",
            y=0.5,
            yref=f"y{subplot_row} domain",
            showarrow=False,
            align="center",
            row=subplot_row,
            col=1,
        )

    fig.update_yaxes(title_text="Hours", row=subplot_row, col=1)
    fig.update_xaxes(title_text="Week", row=subplot_row, col=1, type="date")
    fig.add_annotation(
        text=f"<b>{title}</b>",
        x=0,
        xref="x domain",
        y=1.12,
        yref=f"y{subplot_row} domain",
        showarrow=False,
        align="left",
        row=subplot_row,
        col=1,
    )


def add_estimated_magnitude_per_week(
    fig: go.Figure,
    projects_df: pd.DataFrame,
    subplot_row: int,
    title: str,
    project_color_map: Optional[Dict[str, str]] = None,
    period_start: Optional[date] = None,
    period_end: Optional[date] = None,
) -> None:
    if period_start is None or period_end is None:
        period_start = date.today().replace(month=1, day=1)
        period_end = date.today()
    year_end = period_end
    week_starts, week_ends, week_positions, bar_width_ms = build_period_week_grid(period_start, period_end)
    x_positions = week_positions.to_pydatetime().tolist() if len(week_positions) else []
    if week_starts.empty:
        fig.add_trace(
            go.Bar(x=[], y=[], hovertemplate="No weekly magnitude data.<extra></extra>"),
            row=subplot_row,
            col=1,
        )
        fig.add_annotation(
            text=f"<b>{title}</b> (no weeks found)",
            x=0,
            xref="x domain",
            y=1.12,
            yref=f"y{subplot_row} domain",
            showarrow=False,
            align="left",
            row=subplot_row,
            col=1,
        )
        fig.update_yaxes(title_text="Estimated magnitude", row=subplot_row, col=1)
        fig.update_xaxes(title_text="Week", row=subplot_row, col=1, type="date")
        return

    project_rows = projects_df.dropna(subset=["start_date"]).copy()
    if project_rows.empty:
        fig.add_trace(
            go.Bar(x=[], y=[], hovertemplate="No weekly magnitude data.<extra></extra>"),
            row=subplot_row,
            col=1,
        )
        fig.add_annotation(
            text=f"<b>{title}</b> (no project dates found)",
            x=0,
            xref="x domain",
            y=1.12,
            yref=f"y{subplot_row} domain",
            showarrow=False,
            align="left",
            row=subplot_row,
            col=1,
        )
        fig.update_yaxes(title_text="Estimated magnitude", row=subplot_row, col=1)
        fig.update_xaxes(title_text="Week", row=subplot_row, col=1, type="date")
        return

    project_rows["project_id"] = project_rows["project_id"].astype(str)
    project_rows = project_rows.set_index("project_id")

    def _resolve_expected_end(row: pd.Series) -> Optional[pd.Timestamp]:
        expected = row.get("expected_end_date")
        expected = parse_date(expected) if expected is not None else None
        if expected is not None:
            return expected
        target_end = row.get("target_end_date")
        if pd.notna(target_end):
            return pd.Timestamp(target_end)
        return None

    def _project_sort_key(project_id: str) -> Tuple[int, str]:
        try:
            return int(project_id.replace("_", "")), project_id
        except (TypeError, ValueError):
            return float("inf"), project_id

    ordered_projects = sorted(project_rows.index.tolist(), key=_project_sort_key)
    color_map = project_color_map or {}

    for project_id in ordered_projects:
        row = project_rows.loc[project_id]
        project_start = row.get("start_date")
        if pd.isna(project_start):
            continue
        project_start = pd.Timestamp(project_start)

        status_val = str(row.get("status", "Active")).strip()
        if status_val == "Closed":
            project_end = row.get("actual_end_date")
            if pd.isna(project_end):
                continue
            project_end = pd.Timestamp(project_end)
            alpha = 0.18
        else:
            expected_end = _resolve_expected_end(row)
            project_end = expected_end if expected_end is not None else pd.Timestamp(year_end)
            alpha = 1.0 if status_val == "Active" else 0.25

        active = (week_ends >= project_start) & (week_starts <= project_end)
        total_week_start = project_start - pd.Timedelta(days=project_start.weekday())
        total_week_end = project_end - pd.Timedelta(days=project_end.weekday())
        total_weeks = pd.date_range(total_week_start, total_week_end, freq="W-MON")
        total_active_weeks = len(total_weeks)
        if total_active_weeks == 0:
            continue

        magnitude_value = row.get("estimated_magnitude")
        weight = estimate_magnitude_weight(magnitude_value)
        per_week = weight / total_active_weeks
        y_vals = [per_week if is_active else 0.0 for is_active in active.tolist()]

        base_color = color_map.get(project_id, BASE_BLACK)
        marker_color = base_color if alpha >= 1.0 else hex_to_rgba(base_color, alpha)
        tick_text = ["✓" if v > 0 else "" for v in y_vals] if status_val == "Closed" else None
        hover_text = build_hover_text(row, extra={"estimated_magnitude": magnitude_value, "weight": weight})

        fig.add_trace(
            go.Bar(
                x=x_positions,
                y=y_vals,
                name=project_id,
                width=[bar_width_ms] * len(y_vals),
                marker_color=marker_color,
                text=tick_text,
                textposition="inside",
                textfont=dict(color=BASE_GREEN, size=14),
                hovertext=[hover_text] * len(y_vals),
                hovertemplate="%{hovertext}<br>Week starting %{x|%Y-%m-%d}<br>Weight=%{y:.2f}<extra></extra>",
                showlegend=False,
            ),
            row=subplot_row,
            col=1,
        )

    fig.update_yaxes(title_text="Estimated magnitude", row=subplot_row, col=1)
    fig.update_xaxes(title_text="Week", row=subplot_row, col=1, type="date")
    fig.add_annotation(
        text=f"<b>{title}</b>",
        x=0,
        xref="x domain",
        y=1.12,
        yref=f"y{subplot_row} domain",
        showarrow=False,
        align="left",
        row=subplot_row,
        col=1,
    )


def add_reported_hours_per_project(
    fig: go.Figure,
    projects_df: pd.DataFrame,
    time_entries_df: pd.DataFrame,
    subplot_row: int,
    title: str,
    project_color_map: Optional[Dict[str, str]] = None,
) -> None:
    if time_entries_df.empty or "duration_hours" not in time_entries_df.columns:
        fig.add_trace(
            go.Bar(x=["(no reported hours)"], y=[0], hovertemplate="No reported hours data.<extra></extra>", showlegend=False),
            row=subplot_row,
            col=1,
        )
        fig.update_yaxes(title_text="Hours", row=subplot_row, col=1)
        fig.add_annotation(
            text=f"<b>{title}</b>",
            x=0,
            xref="x domain",
            y=1.12,
            yref=axis_domain_ref("y", subplot_row),
            showarrow=False,
            align="left",
            row=subplot_row,
            col=1,
        )
        return

    hours_by_project = (
        time_entries_df.groupby("project_id", as_index=False)["duration_hours"]
        .sum()
        .rename(columns={"duration_hours": "total_hours"})
    )
    if hours_by_project.empty:
        fig.add_trace(
            go.Bar(x=["(no reported hours)"], y=[0], hovertemplate="No reported hours data.<extra></extra>", showlegend=False),
            row=subplot_row,
            col=1,
        )
        fig.update_yaxes(title_text="Hours", row=subplot_row, col=1)
        fig.add_annotation(
            text=f"<b>{title}</b>",
            x=0,
            xref="x domain",
            y=1.12,
            yref=axis_domain_ref("y", subplot_row),
            showarrow=False,
            align="left",
            row=subplot_row,
            col=1,
        )
        return

    hours_by_project["project_id"] = hours_by_project["project_id"].astype(str)
    project_rows = projects_df.copy()
    project_rows["project_id"] = project_rows["project_id"].astype(str)
    merged = project_rows.merge(hours_by_project, on="project_id", how="right")
    merged = merged[merged["total_hours"] > 0].copy()

    if merged.empty:
        fig.add_trace(
            go.Bar(x=["(no reported hours)"], y=[0], hovertemplate="No reported hours data.<extra></extra>", showlegend=False),
            row=subplot_row,
            col=1,
        )
        fig.update_yaxes(title_text="Hours", row=subplot_row, col=1)
        fig.add_annotation(
            text=f"<b>{title}</b>",
            x=0,
            xref="x domain",
            y=1.12,
            yref=axis_domain_ref("y", subplot_row),
            showarrow=False,
            align="left",
            row=subplot_row,
            col=1,
        )
        return

    merged = merged.sort_values("total_hours", ascending=False)
    labels: List[str] = []
    colors: List[str] = []
    hovers: List[str] = []
    hours: List[float] = []
    color_map = project_color_map or {}

    for _, row in merged.iterrows():
        project_id = str(row.get("project_id", "")).strip()
        project_name = str(row.get("project_name", project_id)).strip()
        label = f"{project_name} ({project_id})" if project_name else project_id
        labels.append(label)
        hours.append(float(row.get("total_hours", 0.0)))
        colors.append(color_map.get(project_id, BASE_BLACK))
        hovers.append(build_hover_text(row, extra={"total_hours": f"{row.get('total_hours', 0.0):.2f}"}))

    fig.add_trace(
        go.Bar(
            x=labels,
            y=hours,
            marker_color=colors,
            hovertext=hovers,
            hovertemplate="%{hovertext}<br>Total hours=%{y:.2f}<extra></extra>",
            showlegend=False,
        ),
        row=subplot_row,
        col=1,
    )
    fig.update_xaxes(categoryorder="array", categoryarray=labels, row=subplot_row, col=1)
    fig.update_yaxes(title_text="Hours", row=subplot_row, col=1)
    fig.add_annotation(
        text=f"<b>{title}</b>",
        x=0,
        xref="x domain",
        y=1.12,
        yref=axis_domain_ref("y", subplot_row),
        showarrow=False,
        align="left",
        row=subplot_row,
        col=1,
    )


def add_nn_summary_bars(
    fig: go.Figure,
    nn_summary: Optional[Dict[str, Any]],
    subplot_row: int,
    title: str,
) -> None:
    if not nn_summary or nn_summary.get("billed") is None or nn_summary.get("remaining") is None:
        fig.add_trace(
            go.Bar(x=["NN summary unavailable"], y=[0], hovertemplate="No NN summary available.<extra></extra>", showlegend=False),
            row=subplot_row,
            col=1,
        )
        fig.update_yaxes(title_text="Hours", row=subplot_row, col=1)
        fig.add_annotation(
            text=f"<b>{title}</b>",
            x=0,
            xref="x domain",
            y=1.12,
            yref=axis_domain_ref("y", subplot_row),
            showarrow=False,
            align="left",
            row=subplot_row,
            col=1,
        )
        return

    billed = float(nn_summary.get("billed", 0.0))
    remaining = float(nn_summary.get("remaining", 0.0))
    fig.add_trace(
        go.Bar(x=["Gefactureerd"], y=[billed], marker_color=BASE_BLUE, showlegend=False),
        row=subplot_row,
        col=1,
    )
    fig.add_trace(
        go.Bar(x=["Resterend"], y=[remaining], marker_color=BASE_YELLOW, showlegend=False),
        row=subplot_row,
        col=1,
    )
    fig.update_yaxes(title_text="Hours", row=subplot_row, col=1)
    fig.add_annotation(
        text=f"<b>{title}</b>",
        x=0,
        xref="x domain",
        y=1.12,
        yref=axis_domain_ref("y", subplot_row),
        showarrow=False,
        align="left",
        row=subplot_row,
        col=1,
    )


def build_counts_figure(
    projects_df: pd.DataFrame,
    export_date: str,
    period_start: date,
    period_end: date,
    period_label: str,
    project_color_map: Optional[Dict[str, str]] = None,
    timeline_projects_df: Optional[pd.DataFrame] = None,
    timeline_year: Optional[int] = None,
) -> go.Figure:
    total_rows = 4
    if project_color_map is None:
        _, project_color_map = build_color_maps(projects_df)
    if timeline_projects_df is None:
        timeline_projects_df = projects_df
    fig = make_subplots(rows=total_rows, cols=1, shared_xaxes=False, vertical_spacing=0.10)

    add_stacked_project_count_bars(fig, projects_df, "programma", 1,
                                   "Projects per programma (stacked: each project = 1 block)",
                                   project_color_map)
    add_stacked_project_count_bars(fig, projects_df, "theme", 2,
                                   "Projects per theme (stacked: each project = 1 block)",
                                   project_color_map)
    add_stacked_project_count_bars(fig, projects_df, "requester", 3,
                                   "Projects per requester (stacked: each project = 1 block)",
                                   project_color_map)
    add_trend_started_closed(
        fig,
        timeline_projects_df,
        4,
        "Active projects per week (stacked by project)",
        project_color_map,
        target_year=timeline_year,
    )

    apply_axis_style(fig, total_rows)
    fig.update_layout(
        barmode="stack",
        height=1600,
        margin=dict(l=60, r=60, t=40, b=60),
        plot_bgcolor="rgba(255,255,255,1)",
        paper_bgcolor="rgba(250,250,250,1)",
        hoverlabel=dict(namelength=-1),
        showlegend=False,
    )
    return fig


def build_hours_figure(
    projects_df: pd.DataFrame,
    time_entries_df_filtered: pd.DataFrame,
    export_date: str,
    period_start: date,
    period_end: date,
    period_label: str,
    report_type: str,
) -> go.Figure:
    if report_type in ("weekly", "biweekly"):
        total_rows = 4
    else:
        total_rows = 6
    _, project_color_map = build_color_maps(projects_df)
    vertical_spacing = 0.10 if report_type in ("weekly", "biweekly") else 0.09
    fig = make_subplots(rows=total_rows, cols=1, shared_xaxes=False, vertical_spacing=vertical_spacing)

    display_start = period_start
    display_end = period_end
    if report_type == "yearly":
        display_end = date(period_start.year, 12, 31)

    add_stacked_hours_bars(fig, projects_df, time_entries_df_filtered, "programma", 1,
                           "Hours per programma (stacked: each project contributes its hours)",
                           project_color_map)
    add_stacked_hours_bars(fig, projects_df, time_entries_df_filtered, "theme", 2,
                           "Hours per theme (stacked: each project contributes its hours)",
                           project_color_map)
    add_stacked_hours_bars(fig, projects_df, time_entries_df_filtered, "requester", 3,
                           "Hours per requester (stacked: each project contributes its hours)",
                           project_color_map)
    if report_type in ("weekly", "biweekly"):
        add_reported_hours_per_project(fig, projects_df, time_entries_df_filtered, 4,
                                       "Reported hours per project", project_color_map)
    else:
        add_estimated_magnitude_per_week(
            fig,
            projects_df,
            4,
            "Estimated magnitude per week (stacked by project)",
            project_color_map,
            period_start=display_start,
            period_end=display_end,
        )
        add_hours_per_week(
            fig,
            projects_df,
            time_entries_df_filtered,
            5,
            "Reported hours per week (stacked by project)",
            project_color_map,
            display_start=display_start,
            display_end=display_end,
            data_start=period_start,
            data_end=period_end,
        )
        add_reported_hours_per_project(
            fig,
            projects_df,
            time_entries_df_filtered,
            6,
            "Reported hours per project",
            project_color_map,
        )

    apply_axis_style(fig, total_rows)
    fig.update_layout(
        barmode="stack",
        height=1600 if report_type in ("weekly", "biweekly") else 2300,
        margin=dict(l=60, r=60, t=40, b=60),
        plot_bgcolor="rgba(255,255,255,1)",
        paper_bgcolor="rgba(250,250,250,1)",
        hoverlabel=dict(namelength=-1),
        showlegend=False,
    )
    return fig


def _to_float_list(values: Any) -> List[float]:
    out: List[float] = []
    if values is None:
        return out
    for v in list(values):
        try:
            out.append(float(v))
        except (TypeError, ValueError):
            out.append(0.0)
    return out


def build_percentage_figure_from_hours(hours_fig: go.Figure) -> go.Figure:
    fig = go.Figure(hours_fig.to_dict())

    totals_by_axis: Dict[str, float] = {}
    for trace in fig.data:
        if getattr(trace, "type", None) != "bar":
            continue
        axis_id = getattr(trace, "yaxis", None) or "y"
        y_vals = _to_float_list(getattr(trace, "y", None))
        totals_by_axis[axis_id] = totals_by_axis.get(axis_id, 0.0) + sum(y_vals)

    for trace in fig.data:
        if getattr(trace, "type", None) != "bar":
            continue
        axis_id = getattr(trace, "yaxis", None) or "y"
        denom = totals_by_axis.get(axis_id, 0.0)
        if denom <= 0:
            continue

        y_orig = _to_float_list(getattr(trace, "y", None))
        trace.customdata = y_orig
        trace.y = [v * 100.0 / denom for v in y_orig]

        ht = getattr(trace, "hovertemplate", None) or ""
        if ht and "<extra></extra>" in ht:
            body, _ = ht.split("<extra></extra>", 1)
        else:
            body = ht

        if "%{y" in body:
            body = (
                body.replace("Total hours=%{y:.2f}", "Total hours=%{customdata:.2f}")
                .replace("Hours=%{y:.2f}", "Hours=%{customdata:.2f}")
                .replace("Weight=%{y:.2f}", "Weight=%{customdata:.2f}")
            )
        if "Percent=%{y" not in body:
            body = body + "<br>Percent=%{y:.1f}%"
        trace.hovertemplate = body + "<extra></extra>"

    def _update_axis(axis: Any) -> None:
        axis.title = dict(text="Percent")
        axis.ticksuffix = "%"
        axis.tickformat = ".0f"
        axis.range = [0, 100]

    fig.for_each_yaxis(_update_axis)
    return fig


# ----------------------------
# Export
# ----------------------------
def write_tabbed_html(
    counts_fig: go.Figure,
    hours_fig: go.Figure,
    percentage_fig: go.Figure,
    out_html_path: str,
    header_context: Dict[str, Any],
    tables_html: str,
    hours_metrics_html: str,
    percentage_metrics_html: str,
    nn_pie_html: str,
    nn_note: Optional[str],
) -> None:
    counts_html = pio.to_html(counts_fig, include_plotlyjs=False, full_html=False, div_id="counts-fig")
    hours_html = pio.to_html(hours_fig, include_plotlyjs=False, full_html=False, div_id="hours-fig")
    percentage_html = pio.to_html(percentage_fig, include_plotlyjs=False, full_html=False, div_id="percentage-fig")
    plotly_cdn = _plotly_cdn_src()

    title_text = html.escape(str(header_context.get("title_text", "Project Portfolio Overview")))
    export_date = html.escape(str(header_context.get("export_date", "")))
    period_label = html.escape(str(header_context.get("period_label", "")))
    period_range = html.escape(str(header_context.get("period_range", "")))

    profile_uri = header_context.get("profile_data_uri")
    teamnl_uri = header_context.get("teamnl_data_uri")

    profile_img_html = f"<img class='profile-img' src='{profile_uri}' alt='Profile'/>" if profile_uri else ""
    teamnl_img_html = f"<img class='teamnl-img' src='{teamnl_uri}' alt='TeamNL'/>" if teamnl_uri else ""
    nn_note_html = f"<div class='nn-note'>{html.escape(nn_note)}</div>" if nn_note else ""
    nn_pie_block_html = (
        "<div class='nn-pie-block'>"
        "<div class='nn-pie-title'>Billed (to date)<br>vs remaining</div>"
        f"{nn_pie_html}"
        "</div>"
        if nn_pie_html
        else ""
    )

    html_content = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>{title_text}</title>
  <script src="{plotly_cdn}"></script>
  <style>
    body {{
      font-family: "Segoe UI", Tahoma, sans-serif;
      margin: 0;
      background: #FAFAFA;
      color: #111;
    }}
    .page {{
      padding: 24px 28px 40px;
    }}
    .sticky-header {{
      position: sticky;
      top: 0;
      z-index: 50;
      background: #FAFAFA;
      padding-top: 8px;
      box-shadow: 0 2px 6px rgba(0,0,0,0.08);
    }}
    .report-header {{
      display: flex;
      justify-content: space-between;
      gap: 24px;
      align-items: flex-start;
      flex-wrap: wrap;
      padding: 16px 0 8px;
    }}
    .header-left h1 {{
      margin: 0 0 6px 0;
      font-size: 26px;
    }}
    .header-left .meta {{
      font-size: 14px;
      color: #444;
    }}
    .header-right {{
      display: flex;
      gap: 16px;
      align-items: center;
    }}
    .nn-pie-block {{
      display: flex;
      align-items: center;
      gap: 6px;
    }}
    .nn-pie-title {{
      writing-mode: vertical-rl;
      transform: rotate(180deg);
      font-size: 12px;
      color: #111;
      white-space: nowrap;
    }}
    .profile-img {{
      width: 120px;
      height: 120px;
      object-fit: cover;
      border-radius: 10px;
      border: 2px solid #EEE;
      background: #FFF;
    }}
    .teamnl-img {{
      height: 64px;
      object-fit: contain;
    }}
    .tabs {{
      display: flex;
      gap: 8px;
      margin: 4px 0 12px;
      padding-bottom: 12px;
      flex-wrap: wrap;
    }}
    .tab-btn {{
      padding: 8px 16px;
      border: 1px solid #CCC;
      border-radius: 6px;
      background: #FFF;
      cursor: pointer;
      font-weight: 600;
    }}
    .tab-btn.active {{
      background: #01378A;
      border-color: #01378A;
      color: #FFF;
    }}
    .tab-panel {{
      display: none;
    }}
    .tab-panel.active {{
      display: block;
    }}
    .hours-metrics {{
      margin: 6px 0 16px;
    }}
    .hours-breakdown {{
      background: #FFF;
      border: 1px solid #DDD;
      border-radius: 10px;
      padding: 12px;
      box-shadow: 0 1px 2px rgba(0,0,0,0.05);
    }}
    .hours-breakdown-header {{
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      gap: 10px;
      flex-wrap: wrap;
      margin-bottom: 8px;
    }}
    .hours-breakdown h3 {{
      margin: 0;
      font-size: 16px;
    }}
    .hours-breakdown-note {{
      font-size: 12px;
      color: #444;
    }}
    .hours-breakdown-list {{
      display: flex;
      flex-direction: column;
      gap: 6px;
    }}
    .hours-project {{
      background: #FFF;
      border: 1px solid #EEE;
      border-radius: 8px;
      padding: 6px 10px;
    }}
    .hours-project[open] {{
      border-color: #CCC;
      box-shadow: 0 1px 2px rgba(0,0,0,0.05);
    }}
    .hours-project summary {{
      cursor: pointer;
      font-weight: 600;
      list-style: none;
      outline: none;
    }}
    .hours-project summary::-webkit-details-marker {{
      display: none;
    }}
    .hours-project summary::before {{
      content: "▸";
      display: inline-block;
      width: 1em;
      color: #01378A;
    }}
    .hours-project[open] summary::before {{
      content: "▾";
    }}
    .hours-project-total {{
      font-variant-numeric: tabular-nums;
    }}
    .hours-project-percent {{
      color: #444;
      font-weight: 600;
    }}
    .hours-project-entries {{
      margin-top: 8px;
      padding-left: 1.2em;
    }}
    .hours-entry-table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 12px;
    }}
    .hours-entry-table th {{
      text-align: left;
      padding: 6px 6px;
      color: #555;
      border-bottom: 1px solid #EEE;
    }}
    .hours-entry-table td {{
      padding: 6px 6px;
      border-bottom: 1px solid #F3F3F3;
      vertical-align: top;
      word-break: break-word;
    }}
    .hours-entry-duration {{
      width: 110px;
      text-align: right;
      font-variant-numeric: tabular-nums;
      white-space: nowrap;
    }}
    .hours-entry-percent {{
      width: 90px;
      text-align: right;
      font-variant-numeric: tabular-nums;
      white-space: nowrap;
    }}
    .hours-entry-empty {{
      color: #777;
      font-style: italic;
    }}
    .nn-metrics {{
      display: flex;
      flex-wrap: wrap;
      gap: 12px 18px;
      font-size: 14px;
      background: #FFF;
      border: 1px solid #DDD;
      border-radius: 8px;
      padding: 10px 12px;
      box-shadow: 0 1px 2px rgba(0,0,0,0.05);
    }}
    .nn-note {{
      margin-top: 6px;
      font-size: 13px;
      color: #8A3B3B;
    }}
    .project-info-section {{
      margin-top: 28px;
    }}
    .project-cards {{
      display: flex;
      gap: 16px;
      overflow-x: auto;
      padding-bottom: 8px;
    }}
    .project-card {{
      min-width: 280px;
      background: #FFF;
      border: 1px solid #DDD;
      border-radius: 10px;
      padding: 12px;
      box-shadow: 0 1px 2px rgba(0,0,0,0.05);
    }}
    .project-card-header {{
      font-weight: 700;
      margin-bottom: 8px;
    }}
    .project-card table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 12px;
    }}
    .project-card td {{
      padding: 3px 4px;
      border-bottom: 1px solid #EEE;
      vertical-align: top;
      word-break: break-word;
    }}
    .project-card td:first-child {{
      width: 45%;
      color: #555;
    }}
  </style>
</head>
<body>
  <div class="page">
    <div class="sticky-header">
      <div class="report-header">
        <div class="header-left">
          <h1>{title_text}</h1>
          <div class="meta"><b>{period_label}</b> — {period_range}</div>
          <div class="meta">Generated: {export_date}</div>
          {nn_note_html}
        </div>
        <div class="header-right">
          {nn_pie_block_html}
          {teamnl_img_html}
          {profile_img_html}          
        </div>
      </div>

      <div class="tabs">
        <button class="tab-btn active" id="btn-counts" onclick="showTab('counts')">Counts</button>
        <button class="tab-btn" id="btn-hours" onclick="showTab('hours')">Hours</button>
        <button class="tab-btn" id="btn-percentage" onclick="showTab('percentage')">Percentage</button>
      </div>
    </div>

    <div class="tab-panel active" id="tab-counts">
      {counts_html}
    </div>
    <div class="tab-panel" id="tab-hours">
      <div class="hours-metrics">{hours_metrics_html}</div>
      {hours_html}
    </div>
    <div class="tab-panel" id="tab-percentage">
      <div class="hours-metrics">{percentage_metrics_html}</div>
      {percentage_html}
    </div>

    <div class="project-info-section">
      <h2>Project details</h2>
      {tables_html}
    </div>
  </div>

  <script>
    function showTab(name) {{
      document.getElementById("tab-counts").classList.remove("active");
      document.getElementById("tab-hours").classList.remove("active");
      document.getElementById("tab-percentage").classList.remove("active");
      document.getElementById("btn-counts").classList.remove("active");
      document.getElementById("btn-hours").classList.remove("active");
      document.getElementById("btn-percentage").classList.remove("active");
      document.getElementById("tab-" + name).classList.add("active");
      document.getElementById("btn-" + name).classList.add("active");
      var figId = name + "-fig";
      var figEl = document.getElementById(figId);
      if (figEl && window.Plotly) {{
        Plotly.Plots.resize(figEl);
      }}
    }}
  </script>
</body>
</html>
"""

    with open(out_html_path, "w", encoding="utf-8") as f:
        f.write(html_content)


def write_multi_period_tabbed_html(
    period_payloads: Dict[str, Dict[str, Any]],
    out_html_path: str,
    header_context: Dict[str, Any],
    tables_html: str,
) -> None:
    plotly_cdn = _plotly_cdn_src()
    title_text = html.escape(str(header_context.get("title_text", "Project Portfolio Overview")))
    export_date = html.escape(str(header_context.get("export_date", "")))

    profile_uri = header_context.get("profile_data_uri")
    teamnl_uri = header_context.get("teamnl_data_uri")

    profile_img_html = f"<img class='profile-img' src='{profile_uri}' alt='Profile'/>" if profile_uri else ""
    teamnl_img_html = f"<img class='teamnl-img' src='{teamnl_uri}' alt='TeamNL'/>" if teamnl_uri else ""

    month_period_ids = sorted(
        [p for p in period_payloads.keys() if str(p).startswith("monthly-")],
        key=lambda p: str(p)[len("monthly-"):],
        reverse=True,
    )

    period_groups: List[str] = []
    if "weekly" in period_payloads:
        period_groups.append("weekly")
    if "biweekly" in period_payloads:
        period_groups.append("biweekly")
    if month_period_ids:
        period_groups.append("monthly")
    if "yearly" in period_payloads:
        period_groups.append("yearly")
    if not period_groups:
        raise ValueError("No period payloads provided.")

    default_group = period_groups[0]
    default_month_id = month_period_ids[0] if month_period_ids else ""
    default_period_id = default_month_id if default_group == "monthly" else default_group

    period_group_buttons_html_parts: List[str] = []
    month_buttons_html_parts: List[str] = []
    period_meta_html_parts: List[str] = []
    period_note_html_parts: List[str] = []
    period_nn_pie_block_parts: List[str] = []
    period_counts_panels_parts: List[str] = []
    period_hours_panels_parts: List[str] = []
    period_percentage_panels_parts: List[str] = []

    group_labels: Dict[str, str] = {}
    if "weekly" in period_payloads:
        group_labels["weekly"] = html.escape(str(period_payloads["weekly"].get("label", "1-week")))
    if "biweekly" in period_payloads:
        group_labels["biweekly"] = html.escape(str(period_payloads["biweekly"].get("label", "2-weeks")))
    if month_period_ids:
        group_labels["monthly"] = "Month"
    if "yearly" in period_payloads:
        group_labels["yearly"] = html.escape(str(period_payloads["yearly"].get("label", "Year")))

    for group_key in ("weekly", "biweekly", "monthly", "yearly"):
        if group_key not in period_groups:
            continue
        label = group_labels.get(group_key, html.escape(group_key))
        is_default = group_key == default_group
        period_group_buttons_html_parts.append(
            (
                f"<button class=\"tab-btn period-btn{' active' if is_default else ''}\" "
                f"id=\"btn-period-{group_key}\" onclick=\"showPeriodGroup('{group_key}')\">{label}</button>"
            )
        )

    for month_id in month_period_ids:
        payload = period_payloads[month_id]
        label = html.escape(str(payload.get("label", month_id)))
        is_default = month_id == default_month_id
        month_buttons_html_parts.append(
            (
                f"<button class=\"tab-btn month-btn{' active' if is_default else ''}\" "
                f"id=\"btn-month-{month_id}\" onclick=\"showMonth('{month_id}')\">{label}</button>"
            )
        )

    period_ids: List[str] = []
    for key in ("weekly", "biweekly"):
        if key in period_payloads:
            period_ids.append(key)
    period_ids.extend(month_period_ids)
    if "yearly" in period_payloads:
        period_ids.append("yearly")

    for period_id in period_ids:
        payload = period_payloads[period_id]
        label = html.escape(str(payload.get("label", period_id)))
        period_range = html.escape(str(payload.get("period_range", "")))
        is_default = period_id == default_period_id

        period_meta_html_parts.append(
            (
                f"<span class=\"period-meta{' active' if is_default else ''}\" "
                f"id=\"meta-{period_id}\"><b>{label}</b> — {period_range}</span>"
            )
        )

        nn_note = payload.get("nn_note") or ""
        period_note_html_parts.append(
            (
                f"<div class=\"nn-note period-note{' active' if is_default else ''}\" "
                f"id=\"nn-note-{period_id}\">{html.escape(str(nn_note))}</div>"
            )
        )

        nn_pie_html = payload.get("nn_pie_html") or ""
        if nn_pie_html:
            period_nn_pie_block_parts.append(
                (
                    f"<div class=\"nn-pie-block period-nn{' active' if is_default else ''}\" "
                    f"id=\"nn-pie-block-{period_id}\">"
                    "<div class='nn-pie-title'>Billed (to date)<br>vs remaining</div>"
                    f"{nn_pie_html}"
                    "</div>"
                )
            )

        counts_fig = payload["counts_fig"]
        hours_fig = payload["hours_fig"]
        percentage_fig = payload["percentage_fig"]
        counts_div_id = f"counts-fig-{period_id}"
        hours_div_id = f"hours-fig-{period_id}"
        percentage_div_id = f"percentage-fig-{period_id}"
        counts_html = pio.to_html(counts_fig, include_plotlyjs=False, full_html=False, div_id=counts_div_id)
        hours_html = pio.to_html(hours_fig, include_plotlyjs=False, full_html=False, div_id=hours_div_id)
        percentage_html = pio.to_html(percentage_fig, include_plotlyjs=False, full_html=False, div_id=percentage_div_id)
        hours_metrics_html = payload.get("hours_metrics_html") or ""
        percentage_metrics_html = payload.get("percentage_metrics_html") or hours_metrics_html

        period_counts_panels_parts.append(
            (
                f"<div class=\"period-panel{' active' if is_default else ''}\" "
                f"id=\"period-counts-{period_id}\">{counts_html}</div>"
            )
        )
        period_hours_panels_parts.append(
            (
                f"<div class=\"period-panel{' active' if is_default else ''}\" "
                f"id=\"period-hours-{period_id}\">"
                f"<div class=\"hours-metrics\">{hours_metrics_html}</div>"
                f"{hours_html}"
                "</div>"
            )
        )
        period_percentage_panels_parts.append(
            (
                f"<div class=\"period-panel{' active' if is_default else ''}\" "
                f"id=\"period-percentage-{period_id}\">"
                f"<div class=\"hours-metrics\">{percentage_metrics_html}</div>"
                f"{percentage_html}"
                "</div>"
            )
        )

    period_group_buttons_html = "\n".join(period_group_buttons_html_parts)
    month_buttons_html = "\n".join(month_buttons_html_parts)
    period_meta_html = "\n".join(period_meta_html_parts)
    period_note_html = "\n".join(period_note_html_parts)
    nn_pie_blocks_html = "\n".join(period_nn_pie_block_parts)
    counts_panels_html = "\n".join(period_counts_panels_parts)
    hours_panels_html = "\n".join(period_hours_panels_parts)
    percentage_panels_html = "\n".join(period_percentage_panels_parts)

    html_content = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>{title_text}</title>
  <script src="{plotly_cdn}"></script>
  <style>
    body {{
      font-family: "Segoe UI", Tahoma, sans-serif;
      margin: 0;
      background: #FAFAFA;
      color: #111;
    }}
    .page {{
      padding: 24px 28px 40px;
    }}
    .sticky-header {{
      position: sticky;
      top: 0;
      z-index: 50;
      background: #FAFAFA;
      padding-top: 8px;
      box-shadow: 0 2px 6px rgba(0,0,0,0.08);
    }}
    .report-header {{
      display: flex;
      justify-content: space-between;
      gap: 24px;
      align-items: flex-start;
      flex-wrap: wrap;
      padding: 16px 0 8px;
    }}
    .header-left h1 {{
      margin: 0 0 6px 0;
      font-size: 26px;
    }}
    .header-left .meta {{
      font-size: 14px;
      color: #444;
    }}
    .header-right {{
      display: flex;
      gap: 16px;
      align-items: center;
    }}
    .nn-pie-block {{
      display: flex;
      align-items: center;
      gap: 6px;
    }}
    .nn-pie-title {{
      writing-mode: vertical-rl;
      transform: rotate(180deg);
      font-size: 12px;
      color: #111;
      white-space: nowrap;
    }}
    .profile-img {{
      width: 120px;
      height: 120px;
      object-fit: cover;
      border-radius: 10px;
      border: 2px solid #EEE;
      background: #FFF;
    }}
    .teamnl-img {{
      height: 64px;
      object-fit: contain;
    }}
    .tabs {{
      display: flex;
      gap: 8px;
      margin: 4px 0 12px;
      padding-bottom: 12px;
      flex-wrap: wrap;
    }}
    .month-tabs {{
      display: none;
    }}
    .month-tabs.active {{
      display: flex;
    }}
    .tab-btn {{
      padding: 8px 16px;
      border: 1px solid #CCC;
      border-radius: 6px;
      background: #FFF;
      cursor: pointer;
      font-weight: 600;
    }}
    .tab-btn.month-btn {{
      padding: 6px 12px;
      font-size: 13px;
    }}
    .tab-btn.active {{
      background: #01378A;
      border-color: #01378A;
      color: #FFF;
    }}
    .tab-panel {{
      display: none;
    }}
    .tab-panel.active {{
      display: block;
    }}
    .period-panel {{
      display: none;
    }}
    .period-panel.active {{
      display: block;
    }}
    .period-meta {{
      display: none;
    }}
    .period-meta.active {{
      display: inline;
    }}
    .period-note {{
      display: none;
    }}
    .period-note.active {{
      display: block;
    }}
    .period-note.active:empty {{
      display: none;
    }}
    .period-nn {{
      display: none;
    }}
    .period-nn.active {{
      display: flex;
    }}
    .hours-metrics {{
      margin: 6px 0 16px;
    }}
    .hours-metrics:empty {{
      display: none;
      margin: 0;
    }}
    .hours-breakdown {{
      background: #FFF;
      border: 1px solid #DDD;
      border-radius: 10px;
      padding: 12px;
      box-shadow: 0 1px 2px rgba(0,0,0,0.05);
    }}
    .hours-breakdown-header {{
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      gap: 10px;
      flex-wrap: wrap;
      margin-bottom: 8px;
    }}
    .hours-breakdown h3 {{
      margin: 0;
      font-size: 16px;
    }}
    .hours-breakdown-note {{
      font-size: 12px;
      color: #444;
    }}
    .hours-breakdown-list {{
      display: flex;
      flex-direction: column;
      gap: 6px;
    }}
    .hours-project {{
      background: #FFF;
      border: 1px solid #EEE;
      border-radius: 8px;
      padding: 6px 10px;
    }}
    .hours-project[open] {{
      border-color: #CCC;
      box-shadow: 0 1px 2px rgba(0,0,0,0.05);
    }}
    .hours-project summary {{
      cursor: pointer;
      font-weight: 600;
      list-style: none;
      outline: none;
    }}
    .hours-project summary::-webkit-details-marker {{
      display: none;
    }}
    .hours-project summary::before {{
      content: "▸";
      display: inline-block;
      width: 1em;
      color: #01378A;
    }}
    .hours-project[open] summary::before {{
      content: "▾";
    }}
    .hours-project-total {{
      font-variant-numeric: tabular-nums;
    }}
    .hours-project-percent {{
      color: #444;
      font-weight: 600;
    }}
    .hours-project-entries {{
      margin-top: 8px;
      padding-left: 1.2em;
    }}
    .hours-entry-table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 12px;
    }}
    .hours-entry-table th {{
      text-align: left;
      padding: 6px 6px;
      color: #555;
      border-bottom: 1px solid #EEE;
    }}
    .hours-entry-table td {{
      padding: 6px 6px;
      border-bottom: 1px solid #F3F3F3;
      vertical-align: top;
      word-break: break-word;
    }}
    .hours-entry-duration {{
      width: 110px;
      text-align: right;
      font-variant-numeric: tabular-nums;
      white-space: nowrap;
    }}
    .hours-entry-percent {{
      width: 90px;
      text-align: right;
      font-variant-numeric: tabular-nums;
      white-space: nowrap;
    }}
    .hours-entry-empty {{
      color: #777;
      font-style: italic;
    }}
    .nn-metrics {{
      display: flex;
      flex-wrap: wrap;
      gap: 12px 18px;
      font-size: 14px;
      background: #FFF;
      border: 1px solid #DDD;
      border-radius: 8px;
      padding: 10px 12px;
      box-shadow: 0 1px 2px rgba(0,0,0,0.05);
    }}
    .nn-note {{
      margin-top: 6px;
      font-size: 13px;
      color: #8A3B3B;
    }}
    .project-info-section {{
      margin-top: 28px;
    }}
    .project-cards {{
      display: flex;
      gap: 16px;
      overflow-x: auto;
      padding-bottom: 8px;
    }}
    .project-card {{
      min-width: 280px;
      background: #FFF;
      border: 1px solid #DDD;
      border-radius: 10px;
      padding: 12px;
      box-shadow: 0 1px 2px rgba(0,0,0,0.05);
    }}
    .project-card-header {{
      font-weight: 700;
      margin-bottom: 8px;
    }}
    .project-card table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 12px;
    }}
    .project-card td {{
      padding: 3px 4px;
      border-bottom: 1px solid #EEE;
      vertical-align: top;
      word-break: break-word;
    }}
    .project-card td:first-child {{
      width: 45%;
      color: #555;
    }}
  </style>
</head>
<body>
  <div class="page">
    <div class="sticky-header">
      <div class="report-header">
        <div class="header-left">
          <h1>{title_text}</h1>
          <div class="meta">{period_meta_html}</div>
          <div class="meta">Generated: {export_date}</div>
          {period_note_html}
        </div>
        <div class="header-right">
          {nn_pie_blocks_html}
          {teamnl_img_html}
          {profile_img_html}
        </div>
      </div>

      <div class="tabs">
        <button class="tab-btn active" id="btn-counts" onclick="showTab('counts')">Counts</button>
        <button class="tab-btn" id="btn-hours" onclick="showTab('hours')">Hours</button>
        <button class="tab-btn" id="btn-percentage" onclick="showTab('percentage')">Percentage</button>
      </div>

      <div class="tabs">
        {period_group_buttons_html}
      </div>
      <div class="tabs month-tabs{' active' if default_group == 'monthly' else ''}" id="month-tabs">
        {month_buttons_html}
      </div>
    </div>

    <div class="tab-panel active" id="tab-counts">
      {counts_panels_html}
    </div>
    <div class="tab-panel" id="tab-hours">
      {hours_panels_html}
    </div>
    <div class="tab-panel" id="tab-percentage">
      {percentage_panels_html}
    </div>

    <div class="project-info-section">
      <h2>Project details</h2>
      {tables_html}
    </div>
  </div>

  <script>
    var currentTab = "counts";
    var currentPeriodId = "{default_period_id}";
    var currentMonthlyId = "{default_month_id}";

    function showTab(name) {{
      currentTab = name;
      updateView();
    }}

    function showPeriodGroup(group) {{
      if (group === "monthly") {{
        if (currentMonthlyId) {{
          currentPeriodId = currentMonthlyId;
        }}
      }} else {{
        currentPeriodId = group;
      }}
      updateView();
    }}

    function showMonth(monthId) {{
      currentMonthlyId = monthId;
      currentPeriodId = monthId;
      updateView();
    }}

    function updateView() {{
      document.getElementById("tab-counts").classList.toggle("active", currentTab === "counts");
      document.getElementById("tab-hours").classList.toggle("active", currentTab === "hours");
      document.getElementById("tab-percentage").classList.toggle("active", currentTab === "percentage");
      document.getElementById("btn-counts").classList.toggle("active", currentTab === "counts");
      document.getElementById("btn-hours").classList.toggle("active", currentTab === "hours");
      document.getElementById("btn-percentage").classList.toggle("active", currentTab === "percentage");

      var isMonthly = currentPeriodId && currentPeriodId.startsWith("monthly-");
      var activeGroup = isMonthly ? "monthly" : currentPeriodId;

      document.querySelectorAll(".period-btn").forEach(function(btn) {{
        btn.classList.remove("active");
      }});
      var activeBtn = document.getElementById("btn-period-" + activeGroup);
      if (activeBtn) {{
        activeBtn.classList.add("active");
      }}

      var monthTabs = document.getElementById("month-tabs");
      if (monthTabs) {{
        monthTabs.classList.toggle("active", isMonthly);
      }}
      document.querySelectorAll(".month-btn").forEach(function(btn) {{
        btn.classList.remove("active");
      }});
      var activeMonthBtn = document.getElementById("btn-month-" + currentMonthlyId);
      if (activeMonthBtn) {{
        activeMonthBtn.classList.add("active");
      }}

      document.querySelectorAll(".period-panel").forEach(function(panel) {{
        panel.classList.remove("active");
      }});
      var countsPanel = document.getElementById("period-counts-" + currentPeriodId);
      var hoursPanel = document.getElementById("period-hours-" + currentPeriodId);
      var percentagePanel = document.getElementById("period-percentage-" + currentPeriodId);
      if (countsPanel) {{
        countsPanel.classList.add("active");
      }}
      if (hoursPanel) {{
        hoursPanel.classList.add("active");
      }}
      if (percentagePanel) {{
        percentagePanel.classList.add("active");
      }}

      document.querySelectorAll(".period-meta").forEach(function(el) {{
        el.classList.remove("active");
      }});
      var metaEl = document.getElementById("meta-" + currentPeriodId);
      if (metaEl) {{
        metaEl.classList.add("active");
      }}

      document.querySelectorAll(".period-note").forEach(function(el) {{
        el.classList.remove("active");
      }});
      var noteEl = document.getElementById("nn-note-" + currentPeriodId);
      if (noteEl) {{
        noteEl.classList.add("active");
      }}

      document.querySelectorAll(".period-nn").forEach(function(el) {{
        el.classList.remove("active");
      }});
      var pieEl = document.getElementById("nn-pie-block-" + currentPeriodId);
      if (pieEl) {{
        pieEl.classList.add("active");
      }}

      var figId = currentTab + "-fig-" + currentPeriodId;
      var figEl = document.getElementById(figId);
      if (figEl && window.Plotly) {{
        Plotly.Plots.resize(figEl);
      }}

      var pieFigEl = document.getElementById("nn-pie-" + currentPeriodId);
      if (pieFigEl && window.Plotly) {{
        Plotly.Plots.resize(pieFigEl);
      }}
    }}

    window.addEventListener("load", function() {{
      updateView();
    }});
  </script>
</body>
</html>
"""

    with open(out_html_path, "w", encoding="utf-8") as f:
        f.write(html_content)


def export_tabbed_report(
    counts_fig: go.Figure,
    hours_fig: go.Figure,
    percentage_fig: go.Figure,
    output_dir: str,
    output_archive_dir: str,
    base_name: str,
    archive_base_name: str,
    export_date: str,
    header_context: Dict[str, Any],
    tables_html: str,
    hours_metrics_html: str,
    percentage_metrics_html: str,
    nn_pie_html: str,
    nn_note: Optional[str],
) -> Tuple[str, str]:
    html_path = os.path.join(output_dir, f"{base_name}.html")
    png_path = os.path.join(output_dir, f"{base_name}.png")

    dated_base_name = f"{archive_base_name}_generated_{export_date}"
    dated_html_path = os.path.join(output_archive_dir, f"{dated_base_name}.html")
    dated_png_path = os.path.join(output_archive_dir, f"{dated_base_name}.png")

    write_tabbed_html(
        counts_fig,
        hours_fig,
        percentage_fig,
        html_path,
        header_context,
        tables_html,
        hours_metrics_html,
        percentage_metrics_html,
        nn_pie_html,
        nn_note,
    )

    counts_fig.write_image(png_path, scale=2)  # requires kaleido

    shutil.copyfile(html_path, dated_html_path)
    shutil.copyfile(png_path, dated_png_path)

    return html_path, png_path


def export_multi_period_report(
    period_payloads: Dict[str, Dict[str, Any]],
    output_dir: str,
    output_archive_dir: str,
    base_name: str,
    archive_base_name: str,
    export_date: str,
    header_context: Dict[str, Any],
    tables_html: str,
) -> str:
    html_path = os.path.join(output_dir, f"{base_name}.html")
    dated_base_name = f"{archive_base_name}_generated_{export_date}"
    dated_html_path = os.path.join(output_archive_dir, f"{dated_base_name}.html")

    write_multi_period_tabbed_html(
        period_payloads,
        html_path,
        header_context,
        tables_html,
    )

    shutil.copyfile(html_path, dated_html_path)
    return html_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate project portfolio reports.")
    parser.add_argument(
        "--report-type",
        choices=["combined", "yearly", "monthly", "biweekly", "weekly", "all"],
        default="combined",
        help="Report type to generate.",
    )
    parser.add_argument(
        "--asof",
        default=None,
        help="As-of date in YYYY-MM-DD (defaults to today).",
    )
    return parser.parse_args()


def parse_asof_date(asof_str: Optional[str]) -> date:
    if not asof_str:
        return date.today()
    try:
        return date.fromisoformat(asof_str)
    except ValueError as exc:
        raise SystemExit(f"Invalid --asof date '{asof_str}' (expected YYYY-MM-DD).") from exc


def generate_reports(report_type: str, asof_date: date) -> None:
    export_date = date.today().isoformat()  # YYYY-MM-DD
    print(f"As-of date used: {asof_date.isoformat()}")

    projects_df, time_entries_df, project_info_map = load_and_validate_projects(PROJECTEN_DIR)
    if projects_df.empty:
        raise SystemExit(f"No project folders found under: {PROJECTEN_DIR}")

    REPORT_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), "Reports")
    REPORTS_ARCHIVE_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), "Reports", "Archive")
    os.makedirs(REPORT_DIR, exist_ok=True)
    os.makedirs(REPORTS_ARCHIVE_DIR, exist_ok=True)

    projects_df.to_csv(os.path.join(REPORT_DIR, "projects_overview.csv"), index=False)
    time_entries_df.to_csv(os.path.join(REPORT_DIR, "time_entries_df.csv"), index=False)

    periods = compute_report_periods(asof_date)
    header_assets = build_header_assets()
    tables_html = build_project_info_tables_html(projects_df, project_info_map)

    nn_df, nn_path, nn_status = load_nn_maandelijks_df()
    print(nn_status)
    _, project_color_map = build_color_maps(projects_df)

    timeline_year = asof_date.year

    if report_type in ("combined", "all"):
        period_payloads: Dict[str, Dict[str, Any]] = {}
        for rtype in ("weekly", "biweekly", "yearly"):
            period_info = periods[rtype]
            period_start = period_info["start"]
            period_end = period_info["end"]
            period_label = period_info["label"]

            time_entries_filtered = filter_time_entries_by_period(time_entries_df, period_start, period_end)

            nn_summary = None
            nn_note = None
            nn_pie_html = ""
            hours_metrics_html = ""
            percentage_metrics_html = ""
            if rtype == "yearly":
                if nn_df is None:
                    nn_note = nn_status
                else:
                    nn_summary, nn_note = compute_nn_summary(nn_df, "yearly", period_end, time_entries_filtered)
                    if nn_note:
                        nn_note = f"NN_maandelijks: {nn_note}"
                nn_pie_html = build_nn_pie_html(nn_summary, div_id=f"nn-pie-{rtype}")
                hours_metrics_html = build_nn_metrics_html(nn_summary, nn_note)
                percentage_metrics_html = hours_metrics_html
            else:
                hours_metrics_html = build_logged_hours_breakdown_html(time_entries_filtered)
                percentage_metrics_html = build_logged_hours_breakdown_html(time_entries_filtered, show_percentage=True)

            projects_for_counts = projects_df
            if rtype in ("weekly", "biweekly"):
                projects_for_counts = filter_projects_with_hours(projects_df, time_entries_filtered)

            counts_fig = build_counts_figure(
                projects_for_counts,
                export_date,
                period_start,
                period_end,
                period_label,
                project_color_map=project_color_map,
                timeline_projects_df=projects_df,
                timeline_year=timeline_year,
            )
            hours_fig = build_hours_figure(
                projects_df,
                time_entries_filtered,
                export_date,
                period_start,
                period_end,
                period_label,
                report_type=rtype,
            )
            percentage_fig = build_percentage_figure_from_hours(hours_fig)

            period_payloads[rtype] = dict(
                label=period_label,
                period_range=f"{period_start.isoformat()} to {period_end.isoformat()}",
                counts_fig=counts_fig,
                hours_fig=hours_fig,
                percentage_fig=percentage_fig,
                hours_metrics_html=hours_metrics_html,
                percentage_metrics_html=percentage_metrics_html,
                nn_pie_html=nn_pie_html,
                nn_note=nn_note,
            )

        for month_info in list_completed_month_periods(asof_date, time_entries_df):
            period_start = month_info["start"]
            period_end = month_info["end"]
            month_key = month_info["key"]
            period_id = f"monthly-{month_key}"
            period_label = month_info["label"]

            time_entries_filtered = filter_time_entries_by_period(time_entries_df, period_start, period_end)

            nn_summary = None
            nn_note = None
            nn_pie_html = ""
            hours_metrics_html = ""
            percentage_metrics_html = ""
            if nn_df is None:
                nn_note = nn_status
            else:
                nn_summary, nn_note = compute_nn_summary(nn_df, "monthly", period_end, time_entries_filtered)
                if nn_note:
                    nn_note = f"NN_maandelijks: {nn_note}"
            nn_pie_html = build_nn_pie_html(nn_summary, div_id=f"nn-pie-{period_id}")
            hours_metrics_html = build_nn_metrics_html(nn_summary, nn_note)
            percentage_metrics_html = hours_metrics_html

            projects_for_counts = filter_projects_with_hours(projects_df, time_entries_filtered)
            counts_fig = build_counts_figure(
                projects_for_counts,
                export_date,
                period_start,
                period_end,
                period_label,
                project_color_map=project_color_map,
                timeline_projects_df=projects_df,
                timeline_year=timeline_year,
            )
            hours_fig = build_hours_figure(
                projects_df,
                time_entries_filtered,
                export_date,
                period_start,
                period_end,
                period_label,
                report_type="monthly",
            )
            percentage_fig = build_percentage_figure_from_hours(hours_fig)

            period_payloads[period_id] = dict(
                label=period_label,
                period_range=f"{period_start.isoformat()} to {period_end.isoformat()}",
                counts_fig=counts_fig,
                hours_fig=hours_fig,
                percentage_fig=percentage_fig,
                hours_metrics_html=hours_metrics_html,
                percentage_metrics_html=percentage_metrics_html,
                nn_pie_html=nn_pie_html,
                nn_note=nn_note,
            )

        header_context = dict(
            title_text="Project Portfolio Overview — Rens",
            export_date=export_date,
            **header_assets,
        )

        base_name = "project_report"
        archive_base_name = f"project_report_asof_{asof_date.isoformat()}"
        html_path = export_multi_period_report(
            period_payloads,
            REPORT_DIR,
            REPORTS_ARCHIVE_DIR,
            base_name,
            archive_base_name,
            export_date,
            header_context,
            tables_html,
        )
        print(f"Generated combined report -> {html_path}")
        return

    rtype = report_type
    if rtype not in periods:
        raise SystemExit(f"Unknown report type: {rtype}")

    period_info = periods[rtype]
    period_start = period_info["start"]
    period_end = period_info["end"]
    period_label = period_info["label"]
    period_key = period_info["key"]

    time_entries_filtered = filter_time_entries_by_period(time_entries_df, period_start, period_end)

    nn_summary = None
    nn_note = None
    nn_pie_html = ""
    hours_metrics_html = ""
    percentage_metrics_html = ""
    if rtype in ("monthly", "yearly"):
        if nn_df is None:
            nn_note = nn_status
        else:
            nn_summary, nn_note = compute_nn_summary(nn_df, "monthly" if rtype == "monthly" else "yearly", period_end, time_entries_filtered)
            if nn_note:
                nn_note = f"NN_maandelijks: {nn_note}"
        nn_pie_html = build_nn_pie_html(nn_summary)
        hours_metrics_html = build_nn_metrics_html(nn_summary, nn_note)
        percentage_metrics_html = hours_metrics_html
    if rtype in ("weekly", "biweekly"):
        hours_metrics_html = build_logged_hours_breakdown_html(time_entries_filtered)
        percentage_metrics_html = build_logged_hours_breakdown_html(time_entries_filtered, show_percentage=True)

    projects_for_counts = projects_df
    if rtype in ("weekly", "biweekly", "monthly"):
        projects_for_counts = filter_projects_with_hours(projects_df, time_entries_filtered)

    counts_fig = build_counts_figure(
        projects_for_counts,
        export_date,
        period_start,
        period_end,
        period_label,
        project_color_map=project_color_map,
        timeline_projects_df=projects_df,
        timeline_year=timeline_year,
    )
    hours_fig = build_hours_figure(
        projects_df,
        time_entries_filtered,
        export_date,
        period_start,
        period_end,
        period_label,
        report_type=rtype,
    )
    percentage_fig = build_percentage_figure_from_hours(hours_fig)

    period_range = f"{period_start.isoformat()} to {period_end.isoformat()}"
    header_context = dict(
        title_text="Project Portfolio Overview — Rens",
        export_date=export_date,
        period_label=period_label,
        period_range=period_range,
        **header_assets,
    )

    if rtype == "yearly":
        base_name = "project_report_yearly"
        archive_base_name = f"project_report_yearly_{period_key}"
    elif rtype == "monthly":
        base_name = f"project_report_monthly_{period_key}"
        archive_base_name = base_name
    elif rtype == "biweekly":
        base_name = f"project_report_biweekly_{period_key}"
        archive_base_name = base_name
    else:
        base_name = f"project_report_weekly_{period_key}"
        archive_base_name = base_name

    html_path, png_path = export_tabbed_report(
        counts_fig,
        hours_fig,
        percentage_fig,
        REPORT_DIR,
        REPORTS_ARCHIVE_DIR,
        base_name,
        archive_base_name,
        export_date,
        header_context,
        tables_html,
        hours_metrics_html,
        percentage_metrics_html,
        nn_pie_html,
        nn_note,
    )

    print(f"Generated {rtype} report: {period_range} -> {html_path}")
    print(f"PNG exported: {png_path}")


def main() -> None:
    args = parse_args()
    asof_date = parse_asof_date(args.asof)
    generate_reports(args.report_type, asof_date)


if __name__ == "__main__":
    main()

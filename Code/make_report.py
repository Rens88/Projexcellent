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
   - Projects per programma (stacked: each project is one block)
   - Projects per requester (stacked: each project is one block)
   - Hours per programma (stacked: each project contributes its hours)
   - Trend: projects started per month vs closed per month
7) Exports:
   - YYYY-MM-DD_project_report.html
   - YYYY-MM-DD_project_report.png
     (PNG requires `pip install kaleido`)

Dependencies
------------
pip install pandas openpyxl plotly kaleido
"""

from __future__ import annotations

import base64
import os
import warnings
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


# ----------------------------
# Paths (script lives in Rapportage/)
# ----------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECTEN_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "Projecten"))

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
def load_and_validate_projects(projecten_dir: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns:
      projects_df: one row per project (metadata)
      time_entries_df: one row per time log entry (enriched with project fields)
    """
    project_rows: List[Dict[str, Any]] = []
    all_time_entries: List[pd.DataFrame] = []

    for folder_path in discover_project_folders(projecten_dir):
        folder_name = os.path.basename(folder_path)
        derived_project_id = derive_project_id_from_folder(folder_name)

        project_info_path = os.path.join(folder_path, "project_info.xlsx")
        time_log_path = os.path.join(folder_path, "time_log.xlsx")

        if not os.path.exists(project_info_path):
            raise FileNotFoundError(f"Missing project_info.xlsx in project folder '{folder_name}'")

        info = read_project_info_kv_from_xlsx(project_info_path)

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
                time_df["requester"] = str(project_row.get("requester", "Unknown") or "Unknown")
                time_df["project_name"] = str(project_row.get("project_name", derived_project_id) or derived_project_id)
                time_df["__project_folder"] = project_row["__project_folder"]
                time_df["duration_hours"] = pd.to_numeric(time_df["duration_minutes"], errors="coerce") / 60.0
                all_time_entries.append(time_df)

        project_rows.append(project_row)

    projects_df = pd.DataFrame(project_rows)

    for col in ["programma", "requester", "status", "project_name"]:
        if col not in projects_df.columns:
            projects_df[col] = "Unknown"
        projects_df[col] = projects_df[col].fillna("Unknown").replace("", "Unknown")

    time_entries_df = pd.concat(all_time_entries, ignore_index=True) if all_time_entries else pd.DataFrame()
    return projects_df, time_entries_df


# ----------------------------
# Hover helper
# ----------------------------
HOVER_KEYS = [
    "project_id",
    "project_name",
    "programma",
    "requester",
    "owner",
    "status",
    "priority",
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
    groups = sorted(projects_df[group_col].dropna().unique().tolist())
    fig.update_xaxes(categoryorder="array", categoryarray=groups, row=subplot_row, col=1)

    for _, project in projects_df.iterrows():
        group_val = project.get(group_col, "Unknown")
        hover = build_hover_text(project)
        project_id = project.get("project_id")

        fig.add_trace(
            go.Bar(
                x=[group_val],
                y=[1],
                name=str(project.get("project_name", project.get("project_id", "project"))),
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

    groups = sorted(merged[group_col].dropna().unique().tolist())
    fig.update_xaxes(categoryorder="array", categoryarray=groups, row=subplot_row, col=1)

    for _, project in merged.iterrows():
        group_val = project.get(group_col, "Unknown")
        hours = float(project.get("total_hours", 0.0))
        hover = build_hover_text(project, extra={"total_hours": f"{hours:.2f}"})
        project_id = project.get("project_id")

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

def add_trend_started_closed(fig: go.Figure, projects_df: pd.DataFrame, subplot_row: int, title: str) -> None:
    started = projects_df.dropna(subset=["start_date"]).copy()
    started["month"] = started["start_date"].dt.to_period("M").dt.to_timestamp()
    started_counts = started.groupby("month").size().rename("started").reset_index()

    closed = projects_df.dropna(subset=["actual_end_date"]).copy()
    closed["month"] = closed["actual_end_date"].dt.to_period("M").dt.to_timestamp()
    closed_counts = closed.groupby("month").size().rename("closed").reset_index()

    month_series: List[pd.Series] = []
    if not started_counts.empty:
        month_series.append(started_counts["month"])
    if not closed_counts.empty:
        month_series.append(closed_counts["month"])

    if month_series:
        months = pd.concat(month_series).drop_duplicates().sort_values().reset_index(drop=True)
    else:
        months = pd.Series(dtype="datetime64[ns]")

    if len(months) == 0:
        fig.add_trace(go.Scatter(x=[], y=[], mode="lines", hovertemplate="No trend data.<extra></extra>"), row=subplot_row, col=1)
        fig.add_annotation(text=f"<b>{title}</b> (no dates found)", x=0, xref="x domain", y=1.12, yref=f"y{subplot_row} domain",
                           showarrow=False, align="left", row=subplot_row, col=1)
        fig.update_yaxes(title_text="Projects", row=subplot_row, col=1)
        return

    trend = pd.DataFrame({"month": months})
    trend = trend.merge(started_counts, on="month", how="left").merge(closed_counts, on="month", how="left").fillna(0)

    fig.add_trace(
        go.Scatter(
            x=trend["month"],
            y=trend["started"],
            mode="lines+markers",
            name="Started",
            hovertemplate="Month=%{x|%Y-%m}<br>Started=%{y}<extra></extra>",
            line=dict(color=BASE_BLUE),
            marker=dict(color=BASE_BLUE),
        ),
        row=subplot_row, col=1
    )
    fig.add_trace(
        go.Scatter(
            x=trend["month"],
            y=trend["closed"],
            mode="lines+markers",
            name="Closed",
            hovertemplate="Month=%{x|%Y-%m}<br>Closed=%{y}<extra></extra>",
            line=dict(color=BASE_ORANGE),
            marker=dict(color=BASE_ORANGE),
        ),
        row=subplot_row, col=1
    )

    fig.update_yaxes(title_text="Projects", row=subplot_row, col=1)
    fig.update_xaxes(title_text="Month", row=subplot_row, col=1)
    fig.add_annotation(text=f"<b>{title}</b>", x=0, xref="x domain", y=1.12, yref=f"y{subplot_row} domain",
                       showarrow=False, align="left", row=subplot_row, col=1)


def build_report_figure(projects_df: pd.DataFrame, time_entries_df: pd.DataFrame, export_date: str) -> go.Figure:
    total_rows = 4
    _, project_color_map = build_color_maps(projects_df)
    fig = make_subplots(rows=total_rows, cols=1, shared_xaxes=False, vertical_spacing=0.10)

    add_stacked_project_count_bars(fig, projects_df, "programma", 1,
                                   "Projects per programma (stacked: each project = 1 block)",
                                   project_color_map)
    add_stacked_project_count_bars(fig, projects_df, "requester", 2,
                                   "Projects per requester (stacked: each project = 1 block)",
                                   project_color_map)
    add_stacked_hours_bars(fig, projects_df, time_entries_df, "programma", 3,
                           "Hours per programma (stacked: each project contributes its hours)",
                           project_color_map)
    add_trend_started_closed(fig, projects_df, 4,
                             "Trend: projects started vs closed per month")

    apply_axis_style(fig, total_rows)
    fig.update_layout(
        barmode="stack",
        height=1400,
        margin=dict(l=60, r=60, t=200, b=60),
        title_text=f"Project Portfolio Overview — Rens<br><sup>Report generation date:{export_date}</sup>",
        title_x=0.0,
        plot_bgcolor="rgba(255,255,255,1)",
        paper_bgcolor="rgba(250,250,250,1)",
        hoverlabel=dict(namelength=-1),
        showlegend=False,
        # legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    add_profile_picture(fig)
    add_teamnl_logo(fig)
    return fig


# ----------------------------
# Export
# ----------------------------
def export_report(fig: go.Figure, output_dir: str, export_date: str) -> Tuple[str, str]:
    base_name = f"{export_date}_project_report"
    html_path = os.path.join(output_dir, f"{base_name}.html")
    png_path = os.path.join(output_dir, f"{base_name}.png")

    fig.write_html(html_path, include_plotlyjs="cdn", full_html=True)
    fig.write_image(png_path, scale=2)  # requires kaleido

    return html_path, png_path


def main() -> None:
    export_date = date.today().isoformat()  # YYYY-MM-DD
    projects_df, time_entries_df = load_and_validate_projects(PROJECTEN_DIR)
    if projects_df.empty:
        raise SystemExit(f"No project folders found under: {PROJECTEN_DIR}")

    REPORT_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), "Reports")
    os.makedirs(REPORT_DIR, exist_ok=True)
    fig = build_report_figure(projects_df, time_entries_df, export_date)
    html_path, png_path = export_report(fig, REPORT_DIR, export_date)

    print(f"Exported HTML: {html_path}")
    print(f"Exported PNG : {png_path}")


if __name__ == "__main__":
    main()

# Transfer Guide

## Goal

Make the workflow reproducible for colleagues with minimal setup.

The expected touchpoints are:
1. `projexcellent_config.json` (all configuration)
2. `Projecten/` (project data)
3. `make_report.bat` or `make_report.command` (launcher)

## Current Source Of Truth

- Configuration: `projexcellent_config.json`
- Config reference: `projexcellent_config_explanation.txt`
- Report engine: `Code/make_report.py`
- Launcher: `Code/run_report.py` and root launcher scripts
- Project bootstrap: `Code/new_project.py` via `new_project.bat` / `new_project.command`
- Templates: `Templates/project_info_template.xlsx`, `Templates/time_log_template.xlsx`, `Templates/hours_remaining_template.csv`

## Mandatory vs Optional Inputs

Mandatory per project:
- folder name format: `YYYY_NNNN_description`
- `project_info.xlsx` with matching `project_id`
- unique `project_name` across projects (case-insensitive)
- `Deliverables/` folder

Conditionally mandatory:
- if `status` is `Closed`, `actual_end_date` must be filled

Optional:
- `time_log.xlsx` (recommended)
- hours-remaining workbook configured via `paths.hours_remaining` (optional)
- or yearly capacity directly via `hours.workable_hours_per_year` (optional)

Notes:
- `paths.hours_remaining.excel_paths` can contain multiple candidate files; first existing path is used.
- `paths.hours_remaining.sheet_name` controls which worksheet is read.

## Colleague Onboarding Checklist

1. Edit `projexcellent_config.json`.
2. Run launcher:
- Windows: `make_report.bat`
- macOS/Linux: `./make_report.command`
3. Put branding images in `assets/` (recommended): `profile_photo.jpg` and `logo.png`.
4. Add or create a project folder under configured projects directory.
5. Re-run launcher and verify outputs in configured reports directory.

If there are no real projects yet, the launcher will use `DummyProjecten/` as demo input
when `runtime.use_dummy_projects_when_projects_empty=true`.

## Why One Config File

The JSON config now contains:
- report title/name
- person name subtitle
- company name + abbreviation
- default report type
- dummy fallback toggle and path
- profile photo path
- logo path
- projects folder path
- templates folder path
- reports folder path
- optional hours-remaining workbook settings
- optional yearly/weekly capacity settings
- color scheme

No environment variable setup is required.

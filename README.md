# Projexcellent

Generate portfolio reports from project folders.

## What To Edit (Only 3 Things)

1. `projexcellent_config.json`
2. Your project folders in `Projecten/` (or the folder configured in `paths.projects_dir`)
3. Launcher script:
- Windows: `make_report.bat`
- macOS/Linux: `make_report.command`

## Quick Start

1. Edit `projexcellent_config.json` in the repo root.
2. Add your project folders under `Projecten/` (or configured path).
3. Run:

Windows:
```bat
make_report.bat
```

macOS/Linux:
```bash
./make_report.command
```

Outputs are written to `Reports/` (or configured `paths.reports_dir`).
Default report type comes from `runtime.default_report_type` in config.

## Assets Folder

Use `assets/` in the repo root for branding images:
- `assets/profile_photo.jpg`
- `assets/logo.png`

These are the default values in `projexcellent_config.json` and are a good place to store your own replacements.

## Dummy Demo Mode

`DummyProjecten/` is kept on purpose so users can preview report output before entering real data.

When `Projecten/` (or `paths.projects_dir`) has no project subfolders and
`runtime.use_dummy_projects_when_projects_empty=true`, the report automatically loads
`paths.dummy_projects_dir` (default: `DummyProjecten`).

## Single Source Of Configuration

All runtime settings are in `projexcellent_config.json`, including:
- report name/title
- person name (subtitle under title)
- company name/abbreviation used in report text
- projects folder location
- templates folder location
- reports output location
- profile photo path
- logo path
- optional hours-remaining workbook source (`paths.hours_remaining`)
- optional yearly capacity override (`hours.workable_hours_per_year`)
- optional weekly percent denominator (`hours.workable_hours_per_week_reference_value`)
- customizable color scheme (`color_scheme`)

Detailed field-by-field explanation is in `projexcellent_config_explanation.txt`.

No environment variables are required.

## Project Structure

Each project folder must follow:

```text
YYYY_NNNN_description/
  project_info.xlsx
  Deliverables/
  time_log.xlsx   (optional but recommended)
```

Validation includes:
- folder naming format (`YYYY_NNNN_description`)
- `project_id` in folder and `project_info.xlsx` must match
- `Deliverables/` folder must exist
- `status=Closed` requires `actual_end_date`
- `project_name` must be unique across projects (case-insensitive)

## Creating A New Project (Optional Helper)

Windows:
```bat
new_project.bat --counter 12 --slug sleep_study --project-name "Sleep Study"
```

macOS/Linux:
```bash
./new_project.command --counter 12 --slug sleep_study --project-name "Sleep Study"
```

The launcher calls `Code/new_project.py` and uses `projexcellent_config.json`.
If you run `new_project.bat` or `./new_project.command` without arguments, it now starts an interactive prompt sequence.

## Notes

- You can provide yearly-capacity data in two ways:
  - set `hours.workable_hours_per_year` in config (no workbook needed), or
  - provide a workbook via `paths.hours_remaining`.
- `paths.hours_remaining.excel_paths` accepts multiple paths so teammates can keep different local file names/locations; the first existing file is used.
- `paths.hours_remaining.sheet_name` lets you set the workbook sheet explicitly.
- A starter template is available at `Templates/hours_remaining_template.csv`.

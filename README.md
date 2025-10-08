# LeadSwift Google Uploader

This repository contains a refactored LeadSwift export pipeline and a Google Drive/Sheets uploader.

- `src/LeadSwift/Pipeline.php` — orchestration of the daily export, file preparation, weekly aggregation, and campaign discovery.
- `src/LeadSwift/Utils.php` — helpers (HTTP, progress detection/printing, atomic write).
- `src/LeadSwift/Logger.php` — simple logger that writes to stdout and a log file.
- `src/Google/DriveUploader.php` — Google Drive/Sheets uploader wrapper.
- `bin/run.php` — CLI entrypoint (supports `--daily-start`, `--weekly-start`, `--upload-to-drive`).

## How the pipeline works

Daily and weekly runs share the same configuration file and operate inside `base_dir`. The pipeline also supports optional Google Drive uploads.

### Daily run highlights

1. **Campaign discovery** — if `campaign_keyword` is set, a call to `https://leadswift.com/api/campaigns` finds new campaign IDs that match the keyword. They are added to `campaigns_unexported` while respecting the `campaigns_exported_*` queues to avoid duplicates.
2. **Locking** — `export.lock` inside `base_dir` prevents concurrent runs. Weekly jobs poll until the lock is released.
3. **Exports** — for each queued campaign:
   - Fetch all searches, start exports via `export_leads_begin`, and poll `export_leads_status` until a downloadable CSV is returned.
   - Downloaded CSVs for a campaign are stored in `LeadSwift_RAW/campaign_<id>/search_XX.csv`.
4. **Merge** — all downloaded CSVs are merged into `LeadSwift_MERGED/campaign_<id>/merge_N.csv` while keeping a single header row.
5. **Prepare** — contacts are normalised by company: the pipeline produces `LeadSwift_PREPARED/campaign_<id>/prepared_N.csv` with columns `Company`, `Contact Label`, `Email`, `Phone`.
6. **Queues update** — processed campaign IDs move from `campaigns_unexported` to `campaigns_exported_week`; `config.json` is saved back to disk.

### Weekly aggregation

When `--weekly-start` is used, the pipeline waits for any daily lock to clear, then:

1. Reads `campaigns_exported_week` to find campaigns that completed during the week.
2. Concatenates their prepared CSVs (skipping duplicate headers) into `LeadSwift_PREPARED_WEEK/YYYY-MM-DD.csv`.
3. Moves IDs from `campaigns_exported_week` to `campaigns_exported_all` and clears the weekly queue.

### Google Drive / Sheets uploads

Passing `--upload-to-drive` triggers uploads through `DriveUploader`:

- During a daily run, all RAW/MERGED/PREPARED directories for exported campaigns, plus `LeadSwift_PREPARED_WEEK`, can be uploaded. The remote structure mirrors local directories.
- During standalone uploads, any weekly CSVs found in `LeadSwift_PREPARED_WEEK` are pushed individually.
- Use `--only-upload` to push all existing exports (RAW/MERGED/PREPARED directories and weekly aggregates) without rerunning the pipeline.
- Credentials default to `client_secret.json` and `token.json` in the repo root but can be overridden via `--drive-creds` and `--drive-token`.

## Installation

1. Install dependencies:

```bash
composer install
composer dump-autoload
```

2. Prepare `config.json` (example below).

## config.json example

```json
{
  "api_key": "YOUR_KEY",
  "export_csv_params": "export_csv_params=%7B...%7D&one_contact_per_row=1",
  "base_dir": "/path/to/base_dir",
  "log_level": "INFO",
  "campaigns_unexported": [1412, 12341234, 1213],
  "campaigns_exported_week": [],
  "campaigns_exported_all": []
}
```

Place `client_secret.json` (Google credentials) in the repository root or specify via `--drive-creds`.

## Usage

Daily run (cron every day):

```bash
php bin/run.php --config=/path/config.json --daily-start
```

Weekly run (cron weekly):

```bash
php bin/run.php --config=/path/config.json --weekly-start
```

Upload existing artifacts without a new export:

```bash
php bin/run.php --config=/path/config.json --only-upload --drive-creds=/path/client_secret.json --drive-token=/path/token.json
```

Upload prepared weekly files to Drive:

```bash
php bin/run.php --config=/path/config.json --upload-to-drive --drive-creds=/path/client_secret.json --drive-token=/path/token.json
```

## Locking

The pipeline uses `export.lock` in `base_dir` to prevent concurrent daily/weekly runs. Weekly run will wait until lock is gone (with a timeout).

## Logging

Logs are written into `base_dir/logs/lead_swift_YYYY-MM-DD.log` and also echoed to stdout. You can control `log_level` via `config.json` (DEBUG, INFO, WARN, ERROR).

## Notes

- The weekly aggregator writes an aggregated CSV into `LeadSwift_PREPARED_WEEK/YYYY-MM-DD.csv`.
- `leadswift_pipeline.php` and `uploader.php` were kept as thin wrappers for backward compatibility and now advise using `bin/run.php`.

## Next steps (optional)

- Add more robust error handling and retries for downloads.
- Add retention/rotation for logs.
- Add unit tests for parsing and merging logic.

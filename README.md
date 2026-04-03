# LeadSwift Google Uploader

This repository contains a refactored LeadSwift export pipeline and a Google Drive/Sheets uploader.

## Status

This is legacy code from the `openai-worker` project and is provided as-is.
Active maintenance is not guaranteed.

Do not commit credentials or runtime artifacts to git. In particular, keep files such as `config.json`, `.env`, `client_secret.json`, `token.json`, logs, lock files, and generated CSV exports out of the repository.

- `src/LeadSwift/Pipeline.php` — orchestration of the daily export, campaign discovery/queueing, and CSV preparation.
- `src/LeadSwift/Utils.php` — helpers (HTTP, progress detection/printing, atomic write).
- `src/LeadSwift/Logger.php` — simple logger that writes to stdout and a log file.
- `src/Google/DriveUploader.php` — Google Drive/Sheets uploader wrapper.
- `bin/run.php` — CLI entrypoint (supports `--daily-start`, `--upload-to-drive`, `--only-upload`, `--repair-csv`).

## How the pipeline works

Daily runs operate inside `base_dir` and can optionally mirror results to Google Drive.

### Daily run highlights

1. **City schedule upkeep** — when `city_data_sheet` is configured, the worker fills in missing dates (local CSV or Google Sheet) so that each batch of `search_quota` rows shares the same date in `DD/MM/YYYY` format, advancing one day per batch while leaving existing dates untouched.
1. **Drive synchronisation** — before and after the daily run the worker compares `LeadSwift_*` directories with Google Drive, downloading missing remote files and uploading local ones so both sides stay aligned.
1. **Campaign discovery & queue sync** — `campaign_keyword` identifies remote campaigns; IDs already exported are skipped, new IDs are pushed into `campaigns_queued`. Each queued campaign is probed via `GET /api/searches/{id}` and promoted to `campaigns_unexported` once it has at least `search_quota` (default 20) completed searches.
1. **Locking** — `export.lock` inside `base_dir` prevents overlapping daily runs.
1. **Exports** — for each campaign in `campaigns_unexported`:
   - Fetch all searches, start exports via `export_leads_begin`, and poll `export_leads_status` until a downloadable CSV is returned.
   - Downloaded CSVs for a campaign are stored in `LeadSwift_RAW/campaign_<id>/search_XX.csv`.
1. **Merge** — all downloaded CSVs are merged into `LeadSwift_MERGED/campaign_<id>/merge_N.csv` while keeping a single header row.
1. **Prepare** — contacts are normalised by company: the pipeline produces `LeadSwift_PREPARED/campaign_<id>/prepared_N.csv` with columns `Company`, `Contact Label`, `Email`, `Phone`.
1. **Queue cleanup** — exported IDs are removed from `campaigns_unexported`/`campaigns_queued` and appended to `campaigns_exported_all`; `config.json` is saved back to disk.

### Google Drive / Sheets uploads

Passing `--upload-to-drive` triggers uploads through `DriveUploader`:

- During a daily run, all RAW/MERGED/PREPARED directories for exported campaigns are uploaded; the remote structure mirrors the local one.
- Use `--only-upload` to push all existing exports (RAW/MERGED/PREPARED) without rerunning the pipeline.
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
  "log_retention_days": 3,
  "lock_timeout_seconds": 21600,
  "campaign_keyword": "Restaurants Batch",
  "search_keyword": "restaurants",
  "search_quota": 20,
  "city_data_sheet": "local",
  "city_data_range": "E2:F",
  "campaigns_queued": [],
  "campaigns_unexported": [],
  "campaigns_exported_all": []
}
```

Place `client_secret.json` (Google credentials) in the repository root or specify via `--drive-creds`.

## Usage

Daily run (cron every day):

```bash
php bin/run.php --config=/path/config.json --daily-start
```

When `city_data_sheet` is set to `local`, also pass the CSV path (relative paths are resolved from the project root):

```bash
php bin/run.php --config=/path/config.json --daily-start --city-data-file="Top 700 USA cities from D7 - Sheet1.csv"
```

Upload existing artifacts without a new export:

```bash
php bin/run.php --config=/path/config.json --only-upload --drive-creds=/path/client_secret.json --drive-token=/path/token.json
```

This command performs the same two-way sync step as the daily worker (downloads missing remote files and uploads locally missing ones). When `city_data_sheet` holds a Google Sheet ID, the daily worker refreshes the sheet via the Google Sheets API using the same credential flags (`--drive-creds`, `--drive-token`).

## Locking

The pipeline uses `export.lock` in `base_dir` to prevent concurrent runs. The lock self-heals if it becomes stale (configurable timeout via `lock_timeout_seconds`, default 6 hours).

## Logging

Logs are written into `base_dir/logs/lead_swift_YYYY-MM-DD_HH-MM-SS.log` and also echoed to stdout. You can control `log_level` via `config.json` (DEBUG, INFO, WARN, ERROR). Older log files are pruned automatically (default retention 3 days, configurable via `log_retention_days`).

## Notes

- City scheduling expects column E to contain the LeadSwift-formatted location and column F to contain the planned bulk search date.
- `leadswift_pipeline.php` and `uploader.php` were kept as thin wrappers for backward compatibility and now advise using `bin/run.php`.

## Next steps (optional)

- Add more robust error handling and retries for downloads.
- Add retention/rotation for logs.
- Add unit tests for parsing and merging logic.

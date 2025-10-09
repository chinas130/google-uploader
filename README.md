# LeadSwift Google Uploader

This repository contains a refactored LeadSwift export pipeline and a Google Drive/Sheets uploader.

- `src/LeadSwift/Pipeline.php` — orchestration of the daily export, campaign discovery/queueing, and CSV preparation.
- `src/LeadSwift/Utils.php` — helpers (HTTP, progress detection/printing, atomic write).
- `src/LeadSwift/Logger.php` — simple logger that writes to stdout and a log file.
- `src/Google/DriveUploader.php` — Google Drive/Sheets uploader wrapper.
- `bin/run.php` — CLI entrypoint (supports `--daily-start`, `--upload-to-drive`, `--only-upload`, `--repair-csv`).

## How the pipeline works

Daily runs operate inside `base_dir` and can optionally mirror results to Google Drive.

### Daily run highlights

1. **Campaign discovery & queue sync** — `campaign_keyword` identifies remote campaigns; IDs already exported are skipped, new IDs are pushed into `campaigns_queued`. Each queued campaign is probed via `GET /api/searches/{id}` and promoted to `campaigns_unexported` once it has at least `search_quota` (default 20) completed searches.
2. **Locking** — `export.lock` inside `base_dir` prevents overlapping daily runs.
3. **Exports** — for each campaign in `campaigns_unexported`:
   - Fetch all searches, start exports via `export_leads_begin`, and poll `export_leads_status` until a downloadable CSV is returned.
   - Downloaded CSVs for a campaign are stored in `LeadSwift_RAW/campaign_<id>/search_XX.csv`.
4. **Merge** — all downloaded CSVs are merged into `LeadSwift_MERGED/campaign_<id>/merge_N.csv` while keeping a single header row.
5. **Prepare** — contacts are normalised by company: the pipeline produces `LeadSwift_PREPARED/campaign_<id>/prepared_N.csv` with columns `Company`, `Contact Label`, `Email`, `Phone`.
6. **Queue cleanup** — exported IDs are removed from `campaigns_unexported`/`campaigns_queued` and appended to `campaigns_exported_all`; `config.json` is saved back to disk.

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
  "campaign_keyword": "Restaurants Batch",
  "search_keyword": "restaurants",
  "search_quota": 20,
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

Upload existing artifacts without a new export:

```bash
php bin/run.php --config=/path/config.json --only-upload --drive-creds=/path/client_secret.json --drive-token=/path/token.json
```

## Locking

The pipeline uses `export.lock` in `base_dir` to prevent concurrent runs.

## Logging

Logs are written into `base_dir/logs/lead_swift_YYYY-MM-DD_HH-MM-SS.log` and also echoed to stdout. You can control `log_level` via `config.json` (DEBUG, INFO, WARN, ERROR).

## Notes

- `leadswift_pipeline.php` and `uploader.php` were kept as thin wrappers for backward compatibility and now advise using `bin/run.php`.

## Next steps (optional)

- Add more robust error handling and retries for downloads.
- Add retention/rotation for logs.
- Add unit tests for parsing and merging logic.

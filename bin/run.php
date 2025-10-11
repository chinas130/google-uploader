#!/usr/bin/env php
<?php
require __DIR__ . '/../vendor/autoload.php';

use App\LeadSwift\Pipeline;
use App\LeadSwift\Utils;
use App\Google\DriveUploader;
use App\Google\CitySheetManager;
use App\LeadSwift\CitySchedule;

function buildDriveUploader(array $opts): DriveUploader
{
    $creds = isset($opts['drive-creds']) && $opts['drive-creds'] !== false
        ? (string)$opts['drive-creds']
        : __DIR__ . '/../client_secret.json';
    $token = isset($opts['drive-token']) && $opts['drive-token'] !== false
        ? (string)$opts['drive-token']
        : __DIR__ . '/../token.json';

    return new DriveUploader($creds, $token);
}

function uploadCampaignArtifacts(DriveUploader $uploader, string $baseDir, array $campaignIds): void
{
    foreach ($campaignIds as $campId) {
        if ($campId === null || $campId === '') continue;
        $campId = (string)$campId;
        $campId = trim($campId);
        if ($campId === '') continue;

        $dirs = [
            $baseDir . "/LeadSwift_RAW/campaign_{$campId}",
            $baseDir . "/LeadSwift_MERGED/campaign_{$campId}",
            $baseDir . "/LeadSwift_PREPARED/campaign_{$campId}",
        ];

        foreach ($dirs as $dir) {
            if (!is_dir($dir)) continue;
            $parentFolder = basename(dirname($dir));
            $campaignFolder = basename($dir);
            $remotePrefix = $parentFolder . '/' . $campaignFolder;
            $results = $uploader->uploadDirectoryPreserve($dir, $remotePrefix);
            foreach ($results as $local => $id) {
                if ($id) {
                    echo "Uploaded {$local} -> {$id}\n";
                } else {
                    fwrite(STDERR, "Upload failed for {$local}\n");
                }
            }
        }
    }
}

function uploadPreparedArtifacts(DriveUploader $uploader, string $baseDir, array $campaignIds): void
{
    foreach ($campaignIds as $campId) {
        if ($campId === null || $campId === '') continue;
        $campId = trim((string)$campId);
        if ($campId === '') continue;
        $dir = $baseDir . "/LeadSwift_PREPARED/campaign_{$campId}";
        if (!is_dir($dir)) continue;
        $results = $uploader->uploadDirectoryPreserve($dir, "LeadSwift_PREPARED/campaign_{$campId}");
        foreach ($results as $local => $id) {
            if ($id) {
                echo "Uploaded {$local} -> {$id}\n";
            } else {
                fwrite(STDERR, "Upload failed for {$local}\n");
            }
        }
    }
}

function isAbsolutePath(string $path): bool
{
    if ($path === '') return false;
    return preg_match('/^([A-Za-z]:[\\\\\\/]|\\\\\\\\|\\/)/', $path) === 1;
}

function resolveCityCsvPath(?string $cliPath, string $projectRoot): string
{
    if ($cliPath === null || $cliPath === '') {
        throw new \RuntimeException('--city-data-file is required when city_data_sheet=local');
    }

    $candidate = $cliPath;
    if (!isAbsolutePath($candidate)) {
        $candidate = rtrim($projectRoot, DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR . ltrim($candidate, DIRECTORY_SEPARATOR);
    }

    if (!is_file($candidate)) {
        throw new \RuntimeException("City data CSV not found: {$candidate}");
    }
    if (!is_readable($candidate) || !is_writable($candidate)) {
        throw new \RuntimeException("City data CSV must be readable and writable: {$candidate}");
    }

    $real = realpath($candidate);
    return $real !== false ? $real : $candidate;
}

$opts = getopt('', ['config::','env::','daily-start','no_progress','upload-to-drive::','drive-token::','drive-creds::','city-data-file::','only-upload','repair-csv','repair-csv-no-upload']);

function discoverCampaignIds(string $baseDir, array $config): array
{
    $candidates = [];
    foreach (['campaigns_unexported', 'campaigns_queued', 'campaigns_exported_all'] as $key) {
        if (!empty($config[$key]) && is_array($config[$key])) {
            foreach ($config[$key] as $val) {
                if ($val === null || $val === '') continue;
                $candidates[] = (string)$val;
            }
        }
    }

    $roots = [
        $baseDir . '/LeadSwift_RAW',
        $baseDir . '/LeadSwift_MERGED',
        $baseDir . '/LeadSwift_PREPARED',
    ];

    foreach ($roots as $root) {
        if (!is_dir($root)) continue;
        $dirs = glob($root . '/campaign_*', GLOB_ONLYDIR);
        if (!$dirs) continue;
        foreach ($dirs as $dir) {
            $name = basename($dir);
            if (preg_match('/^campaign_(\d+)$/', $name, $m)) {
                $candidates[] = $m[1];
            }
        }
    }

    $candidates = array_map('trim', array_map('strval', $candidates));
    $candidates = array_filter($candidates, fn($id) => $id !== '');
    $candidates = array_values(array_unique($candidates));
    sort($candidates, SORT_STRING);
    return $candidates;
}

$repairMode = isset($opts['repair-csv']);
$repairNoUpload = isset($opts['repair-csv-no-upload']);
if ($repairNoUpload && !$repairMode) {
    fwrite(STDERR, "--repair-csv-no-upload can only be used together with --repair-csv\n");
    exit(1);
}

$configPath = $opts['config'] ?? __DIR__ . '/../config.json';
$envPath = $opts['env'] ?? null;
$env = Utils::loadEnvFile($envPath);

$config = [];
if (is_readable($configPath)) {
    $decoded = json_decode(file_get_contents($configPath), true);
    if ($decoded === null && json_last_error() !== JSON_ERROR_NONE) {
        fwrite(STDERR, "Invalid config.json\n");
        exit(1);
    }
    if (is_array($decoded)) {
        $config = $decoded;
    }
} elseif (!$repairMode) {
    fwrite(STDERR, "Config not found: {$configPath}\n");
    exit(1);
}

$projectRoot = realpath(__DIR__ . '/..') ?: getcwd();
$defaultBaseDir = is_dir(__DIR__ . '/../basedir') ? realpath(__DIR__ . '/../basedir') : $projectRoot;
$baseDir = $config['base_dir'] ?? $defaultBaseDir;
$onlyUpload = isset($opts['only-upload']);
$uploadRequested = isset($opts['upload-to-drive']) || $onlyUpload;

$cityDataMode = $config['city_data_sheet'] ?? null;
$cityDataRange = $config['city_data_range'] ?? 'E2:F';
$cityDataFileCli = $opts['city-data-file'] ?? null;
$searchQuota = (int)($config['search_quota'] ?? 20);
$cityBatchSize = max(1, $searchQuota > 0 ? $searchQuota : 20);


if ($repairMode && ($onlyUpload || isset($opts['daily-start']))) {
    fwrite(STDERR, "--repair-csv cannot be combined with daily pipeline flag or --only-upload\n");
    exit(1);
}

if ($onlyUpload && isset($opts['daily-start'])) {
    fwrite(STDERR, "--only-upload cannot be combined with daily pipeline flag\n");
    exit(1);
}

$pipeline = new Pipeline($config, $baseDir);

try {
    if ($repairMode) {
        $campaignsForRepair = discoverCampaignIds($baseDir, $config);
        if (!count($campaignsForRepair)) {
            fwrite(STDERR, "No campaigns discovered for repair\n");
        } else {
            $rebuilt = $pipeline->repairCampaignsFromRaw($campaignsForRepair);
            if (!count($rebuilt)) {
                fwrite(STDERR, "Repair command completed but no CSV files were rebuilt\n");
            }
        }

        if (!$repairNoUpload && !empty($rebuilt ?? [])) {
            $uploader = buildDriveUploader($opts);
            foreach (array_keys($rebuilt) as $campId) {
                $uploader->purgeRemotePrefix("LeadSwift_PREPARED/campaign_{$campId}");
            }
            uploadPreparedArtifacts($uploader, $baseDir, array_keys($rebuilt));
        }
        exit(0);
    }

    if ($onlyUpload) {
        $uploader = buildDriveUploader($opts);
        $campaignsForUpload = discoverCampaignIds($baseDir, $config);
        uploadCampaignArtifacts($uploader, $baseDir, $campaignsForUpload);
        exit(0);
    }

    if (isset($opts['daily-start'])) {
        if (is_string($cityDataMode) && $cityDataMode !== '') {
            if (strtolower($cityDataMode) === 'local') {
                $cityCsvPath = resolveCityCsvPath($cityDataFileCli, $projectRoot);
                $updatedRows = CitySchedule::rescheduleCsv($cityCsvPath, $cityBatchSize);
                echo "City schedule refreshed (local CSV): {$updatedRows} rows\n";
            } else {
                $credsPath = $opts['drive-creds'] ?? __DIR__ . '/../client_secret.json';
                $tokenPath = $opts['drive-token'] ?? __DIR__ . '/../token.json';
                $citySheetManager = new CitySheetManager($credsPath, $tokenPath);
                $updatedRows = $citySheetManager->rescheduleSheet($cityDataMode, $cityDataRange, $cityBatchSize);
                echo "City schedule refreshed (Google Sheet): {$updatedRows} rows\n";
            }
        }
        $res = $pipeline->runDaily(isset($opts['no_progress']));
        $pipeline->saveConfig($configPath);
        echo "Daily run finished\n";

        // If upload-to-drive flag provided, upload RAW, MERGED and PREPARED for exported campaigns
        if ($uploadRequested) {
            $uploader = buildDriveUploader($opts);
            $exported = $res['exported_ids'] ?? [];
            uploadCampaignArtifacts($uploader, $baseDir, $exported);
        }
    }
} catch (Exception $e) {
    fwrite(STDERR, "Error: " . $e->getMessage() . "\n");
    exit(2);
}

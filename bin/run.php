#!/usr/bin/env php
<?php
require __DIR__ . '/../vendor/autoload.php';

use App\LeadSwift\Pipeline;
use App\LeadSwift\Utils;
use App\Google\DriveUploader;

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

function uploadPreparedWeekDirectory(DriveUploader $uploader, string $baseDir): void
{
    $pw = $baseDir . '/LeadSwift_PREPARED_WEEK';
    if (!is_dir($pw)) return;
    $results = $uploader->uploadDirectoryPreserve($pw, 'LeadSwift_PREPARED_WEEK');
    foreach ($results as $local => $id) {
        if ($id) {
            echo "Uploaded {$local} -> {$id}\n";
        } else {
            fwrite(STDERR, "Upload failed for {$local}\n");
        }
    }
}

function uploadPreparedWeekFiles(DriveUploader $uploader, string $baseDir): void
{
    $preparedWeekDir = $baseDir . '/LeadSwift_PREPARED_WEEK';
    if (!is_dir($preparedWeekDir)) return;
    foreach (glob($preparedWeekDir . '/*.csv') as $file) {
        if (!is_readable($file)) {
            fwrite(STDERR, "Cannot read {$file}\n");
            continue;
        }
        $remotePath = 'LeadSwift_PREPARED_WEEK/' . basename($file);
        $id = $uploader->uploadFileFromPath($file, $remotePath);
        echo "Uploaded {$file} -> {$id}\n";
    }
}

$opts = getopt('', ['config::','env::','daily-start','weekly-start','no_progress','upload-to-drive::','drive-token::','drive-creds::','only-upload']);

function discoverCampaignIds(string $baseDir, array $config): array
{
    $candidates = [];
    foreach (['campaigns_unexported', 'campaigns_exported_week', 'campaigns_exported_all'] as $key) {
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

$configPath = $opts['config'] ?? __DIR__ . '/../config.json';
$envPath = $opts['env'] ?? null;
$env = Utils::loadEnvFile($envPath);

if (!is_readable($configPath)) {
    fwrite(STDERR, "Config not found: {$configPath}\n");
    exit(1);
}
$config = json_decode(file_get_contents($configPath), true);
if ($config === null) { fwrite(STDERR, "Invalid config.json\n"); exit(1); }

$baseDir = $config['base_dir'] ?? getcwd();
$onlyUpload = isset($opts['only-upload']);
$uploadRequested = isset($opts['upload-to-drive']) || $onlyUpload;

if ($onlyUpload && (isset($opts['daily-start']) || isset($opts['weekly-start']))) {
    fwrite(STDERR, "--only-upload cannot be combined with daily or weekly pipeline flags\n");
    exit(1);
}

$pipeline = new Pipeline($config, $baseDir);

try {
    if ($onlyUpload) {
        $uploader = buildDriveUploader($opts);
        $campaignsForUpload = discoverCampaignIds($baseDir, $config);
        uploadCampaignArtifacts($uploader, $baseDir, $campaignsForUpload);
        uploadPreparedWeekDirectory($uploader, $baseDir);
        uploadPreparedWeekFiles($uploader, $baseDir);
        exit(0);
    }

    if (isset($opts['daily-start'])) {
        $res = $pipeline->runDaily(isset($opts['no_progress']));
        $pipeline->saveConfig($configPath);
        echo "Daily run finished\n";

        // If upload-to-drive flag provided, upload RAW, MERGED and PREPARED for exported campaigns
        if ($uploadRequested) {
            $uploader = buildDriveUploader($opts);
            $exported = $res['exported_ids'] ?? [];
            uploadCampaignArtifacts($uploader, $baseDir, $exported);
            uploadPreparedWeekDirectory($uploader, $baseDir);
        }
    }
    if (isset($opts['weekly-start'])) {
        $res = $pipeline->runWeekly();
        $pipeline->saveConfig($configPath);
        echo "Weekly run finished: " . ($res['weekly_file'] ?? '') . "\n";
    }

    if ($uploadRequested) {
        $uploader = buildDriveUploader($opts);
        uploadPreparedWeekFiles($uploader, $baseDir);
    }
} catch (Exception $e) {
    fwrite(STDERR, "Error: " . $e->getMessage() . "\n");
    exit(2);
}

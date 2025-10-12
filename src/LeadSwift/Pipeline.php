<?php
namespace App\LeadSwift;

use App\LeadSwift\Utils;
use App\LeadSwift\Logger;

class Pipeline {
    private array $config;
    private string $baseDir;
    private string $logFile;
    private string $lockFile;
    private bool $lockHeld = false;
    private Logger $logger;

    public function __construct(array $config, string $baseDir)
    {
        $this->config = $config;
        $this->baseDir = rtrim($baseDir, DIRECTORY_SEPARATOR);
        $logDir = $this->baseDir . '/logs';
        $this->lockFile = $this->baseDir . '/export.lock';
        if (!is_dir($logDir)) mkdir($logDir, 0777, true);
        $this->pruneOldLogs($logDir, (int)($this->config['log_retention_days'] ?? 3));
        $this->logFile = $logDir . '/lead_swift_' . date('Y-m-d_H-i-s') . '.log';
        $this->logger = new Logger($this->logFile, $this->config['log_level'] ?? 'INFO');
        register_shutdown_function(function () {
            $this->releaseLock();
        });
    }
    private function pruneOldLogs(string $logDir, int $days): void
    {
        $days = max(1, $days);
        $threshold = time() - ($days * 86400);
        foreach (glob($logDir . '/lead_swift_*.log') ?: [] as $file) {
            if (!is_file($file)) continue;
            $mtime = @filemtime($file);
            if ($mtime === false) continue;
            if ($mtime < $threshold) {
                @unlink($file);
            }
        }
    }

    private function acquireLock(): void
    {
        $ttl = max(60, (int)($this->config['lock_timeout_seconds'] ?? 21600));
        $lockPath = $this->lockFile;

        if (file_exists($lockPath)) {
            $info = $this->readLockInfo($lockPath);
            $created = (int)($info['created_at'] ?? @filemtime($lockPath) ?: 0);
            $pid = isset($info['pid']) ? (int)$info['pid'] : null;
            $age = time() - $created;
            $stale = $age > $ttl;

            if (!$stale && $pid !== null && function_exists('posix_kill')) {
                if ($pid !== getmypid() && !@posix_kill($pid, 0)) {
                    $stale = true;
                }
            }

            if ($stale) {
                if (isset($this->logger)) {
                    $this->logger->warn("Removing stale export lock (age={$age}s pid=" . ($pid ?? 'unknown') . ")");
                }
                @unlink($lockPath);
            } else {
                throw new \Exception('Another export is running (lock active)');
            }
        }

        $handle = @fopen($lockPath, 'x');
        if ($handle === false) {
            if (file_exists($lockPath)) {
                throw new \Exception('Another export is running (lock active)');
            }
            throw new \Exception('Unable to create export lock file');
        }

        $payload = json_encode([
            'pid' => getmypid(),
            'created_at' => time(),
            'hostname' => php_uname('n'),
        ], JSON_UNESCAPED_SLASHES);
        if ($payload === false) {
            $payload = (string)time();
        }
        fwrite($handle, $payload);
        fclose($handle);
        $this->lockHeld = true;
    }

    private function releaseLock(): void
    {
        if (!$this->lockHeld) return;
        if (is_file($this->lockFile)) {
            $info = $this->readLockInfo($this->lockFile);
            $pid = isset($info['pid']) ? (int)$info['pid'] : null;
            if ($pid === null || $pid === getmypid()) {
                @unlink($this->lockFile);
            }
        }
        $this->lockHeld = false;
    }

    private function readLockInfo(string $path): array
    {
        $contents = @file_get_contents($path);
        if ($contents === false) return [];
        $data = json_decode($contents, true);
        return is_array($data) ? $data : [];
    }

    public function __destruct()
    {
        $this->releaseLock();
    }

    private function syncCampaignQueues(): void {
        $kw = $this->config['campaign_keyword'] ?? null;
        $apiKey = $this->config['api_key'] ?? null;
        if (!$kw || !$apiKey) return;

        try {
            $all = $this->fetchCampaignsList($apiKey);
        } catch (\Exception $e) {
            $this->logger->warn('Campaign discovery failed: ' . $e->getMessage());
            return;
        }

        $matched = [];
        foreach ($all as $c) {
            $name = strtolower($c['name'] ?? $c['title'] ?? '');
            if ($name === '') continue;
            if (stripos($name, strtolower($kw)) !== false) {
                $id = $c['id'] ?? $c['campaign_id'] ?? null;
                if ($id !== null && $id !== '') {
                    $matched[] = (string)$id;
                }
            }
        }
        $matched = array_values(array_unique($matched));

        $queued     = array_map('strval', $this->config['campaigns_queued'] ?? []);
        $unexported = array_map('strval', $this->config['campaigns_unexported'] ?? []);
        $exported   = array_map('strval', $this->config['campaigns_exported_all'] ?? []);

        foreach ($matched as $id) {
            if (in_array($id, $exported, true)) continue;
            if (in_array($id, $unexported, true)) continue;
            if (!in_array($id, $queued, true)) {
                $queued[] = $id;
            }
        }
        $queued = array_values(array_unique(array_filter($queued, fn($v) => $v !== '')));

        $quota = max(1, (int)($this->config['search_quota'] ?? 20));
        $ready = [];
        $stillQueued = [];
        foreach ($queued as $campId) {
            try {
                [$readyCount, $totalCount] = $this->fetchCampaignSearchStats($campId);
            } catch (\Exception $e) {
                $this->logger->warn("Failed to inspect campaign {$campId}: " . $e->getMessage());
                $stillQueued[] = $campId;
                continue;
            }

            $this->logger->debug(sprintf(
                'Campaign %s readiness: ready=%d total=%d quota=%d',
                $campId,
                $readyCount,
                $totalCount,
                $quota
            ));

            if ($totalCount >= $quota) {
                $ready[] = $campId;
            } else {
                $stillQueued[] = $campId;
            }
        }

        $unexported = array_values(array_unique(array_merge($unexported, $ready)));
        $this->config['campaigns_unexported'] = $unexported;
        $this->config['campaigns_queued'] = $stillQueued;
        $this->logger->info(sprintf(
            'Queue sync result: matched=%d ready=%d queued=%d',
            count($matched),
            count($ready),
            count($stillQueued)
        ));
    }

    private function fetchCampaignsList(string $apiKey): array
    {
        $url = "https://leadswift.com/api/campaigns?api_key=" . urlencode($apiKey);
        $resp = Utils::httpGet($url);
        $json = json_decode($resp, true);
        if ($json === null && json_last_error() !== JSON_ERROR_NONE) {
            throw new \Exception('Campaign discovery returned invalid JSON');
        }
        $all = $json['data'] ?? $json;
        if (!is_array($all)) {
            throw new \Exception('Campaign discovery response missing data array');
        }
        return $all;
    }

    public function createCampaignAndSeedSearches(array $cityRows): ?string
    {
        $apiKey = trim((string)($this->config['api_key'] ?? ''));
        $campaignKeyword = trim((string)($this->config['campaign_keyword'] ?? ''));
        $searchKeyword = trim((string)($this->config['search_keyword'] ?? ''));
        $quota = (int)($this->config['search_quota'] ?? 0);

        if ($apiKey === '' || $campaignKeyword === '' || $searchKeyword === '' || $quota <= 0) {
            return null;
        }

        $today = (new \DateTimeImmutable('today'))->format('d/m/Y');
        $locations = [];
        foreach ($cityRows as $row) {
            if (!is_array($row)) continue;
            $location = isset($row[0]) ? trim((string)$row[0]) : '';
            $dateValue = isset($row[1]) ? trim((string)$row[1]) : '';
            if ($location === '' || $dateValue === '') continue;
            $parsed = CitySchedule::parseDateString($dateValue);
            if ($parsed === null) continue;
            if ($parsed->format('d/m/Y') !== $today) continue;
            $locations[] = $location;
            if (count($locations) >= $quota) break;
        }

        if (!count($locations)) {
            $this->logger->info('No city rows available for today — campaign creation skipped');
            return null;
        }

        if (count($locations) < $quota) {
            $this->logger->warn(sprintf(
                'Only %d city rows available for today (quota=%d) — submitting fewer searches',
                count($locations),
                $quota
            ));
        }

        try {
            $campaigns = $this->fetchCampaignsList($apiKey);
        } catch (\Exception $e) {
            $this->logger->warn('Unable to fetch campaigns for creation: ' . $e->getMessage());
            return null;
        }

        $nextNumber = $this->determineNextCampaignNumber($campaigns, $campaignKeyword);
        $existingTitles = [];
        foreach ($campaigns as $campaign) {
            $name = trim((string)($campaign['name'] ?? $campaign['title'] ?? ''));
            if ($name !== '') {
                $existingTitles[strtolower($name)] = true;
            }
        }

        $campaignTitle = trim($campaignKeyword . ' ' . $nextNumber);
        while (isset($existingTitles[strtolower($campaignTitle)])) {
            $nextNumber++;
            $campaignTitle = trim($campaignKeyword . ' ' . $nextNumber);
        }

        try {
            $campaignId = $this->createCampaign($apiKey, $campaignTitle);
        } catch (\Exception $e) {
            $this->logger->error('Campaign creation failed: ' . $e->getMessage());
            return null;
        }

        $success = 0;
        foreach ($locations as $location) {
            try {
                $this->submitSearch($apiKey, $campaignId, $searchKeyword, $location);
                $success++;
            } catch (\Exception $e) {
                $this->logger->warn(sprintf(
                    'Search submission failed (campaign=%s location=%s): %s',
                    $campaignId,
                    $location,
                    $e->getMessage()
                ));
            }
        }

        if ($success === 0) {
            $this->logger->warn(sprintf(
                'No searches submitted successfully for campaign %s — leaving queue unchanged',
                $campaignId
            ));
        }

        $this->ensureCampaignTracked($campaignId);

        $this->logger->info(sprintf(
            'Created campaign "%s" (id=%s); submitted %d/%d searches',
            $campaignTitle,
            $campaignId,
            $success,
            count($locations)
        ));

        return $campaignId;
    }

    public function maybeCreateCampaignForToday(array $cityRows): ?string
    {
        $today = (new \DateTimeImmutable('today'))->format('d-m-Y');
        $created = $this->getCampaignCreationMap();
        $existingId = isset($created[$today]) ? trim((string)$created[$today]) : '';
        if ($existingId !== '') {
            $this->logger->info(sprintf(
                'Campaign for today (%s) already exists: %s',
                $today,
                $existingId
            ));
            $this->ensureCampaignTracked($existingId);
            return null;
        }

        $pendingQueue = array_map('strval', $this->config['campaigns_queued'] ?? []);
        $pendingUnexported = array_map('strval', $this->config['campaigns_unexported'] ?? []);
        $pending = array_values(array_filter(array_unique(array_merge($pendingQueue, $pendingUnexported)), fn($id) => trim((string)$id) !== ''));
        if (count($pending)) {
            $this->logger->info(sprintf(
                'Pending campaigns detected (queued/unexported=%s) — skipping new campaign creation this run',
                json_encode($pending)
            ));
            return null;
        }

        $campaignId = $this->createCampaignAndSeedSearches($cityRows);
        if ($campaignId !== null) {
            $created[$today] = $campaignId;
            $this->setCampaignCreationMap($created);
        }
        return $campaignId;
    }

    private function determineNextCampaignNumber(array $campaigns, string $keyword): int
    {
        $pattern = '/^' . preg_quote($keyword, '/') . '\s*(\d+)$/i';
        $max = 0;
        foreach ($campaigns as $campaign) {
            $name = trim((string)($campaign['name'] ?? $campaign['title'] ?? ''));
            if ($name === '') continue;
            if (preg_match($pattern, $name, $m)) {
                $num = (int)$m[1];
                if ($num > $max) {
                    $max = $num;
                }
            }
        }
        return $max + 1;
    }

    private function createCampaign(string $apiKey, string $title): string
    {
        $url = "https://leadswift.com/api/campaigns?api_key=" . urlencode($apiKey);
        $resp = Utils::httpPostForm($url, ['title' => $title]);
        $json = json_decode($resp, true);
        if ($json === null && json_last_error() !== JSON_ERROR_NONE) {
            throw new \Exception('Create campaign returned invalid JSON');
        }

        $data = $json['data'] ?? $json;
        if (is_array($data)) {
            $id = $data['id'] ?? $data['campaign_id'] ?? $data['data'] ?? null;
            if ($id === null && isset($data['value'])) {
                $id = $data['value'];
            }
            if ($id !== null && $id !== '') {
                return (string)$id;
            }
        } elseif (is_string($data) && $data !== '') {
            return $data;
        }

        throw new \Exception('Create campaign response missing ID');
    }

    private function submitSearch(string $apiKey, string $campaignId, string $keyword, string $location): void
    {
        $url = "https://leadswift.com/api/searches?api_key=" . urlencode($apiKey);
        $resp = Utils::httpPostForm($url, [
            'campaign_id' => $campaignId,
            'keyword' => $keyword,
            'location' => $location,
        ]);

        $json = json_decode($resp, true);
        if ($json === null && json_last_error() !== JSON_ERROR_NONE) {
            throw new \Exception('Search submission returned invalid JSON');
        }

        if (isset($json['success']) && $json['success'] === false) {
            $msg = $json['message'] ?? $json['error'] ?? $json['data'] ?? 'unknown error';
            if (is_array($msg)) $msg = json_encode($msg);
            throw new \Exception((string)$msg);
        }
    }

    private function ensureCampaignTracked(string $campaignId): void
    {
        $campaignId = trim((string)$campaignId);
        if ($campaignId === '') return;
        $unexported = array_map('strval', $this->config['campaigns_unexported'] ?? []);
        if (in_array($campaignId, $unexported, true)) {
            return;
        }
        $queued = array_map('strval', $this->config['campaigns_queued'] ?? []);
        if (!in_array($campaignId, $queued, true)) {
            $queued[] = $campaignId;
            $this->config['campaigns_queued'] = $queued;
        }
    }

    private function getCampaignCreationMap(): array
    {
        $map = $this->config['campaigns_created_on'] ?? [];
        if (!is_array($map)) {
            $map = [];
        }
        return $map;
    }

    private function setCampaignCreationMap(array $map): void
    {
        $this->config['campaigns_created_on'] = $map;
    }

    private function removeExportedCampaignsFromCreationMap(array $exportedIds): void
    {
        if (!count($exportedIds)) return;
        $map = $this->getCampaignCreationMap();
        if (!count($map)) return;

        $exportedIds = array_map('strval', $exportedIds);
        $changed = false;
        foreach ($map as $date => $campId) {
            $campIdStr = trim((string)$campId);
            if ($campIdStr === '') continue;
            if (in_array($campIdStr, $exportedIds, true)) {
                unset($map[$date]);
                $this->logger->info(sprintf(
                    'Cleared daily campaign marker %s for campaign %s',
                    $date,
                    $campIdStr
                ));
                $changed = true;
            }
        }
        if ($changed) {
            $this->setCampaignCreationMap($map);
        }
    }

    public function runDaily(bool $noProgress = false): array {
        $this->logger->info("Starting daily run");
        // Discover campaigns by keyword and update queues
        $this->syncCampaignQueues();

        $this->acquireLock();

        try {
            // pick batch-size from config
            $batchSize = isset($this->config['export_batch_size']) ? (int)$this->config['export_batch_size'] : 0;
            $unexported = $this->config['campaigns_unexported'] ?? [];
            $toProcess = $batchSize > 0 ? array_slice($unexported, 0, $batchSize) : $unexported;
            $this->logger->info('Processing campaigns: ' . json_encode($toProcess));
            $downloaded = $this->processCampaigns($toProcess, $noProgress);
            // remove from unexported
            $processed = $downloaded['exported_ids'];
            $this->config['campaigns_unexported'] = array_values(array_diff($this->config['campaigns_unexported'] ?? [], $processed));
            $this->config['campaigns_queued'] = array_values(array_diff($this->config['campaigns_queued'] ?? [], $processed));

            $exported = $this->config['campaigns_exported_all'] ?? [];
            foreach ($processed as $id) {
                if (!in_array($id, $exported, true)) $exported[] = $id;
            }
            $this->config['campaigns_exported_all'] = array_values($exported);
            $this->removeExportedCampaignsFromCreationMap($processed);

            $this->logger->info("Daily run finished: RAW=" . count($downloaded['downloaded']) . "; EXPORTED_IDS=" . json_encode($downloaded['exported_ids']));
            return $downloaded;
        } finally {
            $this->releaseLock();
        }
    }

    private function processCampaigns(array $campaignIds, bool $noProgress): array {
        $downloaded = [];
        $exportedIds = [];
        foreach ($campaignIds as $campId) {
            // ensure campaignId is valid int/string
            if (empty($campId)) continue;
            $rawDir      = $this->baseDir . "/LeadSwift_RAW/campaign_{$campId}";
            $mergedDir   = $this->baseDir . "/LeadSwift_MERGED/campaign_{$campId}";
            $preparedDir = $this->baseDir . "/LeadSwift_PREPARED/campaign_{$campId}";
            foreach ([$rawDir,$mergedDir,$preparedDir] as $d) { if (!is_dir($d)) mkdir($d, 0777, true); }

            $campaignDownloaded = [];

            // 1) GET searches of campaign
            $searchesUrl = "https://leadswift.com/api/searches/{$campId}?api_key=" . urlencode($this->config['api_key'] ?? '');
            $this->logger->debug("Fetching searches: {$searchesUrl}");
            $resp = Utils::httpGet($searchesUrl);
            $json = json_decode($resp, true);
            $searches = $json['data'] ?? $json;
            if (!is_array($searches) || !count($searches)) continue;

            $total = count($searches);
            $pad = max(2, strlen((string)$total));
            $seq = 1;

            foreach ($searches as $s) {
                $searchId = $s['id'] ?? $s['search_id'] ?? null;
                do {
                    $fileName = sprintf("search_%0{$pad}d.csv", $seq);
                    $destPath = $rawDir . DIRECTORY_SEPARATOR . $fileName;
                    $seq++;
                } while (file_exists($destPath));
                if (!$searchId) continue;

                $beginUrl = "https://leadswift.com/api/export_leads_begin?api_key=" . urlencode($this->config['api_key'] ?? '');
                $postData = ($this->config['export_csv_params'] ?? '') . "&page=0&total_pages=9999&search_id=" . $searchId;

                $this->logger->info("BEGIN export campaign={$campId} search_id={$searchId}");
                $beginResp = Utils::httpPostRaw($beginUrl, $postData);
                $bj = json_decode($beginResp, true);
                $cronId = $bj['data'] ?? null;
                if (!$cronId) continue;

                // poll status
                $statusUrl = "https://leadswift.com/api/export_leads_status?api_key=" . urlencode($this->config['api_key'] ?? '');
                $downloadUrl = null;
                $tries = 240; $sleep = 5;
                $lastPctLogged = -5;
                $lastPageLogged = -1;
                $startTs = microtime(true);

                for ($i=1; $i<=$tries; $i++) {
                    $stResp = Utils::httpPostForm($statusUrl, ['cron_id' => $cronId]);
                    $sj = json_decode($stResp, true);
                    if ($sj === null && json_last_error() !== JSON_ERROR_NONE) {
                        $data = $stResp;
                    } else {
                        $data = $sj['data'] ?? $sj ?? null;
                    }

                    $extra = [];
                    $pct = null;

                    if (is_array($data)) {
                        list($pct,$num,$den) = Utils::detectProgress($data, $extra);
                        $maybe = $data['file_download'] ?? $data['download_url'] ?? $data['file_url'] ?? $data['url'] ?? $data['file'] ?? null;
                        $statusStr = strtolower((string)($data['status'] ?? $data['state'] ?? ''));
                        $done = ($statusStr === 'completed') || (($data['completed'] ?? false) === true) || ($pct !== null && (float)$pct >= 100);
                        if ($maybe && preg_match('~^https?://~i', $maybe)) $downloadUrl = $maybe;
                        elseif (is_string($maybe) && strpos($maybe, '//') === 0) $downloadUrl = 'https:' . $maybe;
                        if ($done && $downloadUrl) { $this->logger->info("Export completed cron_id={$cronId} download={$downloadUrl}"); break; }
                    } elseif (is_string($data) && preg_match('~https?://\S+~', $data, $m)) {
                        $downloadUrl = $m[0];
                        $pct = 100;
                        $this->logger->info("Export completed cron_id={$cronId} download={$downloadUrl}");
                        break;
                    }

                    if (!$noProgress && (PHP_SAPI === 'cli') && $pct !== null) {
                        Utils::printProgressBar("search_id={$searchId}", $pct, $extra, $startTs);
                    }

                    $toLog = false;
                    if ($pct !== null && $pct >= $lastPctLogged + 5) { $toLog = true; $lastPctLogged = floor($pct/5)*5; }
                    if (isset($extra['page']) && $extra['page'] > $lastPageLogged) { $toLog = true; $lastPageLogged = $extra['page']; }
                    if ($toLog) {
                        $this->logger->info("STATUS cron_id={$cronId}: " . json_encode(['pct'=>$pct,'extra'=>$extra]));
                    }

                    if ($downloadUrl) break;
                    sleep($sleep);
                }
                if (!$noProgress && (PHP_SAPI === 'cli')) echo "\n";

                if (!$downloadUrl) continue;

                // download CSV
                $this->logger->info("Downloading -> {$destPath}");
                $csv = Utils::httpGet($downloadUrl);
                file_put_contents($destPath, $csv);
                $campaignDownloaded[] = $destPath;
                $downloaded[] = $destPath;
            }

            if (!count($campaignDownloaded)) {
                $this->logger->info("No new downloads for campaign {$campId}");
                continue;
            }

            $mergePath = $this->mergeSourceFiles($campaignDownloaded, $mergedDir, 'merge');
            if ($mergePath === null) {
                $this->logger->warn("Merge skipped for campaign {$campId} — no readable CSV files");
                continue;
            }

            $preparedPath = $this->buildPreparedFromMerge($mergePath, $preparedDir, 'prepared');
            if ($preparedPath === null) {
                $this->logger->warn("Prepared CSV skipped for campaign {$campId} — unable to parse merge result");
                continue;
            }
            $this->logger->info("Prepared: {$preparedPath}");

            $exportedIds[] = $campId;
            $this->logger->info("Campaign processed: {$campId}");
        }
        return ['downloaded' => $downloaded, 'exported_ids' => $exportedIds];
    }

    private function fetchCampaignSearchStats(string $campaignId): array {
        $url = "https://leadswift.com/api/searches/{$campaignId}?api_key=" . urlencode($this->config['api_key'] ?? '');
        $resp = Utils::httpGet($url);
        $json = json_decode($resp, true);
        if ($json === null && json_last_error() !== JSON_ERROR_NONE) {
            throw new \Exception('Invalid JSON from searches endpoint');
        }
        if (isset($json['success']) && $json['success'] === false) {
            $msg = $json['data'] ?? ($json['message'] ?? 'Unknown error');
            if (is_array($msg)) $msg = json_encode($msg);
            throw new \Exception((string)$msg);
        }
        $data = $json['data'] ?? $json;
        if (!is_array($data)) return [0, 0];

        $ready = 0;
        $total = 0;
        foreach ($data as $search) {
            if (!is_array($search)) continue;
            $total++;
            if ($this->isSearchReady($search)) {
                $ready++;
            }
        }
        return [$ready, $total];
    }

    private function isSearchReady(array $search): bool
    {
        $statusFields = ['status', 'state', 'progress_status', 'export_status'];
        foreach ($statusFields as $field) {
            if (!empty($search[$field]) && is_string($search[$field])) {
                $status = strtolower(trim($search[$field]));
                if (in_array($status, ['completed', 'complete', 'finished', 'done', 'exported', 'ready'], true)) {
                    return true;
                }
                if (in_array($status, ['pending', 'queued', 'processing', 'running', 'in_progress'], true)) {
                    return false;
                }
            }
        }

        $boolFields = ['completed', 'is_completed', 'finished', 'is_finished', 'done'];
        foreach ($boolFields as $field) {
            if (isset($search[$field]) && $search[$field]) {
                return true;
            }
        }

        $dateFields = ['completed_at', 'finished_at', 'exported_at'];
        foreach ($dateFields as $field) {
            if (!empty($search[$field])) {
                return true;
            }
        }

        $progressFields = ['completed_percent', 'progress', 'percent', 'percentage', 'progress_percent'];
        foreach ($progressFields as $field) {
            if (isset($search[$field]) && is_numeric($search[$field]) && (float)$search[$field] >= 100) {
                return true;
            }
        }

        if (isset($search['download_url']) && is_string($search['download_url']) && $search['download_url'] !== '') {
            return true;
        }
        if (isset($search['file_url']) && is_string($search['file_url']) && $search['file_url'] !== '') {
            return true;
        }
        if (isset($search['file_download']) && is_string($search['file_download']) && $search['file_download'] !== '') {
            return true;
        }

        return false;
    }

    private function mergeSourceFiles(array $sourceFiles, string $targetDir, string $prefix): ?string {
        $valid = array_values(array_filter($sourceFiles, fn($file) => is_string($file) && is_readable($file)));
        if (!count($valid)) return null;

        natsort($valid);
        $valid = array_values($valid);

        $mergePath = $this->nextSeqPath($targetDir, $prefix, 'csv');
        $out = fopen($mergePath, 'w');
        if ($out === false) return null;

        $headerWritten = false;
        foreach ($valid as $file) {
            $handle = fopen($file, 'r');
            if ($handle === false) continue;
            $rowIndex = 0;
            while (($row = fgetcsv($handle)) !== false) {
                $rowIndex++;
                if ($rowIndex === 1) {
                    if (!$headerWritten) {
                        fputcsv($out, $row);
                        $headerWritten = true;
                    }
                    continue;
                }
                fputcsv($out, $row);
            }
            fclose($handle);
        }
        fclose($out);
        $this->logger->info("Merged: {$mergePath}");
        return $mergePath;
    }

    private function buildPreparedFromMerge(string $mergePath, string $preparedDir, string $prefix): ?string {
        $in = fopen($mergePath, 'r');
        if ($in === false) return null;

        $preparedPath = $this->nextSeqPath($preparedDir, $prefix, 'csv');
        $out = fopen($preparedPath, 'w');
        if ($out === false) {
            fclose($in);
            return null;
        }

        $header = fgetcsv($in);
        if ($header === false) {
            fclose($in);
            fclose($out);
            unlink($preparedPath);
            return null;
        }

        $map = array_change_key_case(array_flip($header), CASE_LOWER);
        $idxCompany = $map['company'] ?? $map['company_name'] ?? $map['business_name'] ?? $map['organization'] ?? $map['org'] ?? $map['name'] ?? -1;
        $idxEmail   = $map['email'] ?? $map['email_address'] ?? $map['contact_email'] ?? $map['primary_email'] ?? -1;
        $idxPhone   = $map['phone'] ?? $map['phone_number'] ?? $map['contact_phone'] ?? $map['primary_phone'] ?? $map['telephone'] ?? $map['mobile'] ?? -1;

        $byCompany = [];
        while (($row = fgetcsv($in)) !== false) {
            $company = $idxCompany >= 0 ? trim((string)($row[$idxCompany] ?? '')) : '';
            if ($company === '') $company = 'UNKNOWN';
            $email = $idxEmail >= 0 ? trim((string)($row[$idxEmail] ?? '')) : '';
            $phone = $idxPhone >= 0 ? trim((string)($row[$idxPhone] ?? '')) : '';
            if (!isset($byCompany[$company])) $byCompany[$company] = ['phone'=>'','emails'=>[], 'seen'=>[]];
            if ($byCompany[$company]['phone'] === '' && $phone !== '') {
                $byCompany[$company]['phone'] = preg_replace('/[^+0-9]/','',$phone);
            }
            if ($email !== '') {
                $k = strtolower($email);
                if (!isset($byCompany[$company]['seen'][$k])) {
                    $byCompany[$company]['seen'][$k] = 1;
                    $byCompany[$company]['emails'][] = $email;
                }
            }
        }
        fclose($in);

        fputcsv($out, ['Company','Contact Label','Email','Phone']);
        foreach ($byCompany as $company => $info) {
            if (empty($info['emails'])) continue;
            foreach ($info['emails'] as $i => $email) {
                $label = 'contact ' . ($i + 1);
                fputcsv($out, [$company, $label, $email, $info['phone']]);
            }
        }
        fclose($out);
        return $preparedPath;
    }

    public function repairCampaignsFromRaw(array $campaignIds, bool $cleanExisting = true): array {
        $rebuilt = [];
        foreach ($campaignIds as $campId) {
            if ($campId === null || $campId === '') continue;
            $campId = (string)$campId;
            $rawDir      = $this->baseDir . "/LeadSwift_RAW/campaign_{$campId}";
            $mergedDir   = $this->baseDir . "/LeadSwift_MERGED/campaign_{$campId}";
            $preparedDir = $this->baseDir . "/LeadSwift_PREPARED/campaign_{$campId}";

            if (!is_dir($rawDir)) {
                $this->logger->warn("Repair skipped for campaign {$campId} — RAW directory missing");
                continue;
            }

            foreach ([$mergedDir,$preparedDir] as $dir) {
                if (!is_dir($dir)) mkdir($dir, 0777, true);
                if ($cleanExisting) {
                    foreach (glob($dir . '/*.csv') ?: [] as $file) {
                        @unlink($file);
                    }
                }
            }

            $rawFiles = glob($rawDir . '/*.csv') ?: [];
            if (!count($rawFiles)) {
                $this->logger->warn("Repair skipped for campaign {$campId} — no RAW CSV files found");
                continue;
            }

            $mergePath = $this->mergeSourceFiles($rawFiles, $mergedDir, 'merge_repair');
            if ($mergePath === null) continue;

            $preparedPath = $this->buildPreparedFromMerge($mergePath, $preparedDir, 'prepared_repair');
            if ($preparedPath === null) continue;

            $rebuilt[$campId] = [
                'raw_files' => count($rawFiles),
                'merge' => $mergePath,
                'prepared' => $preparedPath,
            ];
            $this->logger->info("Repair completed for campaign {$campId}");
        }
        return $rebuilt;
    }

    private function nextSeqPath(string $dir, string $prefix, string $ext): string {
        $n = 1; do { $p = $dir . "/{$prefix}_{$n}.{$ext}"; $n++; } while (file_exists($p));
        return $p;
    }

    public function saveConfig(string $path): void {
        Utils::atomicWrite($path, json_encode($this->config, JSON_PRETTY_PRINT|JSON_UNESCAPED_SLASHES));
    }
}

<?php
namespace App\LeadSwift;

use App\LeadSwift\Utils;
use App\LeadSwift\Logger;

class Pipeline {
    private array $config;
    private string $baseDir;
    private string $logFile;
    private Logger $logger;

    public function __construct(array $config, string $baseDir)
    {
        $this->config = $config;
        $this->baseDir = rtrim($baseDir, DIRECTORY_SEPARATOR);
        $logDir = $this->baseDir . '/logs';
        if (!is_dir($logDir)) mkdir($logDir, 0777, true);
        $this->logFile = $logDir . '/lead_swift_' . date('Y-m-d') . '.log';
        $this->logger = new Logger($this->logFile, $this->config['log_level'] ?? 'INFO');
    }

    private function discoverCampaigns(): void {
        $kw = $this->config['campaign_keyword'] ?? null;
        if (!$kw) return;
        $url = "https://leadswift.com/api/campaigns?api_key=" . urlencode($this->config['api_key'] ?? '');
        $resp = Utils::httpGet($url);
        $json = json_decode($resp, true);
        $all = $json['data'] ?? $json;
        $found = [];
        foreach ($all as $c) {
            $name = strtolower($c['name'] ?? $c['title'] ?? '');
            if ($name === '') continue;
            if (stripos($name, strtolower($kw)) !== false) {
                $found[] = $c['id'] ?? $c['campaign_id'] ?? null;
            }
        }
        $found = array_values(array_filter($found));
        if (!count($found)) { $this->config['campaigns_all'] = []; return; }

        // Filter out campaigns that are already exported or already queued
        $exportedAll = $this->config['campaigns_exported_all'] ?? [];
        $exportedWeek = $this->config['campaigns_exported_week'] ?? [];
        $unexported = $this->config['campaigns_unexported'] ?? [];

        $exclude = array_values(array_merge((array)$exportedAll, (array)$exportedWeek, (array)$unexported));
        $exclude = array_filter($exclude, fn($v) => $v !== null && $v !== '');

        $filtered = array_values(array_filter(array_diff($found, $exclude)));
        // write only new campaigns into campaigns_all
        $this->config['campaigns_all'] = $filtered;
    }

    public function runDaily(bool $noProgress = false): array {
        $this->logger->info("Starting daily run");
        // Discover campaigns by keyword and update config
        try {
            $this->discoverCampaigns();
            $this->logger->info('Discovered campaigns: ' . json_encode($this->config['campaigns_all'] ?? []));
            // merge discovered into unexported queue
            $this->config['campaigns_unexported'] = array_values(array_unique(array_merge($this->config['campaigns_unexported'] ?? [], $this->config['campaigns_all'] ?? [])));
        } catch (\Exception $e) {
            $this->logger->warn('Discovery failed: ' . $e->getMessage());
        }
        // create lock
        $lock = $this->baseDir . '/export.lock';
        if (file_exists($lock)) {
            throw new \Exception('Another export is running');
        }
        file_put_contents($lock, (string)time());

        try {
            // pick batch-size from config
            $batchSize = isset($this->config['export_batch_size']) ? (int)$this->config['export_batch_size'] : 0;
            $unexported = $this->config['campaigns_unexported'] ?? [];
            $toProcess = $batchSize > 0 ? array_slice($unexported, 0, $batchSize) : $unexported;
            $this->logger->info('Processing campaigns: ' . json_encode($toProcess));
            $downloaded = $this->processCampaigns($toProcess, $noProgress);
            // move exported to weekly
            $exported = $this->config['campaigns_exported_week'] ?? [];
            foreach ($downloaded['exported_ids'] as $id) {
                if (!in_array($id, $exported, true)) $exported[] = $id;
            }
            $this->config['campaigns_exported_week'] = array_values($exported);
            // remove from unexported
            $this->config['campaigns_unexported'] = array_values(array_diff($this->config['campaigns_unexported'] ?? [], $downloaded['exported_ids']));

            $this->logger->info("Daily run finished: RAW=" . count($downloaded['downloaded']) . "; EXPORTED_IDS=" . json_encode($downloaded['exported_ids']));
            return $downloaded;
        } finally {
            if (file_exists($lock)) unlink($lock);
        }
    }

    public function runWeekly(): array {
        $lock = $this->baseDir . '/export.lock';
        // wait for daily to finish (no global timeout)
        while (file_exists($lock)) {
            sleep(5);
        }

        $this->logger->info("Starting weekly run");
        $weekly = $this->config['campaigns_exported_week'] ?? [];
        $preparedWeekDir = $this->baseDir . '/LeadSwift_PREPARED_WEEK';
        if (!is_dir($preparedWeekDir)) mkdir($preparedWeekDir, 0777, true);

        $dateName = date('Y-m-d') . '.csv';
        $outPath = $preparedWeekDir . DIRECTORY_SEPARATOR . $dateName;
        $out = fopen($outPath, 'w');
        if ($out === false) throw new \Exception('Cannot open weekly prepared file');

        $mergedAny = false;
        // collect prepared CSVs from campaigns_exported_week
        foreach ($weekly as $campId) {
            $preparedDir = $this->baseDir . "/LeadSwift_PREPARED/campaign_{$campId}";
            if (!is_dir($preparedDir)) continue;
            foreach (glob($preparedDir . '/*.csv') as $f) {
                $h = fopen($f, 'r'); if ($h === false) continue;
                // skip header for subsequent files
                $i = 0;
                while (($row = fgetcsv($h)) !== false) {
                    $i++;
                    if ($i === 1 && $mergedAny) continue;
                    fputcsv($out, $row);
                    $mergedAny = true;
                }
                fclose($h);
            }
        }
        fclose($out);

        // move weekly ids to all
        $all = $this->config['campaigns_exported_all'] ?? [];
        foreach ($weekly as $id) if (!in_array($id, $all, true)) $all[] = $id;
        $this->config['campaigns_exported_all'] = array_values($all);
        $this->config['campaigns_exported_week'] = [];

        $this->logger->info("Weekly aggregated file: {$outPath}");
        return ['weekly_file' => $outPath, 'ids' => $weekly];
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
                $downloaded[] = $destPath;
            }

            // Merge
            $mergePath = $this->nextSeqPath($mergedDir, 'merge', 'csv');
            $out = fopen($mergePath, 'w');
            $headerWritten = false;
            foreach ($downloaded as $f) {
                if (!is_readable($f)) continue;
                $h = fopen($f,'r'); if ($h === false) continue;
                $i = 0;
                while (($row = fgetcsv($h)) !== false) {
                    $i++;
                    if ($i === 1) {
                        if (!$headerWritten) { fputcsv($out, $row); $headerWritten = true; }
                        continue;
                    }
                    fputcsv($out, $row);
                }
                fclose($h);
            }
            fclose($out);
            $this->logger->info("Merged: {$mergePath}");

            // Prepare
            $preparedPath = $this->nextSeqPath($preparedDir, 'prepared', 'csv');
            $in = fopen($mergePath, 'r'); if ($in === false) continue;
            $out = fopen($preparedPath, 'w'); if ($out === false) { fclose($in); continue; }
            $header = fgetcsv($in);
            if ($header === false) { fclose($in); fclose($out); continue; }
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
                    if (!isset($byCompany[$company]['seen'][$k])) { $byCompany[$company]['seen'][$k] = 1; $byCompany[$company]['emails'][] = $email; }
                }
            }
            fclose($in);
            fputcsv($out, ['Company','Contact Label','Email','Phone']);
            foreach ($byCompany as $company => $info) {
                if (empty($info['emails'])) continue;
                foreach ($info['emails'] as $i => $email) {
                    $label = ($i === 0) ? 'contact 1' : 'contact 2';
                    fputcsv($out, [$company, $label, $email, $info['phone']]);
                }
            }
            fclose($out);
            $this->logger->info("Prepared: {$preparedPath}");

            $exportedIds[] = $campId;
            $this->logger->info("Campaign processed: {$campId}");
        }
        return ['downloaded' => $downloaded, 'exported_ids' => $exportedIds];
    }

    private function nextSeqPath(string $dir, string $prefix, string $ext): string {
        $n = 1; do { $p = $dir . "/{$prefix}_{$n}.{$ext}"; $n++; } while (file_exists($p));
        return $p;
    }

    public function saveConfig(string $path): void {
        Utils::atomicWrite($path, json_encode($this->config, JSON_PRETTY_PRINT|JSON_UNESCAPED_SLASHES));
    }
}



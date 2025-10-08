<?php
namespace App\LeadSwift;

class Utils {
    public static function loadEnvFile(?string $path): array {
        $vars = [];
        if ($path && is_readable($path)) {
            foreach (file($path, FILE_IGNORE_NEW_LINES|FILE_SKIP_EMPTY_LINES) as $line){
                $line = trim($line);
                if ($line === '' || $line[0] === '#') continue;
                if (strpos($line,'=') === false) continue;
                list($k,$v) = explode('=',$line,2);
                $k = trim($k); $v = trim($v);
                $len = strlen($v);
                if ($len >= 2 && ($v[0]==='"' && $v[$len-1]==='"' || $v[0]==='\'' && $v[$len-1]==='\'')) {
                    $v = substr($v,1,-1);
                }
                $vars[$k] = $v;
            }
        }
        return $vars;
    }

    public static function sanitize(string $s): string {
        $s = preg_replace('/[^A-Za-z0-9_\-]+/u','_', $s);
        return trim((string)$s, '_');
    }

    public static function httpGet(string $url): string {
        $ch = curl_init($url);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_HTTPGET, true);
        $resp = curl_exec($ch);
        if ($resp === false) { $e = curl_error($ch); curl_close($ch); throw new \Exception('GET error: '.$e); }
        curl_close($ch);
        return $resp;
    }

    public static function httpPostRaw(string $url, string $body): string {
        $ch = curl_init($url);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_POST, true);
        curl_setopt($ch, CURLOPT_POSTFIELDS, $body);
        $resp = curl_exec($ch);
        if ($resp === false) { $e = curl_error($ch); curl_close($ch); throw new \Exception('POST error: '.$e); }
        curl_close($ch);
        return $resp;
    }

    public static function httpPostForm(string $url, array $arr): string {
        $ch = curl_init($url);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_POST, true);
        curl_setopt($ch, CURLOPT_POSTFIELDS, http_build_query($arr));
        $resp = curl_exec($ch);
        if ($resp === false) { $e = curl_error($ch); curl_close($ch); throw new \Exception('POST error: '.$e); }
        curl_close($ch);
        return $resp;
    }

    public static function detectProgress($data, array &$extra): array {
        // Returns [pct|null, num|null, den|null]
        foreach (['completed_percent','progress','percent','percentage','progress_percent'] as $k) {
            if (isset($data[$k]) && is_numeric($data[$k])) {
                $p = (float)$data[$k];
                if ($p > 1 && $p <= 100) return [$p, null, null];
                if ($p >= 0 && $p <= 1) return [$p * 100, null, null];
            }
        }

        $num = $data['current_page'] ?? $data['page'] ?? $data['done_pages'] ?? null;
        $den = $data['total_pages'] ?? null;
        if (is_numeric($num) && is_numeric($den) && $den > 0) {
            $pct = max(0, min(100, ($num / $den) * 100));
            $extra['page'] = (int)$num;
            $extra['pages_total'] = (int)$den;
            return [$pct, (int)$num, (int)$den];
        }

        $num = $data['processed'] ?? $data['processed_count'] ?? $data['rows_done'] ?? null;
        $den = $data['total'] ?? $data['total_count'] ?? $data['rows_total'] ?? null;
        if (is_numeric($num) && is_numeric($den) && $den > 0) {
            $pct = max(0, min(100, ($num / $den) * 100));
            $extra['processed'] = (int)$num;
            $extra['total'] = (int)$den;
            return [$pct, (int)$num, (int)$den];
        }

        return [null, null, null];
    }

    public static function printProgressBar(string $prefix, float $pct, array $extra, float $startTs): void {
        $width = 38;
        $pct = max(0, min(100, $pct));
        $filled = (int)round($pct * $width / 100);
        $bar = str_repeat('#', $filled) . str_repeat('-', $width - $filled);

        $eta = '';
        if ($pct > 0 && $pct < 100) {
            $elapsed = microtime(true) - $startTs;
            $total_est = $elapsed * (100 / $pct);
            $remain = (int)max(0, $total_est - $elapsed);
            $eta = " | ETA ~" . gmdate("i:s", $remain);
        }

        $meta = '';
        if (isset($extra['page'], $extra['pages_total'])) {
            $meta = " | page {$extra['page']}/{$extra['pages_total']}";
        } elseif (isset($extra['processed'], $extra['total'])) {
            $meta = " | {$extra['processed']}/{$extra['total']}";
        }

        $line = sprintf("\r%s [%s] %5.1f%%%s", $prefix, $bar, $pct, $meta . $eta);
        echo $line;
        fflush(STDOUT);
    }

    // atomic write
    public static function atomicWrite(string $path, string $contents): void {
        $tmp = $path . '.tmp.' . uniqid('', true);
        file_put_contents($tmp, $contents);
        rename($tmp, $path);
    }
}



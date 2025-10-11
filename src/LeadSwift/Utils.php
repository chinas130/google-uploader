<?php
namespace App\LeadSwift;

class Utils {
    private const HTTP_TIMEOUT = 60; // seconds
    private const HTTP_CONNECT_TIMEOUT = 15; // seconds
    private const HTTP_RETRY_ATTEMPTS = 3;
    private const HTTP_RETRY_STATUS = [500, 502, 503, 504, 429];
    private const CURL_RETRY_ERRORS = [
        CURLE_OPERATION_TIMEOUTED,
        CURLE_COULDNT_RESOLVE_HOST,
        CURLE_COULDNT_CONNECT,
        CURLE_RECV_ERROR,
        CURLE_SEND_ERROR,
    ];

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
        return self::performRequest($url, function($ch) {
            curl_setopt($ch, CURLOPT_HTTPGET, true);
        }, 'GET');
    }

    public static function httpPostRaw(string $url, string $body): string {
        return self::performRequest($url, function($ch) use ($body) {
            curl_setopt($ch, CURLOPT_POST, true);
            curl_setopt($ch, CURLOPT_POSTFIELDS, $body);
        }, 'POST');
    }

    public static function httpPostForm(string $url, array $arr): string {
        return self::performRequest($url, function($ch) use ($arr) {
            curl_setopt($ch, CURLOPT_POST, true);
            curl_setopt($ch, CURLOPT_POSTFIELDS, http_build_query($arr));
            curl_setopt($ch, CURLOPT_HTTPHEADER, ['Content-Type: application/x-www-form-urlencoded']);
        }, 'POST');
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

    private static function performRequest(string $url, callable $configure, string $method): string
    {
        $attempts = max(1, self::HTTP_RETRY_ATTEMPTS);
        $backoffBase = 2.0;
        $lastError = 'unknown error';

        for ($attempt = 0; $attempt < $attempts; $attempt++) {
            $ch = curl_init($url);
            curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
            curl_setopt($ch, CURLOPT_HEADER, false);
            curl_setopt($ch, CURLOPT_CONNECTTIMEOUT, self::HTTP_CONNECT_TIMEOUT);
            curl_setopt($ch, CURLOPT_TIMEOUT, self::HTTP_TIMEOUT);
            curl_setopt($ch, CURLOPT_FAILONERROR, false);

            $configure($ch);

            $resp = curl_exec($ch);
            $errno = curl_errno($ch);
            $httpCode = (int)curl_getinfo($ch, CURLINFO_HTTP_CODE);
            $curlError = curl_error($ch);
            curl_close($ch);

            if ($resp === false) {
                $lastError = $curlError ?: 'cURL error '.$errno;
                if ($attempt < $attempts - 1 && in_array($errno, self::CURL_RETRY_ERRORS, true)) {
                    usleep((int)(($backoffBase ** $attempt) * 250000));
                    continue;
                }
                throw new \Exception(sprintf('%s %s failed: %s', $method, $url, $lastError));
            }

            if ($httpCode >= 200 && $httpCode < 300) {
                return $resp;
            }

            $message = self::extractHttpErrorMessage($resp, $httpCode);
            $lastError = "HTTP {$httpCode}: {$message}";

            if ($attempt < $attempts - 1 && in_array($httpCode, self::HTTP_RETRY_STATUS, true)) {
                usleep((int)(($backoffBase ** $attempt) * 250000));
                continue;
            }

            throw new \Exception(sprintf('%s %s failed with status %d: %s', $method, $url, $httpCode, $message));
        }

        throw new \Exception(sprintf('%s %s failed: %s', $method, $url, $lastError));
    }

    private static function extractHttpErrorMessage(string $body, int $code): string
    {
        $trimmed = trim($body);
        if ($trimmed === '') {
            return self::defaultHttpMessage($code);
        }

        $json = json_decode($trimmed, true);
        if (is_array($json)) {
            foreach (['message', 'error', 'detail'] as $key) {
                if (!empty($json[$key]) && is_string($json[$key])) {
                    return $json[$key];
                }
            }
        }

        $snippet = substr($trimmed, 0, 200);
        $snippet = preg_replace('/\\s+/', ' ', $snippet ?? '');
        return $snippet !== '' ? $snippet : self::defaultHttpMessage($code);
    }

    private static function defaultHttpMessage(int $code): string
    {
        return match ($code) {
            400 => 'Bad Request',
            401 => 'Unauthorized',
            403 => 'Forbidden',
            404 => 'Not Found',
            408 => 'Request Timeout',
            429 => 'Too Many Requests',
            500 => 'Internal Server Error',
            502 => 'Bad Gateway',
            503 => 'Service Unavailable',
            504 => 'Gateway Timeout',
            default => 'HTTP error',
        };
    }
}

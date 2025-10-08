<?php
namespace App\LeadSwift;

class Logger {
    private string $file;
    private int $levelThreshold;
    private array $levels = ['DEBUG'=>10,'INFO'=>20,'WARN'=>30,'ERROR'=>40];

    public function __construct(string $file, string $level = 'INFO')
    {
        $this->file = $file;
        $lvl = strtoupper($level);
        $this->levelThreshold = $this->levels[$lvl] ?? $this->levels['INFO'];
        // ensure directory exists
        $dir = dirname($this->file);
        if (!is_dir($dir)) mkdir($dir, 0777, true);
    }

    public function log(string $level, string $msg): void {
        $lvl = $this->levels[strtoupper($level)] ?? $this->levels['INFO'];
        if ($lvl < $this->levelThreshold) return;
        $line = '['.date('Y-m-d H:i:s')."] {$level} {$msg}\n";
        // write to stdout and file
        echo $line;
        file_put_contents($this->file, $line, FILE_APPEND);
    }

    public function debug(string $msg): void { $this->log('DEBUG', $msg); }
    public function info(string $msg): void { $this->log('INFO', $msg); }
    public function warn(string $msg): void { $this->log('WARN', $msg); }
    public function error(string $msg): void { $this->log('ERROR', $msg); }
}



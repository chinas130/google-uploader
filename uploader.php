<?php
// Thin wrapper kept for backward compatibility — use `bin/run.php --upload-to-drive` instead.
require __DIR__ . '/vendor/autoload.php';
fwrite(STDOUT, "This script has been refactored. Use `php bin/run.php --upload-to-drive`\n");
exit(0);

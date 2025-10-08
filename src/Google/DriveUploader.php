<?php
namespace App\Google;

use Google\Client;
use Google\Service\Drive;
use Google\Service\Sheets;

class DriveUploader {
    private Client $client;
    private Drive $drive;
    private Sheets $sheets;
    private array $folderCache = [];

    public function __construct(string $credentialsPath, string $tokenPath)
    {
        $client = new Client();
        $client->setApplicationName('DriveSheets Uploader');
        $client->setScopes([
            'https://www.googleapis.com/auth/drive.file',
            'https://www.googleapis.com/auth/spreadsheets'
        ]);
        $client->setAuthConfig($credentialsPath);
        $client->setAccessType('offline');
        $client->setPrompt('select_account consent');

        if (file_exists($tokenPath)) {
            $client->setAccessToken(json_decode(file_get_contents($tokenPath), true));
        }
        if ($client->isAccessTokenExpired()) {
            if ($client->getRefreshToken()) {
                $client->fetchAccessTokenWithRefreshToken($client->getRefreshToken());
            } else {
                printf("Open this link in your browser:\n%s\n", $client->createAuthUrl());
                echo "Paste the auth code: ";
                $authCode = trim(fgets(STDIN));
                $accessToken = $client->fetchAccessTokenWithAuthCode($authCode);
                $client->setAccessToken($accessToken);
                file_put_contents($tokenPath, json_encode($client->getAccessToken()));
            }
        }

        $this->client = $client;
        $this->drive = new Drive($client);
        $this->sheets = new Sheets($client);
    }

    public function uploadFile(string $name, string $content, string $mime = 'text/csv', ?string $parentId = null) : string {
        $fileMetadata = new \Google\Service\Drive\DriveFile(['name' => $name]);
        if ($parentId !== null) {
            $fileMetadata->setParents([$parentId]);
        }
        $upload = $this->drive->files->create($fileMetadata, [
            'data' => $content,
            'mimeType' => $mime,
            'uploadType' => 'multipart',
            'fields' => 'id'
        ]);
        return $upload->id;
    }

    public function uploadFileFromPath(string $path, ?string $remotePath = null, string $mime = 'text/csv'): ?string {
        if (!is_readable($path)) return null;
        $content = file_get_contents($path);
        $name = $remotePath ?? basename($path);
        if ($remotePath !== null) {
            $normalized = str_replace('\\', '/', $remotePath);
            $normalized = trim($normalized, '/');
            if ($normalized !== '') {
                $segments = array_values(array_filter(
                    explode('/', $normalized),
                    fn($seg) => $seg !== '.' && $seg !== '' && $seg !== '..'
                ));
                if (count($segments) > 0) {
                    $name = array_pop($segments);
                    $parentId = $this->ensureFolderChain($segments, null);
                    return $this->uploadFile($name, $content, $mime, $parentId);
                }
            }
        }
        return $this->uploadFile($name, $content, $mime);
    }

    public function uploadDirectoryPreserve(string $localDir, string $remotePrefix = ''): array {
        $results = [];
        if (!is_dir($localDir)) return $results;
        $it = new \RecursiveIteratorIterator(new \RecursiveDirectoryIterator($localDir));
        foreach ($it as $file) {
            if ($file->isDir()) continue;
            $localPath = $file->getPathname();
            // compute remote path relative to localDir
            $rel = ltrim(str_replace($localDir, '', $localPath), DIRECTORY_SEPARATOR);
            $remotePath = $remotePrefix ? ($remotePrefix . '/' . $rel) : $rel;
            try {
                $id = $this->uploadFileFromPath($localPath, $remotePath);
                $results[$localPath] = $id;
            } catch (\Exception $e) {
                $results[$localPath] = null;
            }
        }
        return $results;
    }

    public function updateSheet(string $spreadsheetId, string $range, array $values): void {
        $body = new \Google\Service\Sheets\ValueRange(['values' => $values]);
        $params = ['valueInputOption' => 'RAW'];
        $this->sheets->spreadsheets_values->update($spreadsheetId, $range, $body, $params);
    }

    private function ensureFolderChain(array $segments, ?string $parentId = null): ?string
    {
        $currentParent = $parentId;
        foreach ($segments as $segment) {
            $segment = trim($segment);
            if ($segment === '' || $segment === '.' || $segment === '..') continue;
            $cacheKey = ($currentParent ?? 'root') . '/' . $segment;
            if (isset($this->folderCache[$cacheKey])) {
                $currentParent = $this->folderCache[$cacheKey];
                continue;
            }

            $folderId = $this->findFolder($segment, $currentParent);
            if ($folderId === null) {
                $folderMetadata = new \Google\Service\Drive\DriveFile([
                    'name' => $segment,
                    'mimeType' => 'application/vnd.google-apps.folder'
                ]);
                if ($currentParent !== null) {
                    $folderMetadata->setParents([$currentParent]);
                }
                $created = $this->drive->files->create($folderMetadata, ['fields' => 'id']);
                $folderId = $created->id;
            }

            $this->folderCache[$cacheKey] = $folderId;
            $currentParent = $folderId;
        }
        return $currentParent;
    }

    private function findFolder(string $name, ?string $parentId = null): ?string
    {
        $safeName = str_replace("'", "\\'", $name);
        $query = "mimeType = 'application/vnd.google-apps.folder' and trashed = false and name = '{$safeName}'";
        if ($parentId !== null) {
            $query .= " and '{$parentId}' in parents";
        } else {
            $query .= " and 'root' in parents";
        }
        $list = $this->drive->files->listFiles([
            'q' => $query,
            'spaces' => 'drive',
            'fields' => 'files(id,name)',
            'pageSize' => 1,
        ]);
        $files = $list->getFiles();
        if (!$files) return null;
        return $files[0]->getId();
    }
}

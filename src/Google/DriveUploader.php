<?php
namespace App\Google;

use Google\Client;
use Google\Service\Drive;
use Google\Service\Sheets;
use Google\Service\Exception as GoogleServiceException;
use Psr\Http\Message\ResponseInterface;
use Psr\Http\Message\StreamInterface;

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
        $parentId = null;
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
                }
            }
        }
        if ($parentId !== null) {
            $existingId = $this->findFile($name, $parentId);
            if ($existingId !== null) {
                $fileMetadata = new \Google\Service\Drive\DriveFile(['name' => $name, 'parents' => [$parentId]]);
                $updated = $this->drive->files->update($existingId, $fileMetadata, [
                    'data' => $content,
                    'mimeType' => $mime,
                    'uploadType' => 'multipart',
                    'fields' => 'id'
                ]);
                return $updated->id;
            }
            return $this->uploadFile($name, $content, $mime, $parentId);
        }
        $existingRoot = $this->findFile($name, null);
        if ($existingRoot !== null) {
            $fileMetadata = new \Google\Service\Drive\DriveFile(['name' => $name]);
            $updated = $this->drive->files->update($existingRoot, $fileMetadata, [
                'data' => $content,
                'mimeType' => $mime,
                'uploadType' => 'multipart',
                'fields' => 'id'
            ]);
            return $updated->id;
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

    public function listRemoteFiles(string $remotePrefix): array
    {
        [$normalized, $segments] = $this->normalizeRemotePath($remotePrefix);
        if ($normalized === '') return [];

        $folderId = $this->locateFolderChain($segments, null);
        if ($folderId === null) return [];

        $result = [];
        $this->collectRemoteFiles($folderId, '', $result);
        return $result;
    }

    public function downloadFileToPath(string $fileId, string $destPath): void
    {
        try {
            $data = $this->downloadFileContents($fileId, false);
        } catch (GoogleServiceException $e) {
            if ($this->shouldRetryAsAbusive($e)) {
                $data = $this->downloadFileContents($fileId, true);
            } else {
                throw $e;
            }
        }
        file_put_contents($destPath, $data);
    }

    private function normalizeRemotePath(string $remotePath): array
    {
        $normalized = trim(str_replace('\\', '/', $remotePath), '/');
        $segments = array_values(array_filter(
            explode('/', $normalized),
            fn($seg) => $seg !== '' && $seg !== '.' && $seg !== '..'
        ));
        return [$normalized, $segments];
    }

    private function collectRemoteFiles(string $folderId, string $relativePath, array &$result): void
    {
        $pageToken = null;
        do {
            $params = [
                'q' => sprintf("'%s' in parents and trashed = false", $folderId),
                'spaces' => 'drive',
                'fields' => 'nextPageToken, files(id,name,mimeType)',
                'pageSize' => 1000,
            ];
            if ($pageToken !== null) {
                $params['pageToken'] = $pageToken;
            }
            $response = $this->drive->files->listFiles($params);
            $files = $response->getFiles();
            if ($files) {
                foreach ($files as $file) {
                    $name = $file->getName();
                    $mime = $file->getMimeType();
                    if ($mime === 'application/vnd.google-apps.folder') {
                        $this->collectRemoteFiles($file->getId(), $relativePath . $name . '/', $result);
                        continue;
                    }
                    $result[$relativePath . $name] = [
                        'id' => $file->getId(),
                        'mimeType' => $mime,
                    ];
                }
            }
            $pageToken = $response->getNextPageToken();
        } while ($pageToken);
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

    private function findFile(string $name, ?string $parentId = null): ?string
    {
        $safeName = str_replace("'", "\\'", $name);
        $query = "mimeType != 'application/vnd.google-apps.folder' and trashed = false and name = '{$safeName}'";
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

    public function purgeRemotePrefix(string $remotePrefix): void
    {
        $normalized = str_replace('\\', '/', $remotePrefix);
        $normalized = trim($normalized, '/');
        if ($normalized === '') return;
        $segments = array_values(array_filter(
            explode('/', $normalized),
            fn($seg) => $seg !== '' && $seg !== '.' && $seg !== '..'
        ));
        if (!count($segments)) return;
        $folderId = $this->locateFolderChain($segments, null);
        if ($folderId === null) return;
        $this->deleteFolderContents($folderId, false);
    }

    private function locateFolderChain(array $segments, ?string $parentId = null): ?string
    {
        $currentParent = $parentId;
        foreach ($segments as $segment) {
            $segment = trim($segment);
            if ($segment === '' || $segment === '.' || $segment === '..') continue;
            $folderId = $this->findFolder($segment, $currentParent);
            if ($folderId === null) return null;
            $currentParent = $folderId;
        }
        return $currentParent;
    }

    private function deleteFolderContents(string $folderId, bool $deleteSelf): void
    {
        $pageToken = null;
        do {
            $params = [
                'q' => sprintf("'%s' in parents and trashed = false", $folderId),
                'spaces' => 'drive',
                'fields' => 'nextPageToken, files(id, mimeType)',
                'pageSize' => 1000,
            ];
            if ($pageToken !== null) {
                $params['pageToken'] = $pageToken;
            }
            $response = $this->drive->files->listFiles($params);
            $files = $response->getFiles();
            if ($files) {
                foreach ($files as $file) {
                    $childId = $file->getId();
                    $mime = $file->getMimeType();
                    try {
                        if ($mime === 'application/vnd.google-apps.folder') {
                            $this->deleteFolderContents($childId, true);
                        } else {
                            $this->drive->files->delete($childId);
                        }
                    } catch (\Exception $e) {
                        fwrite(STDERR, "Failed to delete remote item {$childId}: " . $e->getMessage() . "\n");
                    }
                }
            }
            $pageToken = $response->getNextPageToken();
        } while ($pageToken);

        if ($deleteSelf) {
            try {
                $this->drive->files->delete($folderId);
            } catch (\Exception $e) {
                fwrite(STDERR, "Failed to delete remote folder {$folderId}: " . $e->getMessage() . "\n");
            }
        }
    }

    private function downloadFileContents(string $fileId, bool $acknowledgeAbuse): string
    {
        $params = ['alt' => 'media'];
        if ($acknowledgeAbuse) {
            $params['acknowledgeAbuse'] = true;
        }
        $response = $this->drive->files->get($fileId, $params);

        if ($response instanceof ResponseInterface) {
            return $response->getBody()->getContents();
        }
        if ($response instanceof StreamInterface) {
            return $response->getContents();
        }
        if (is_object($response) && method_exists($response, 'getBody')) {
            return $response->getBody()->getContents();
        }
        return (string)$response;
    }

    private function shouldRetryAsAbusive(GoogleServiceException $e): bool
    {
        if ($e->getCode() !== 403) return false;
        $errors = $e->getErrors();
        if (!is_array($errors)) return false;
        foreach ($errors as $err) {
            if (!isset($err['reason'])) continue;
            if ($err['reason'] === 'cannotDownloadAbusiveFile') {
                return true;
            }
        }
        return false;
    }
}

<?php
namespace App\Google;

use Google\Client;
use Google\Service\Sheets;

class SheetsManager
{
    private Sheets $sheets;

    public function __construct(string $credentialsPath, string $tokenPath)
    {
        $client = new Client();
        $client->setApplicationName('LeadSwift Sheets Manager');
        $client->setScopes([
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive.file'
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

        $this->sheets = new Sheets($client);
    }

    /**
     * @return array<int, array<int, string>>
     */
    public function getValues(string $spreadsheetId, string $range): array
    {
        $response = $this->sheets->spreadsheets_values->get($spreadsheetId, $range);
        return $response->getValues() ?? [];
    }

    /**
     * @param array<int, array<int, string>> $values
     */
    public function updateValues(string $spreadsheetId, string $range, array $values): void
    {
        $body = new \Google\Service\Sheets\ValueRange(['values' => $values]);
        $params = ['valueInputOption' => 'RAW'];
        $this->sheets->spreadsheets_values->update($spreadsheetId, $range, $body, $params);
    }
}


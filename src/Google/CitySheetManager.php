<?php
namespace App\Google;

use App\LeadSwift\CitySchedule;

class CitySheetManager
{
    private SheetsManager $sheets;

    public function __construct(string $credentialsPath, string $tokenPath)
    {
        $this->sheets = new SheetsManager($credentialsPath, $tokenPath);
    }

    public function rescheduleSheet(string $spreadsheetId, string $range = 'E2:F', int $batchSize = 20): int
    {
        $rows = $this->sheets->getValues($spreadsheetId, $range);
        if (!count($rows)) {
            return 0;
        }

        $rows = $this->normaliseRowWidths($rows, 2);
        $dateColumnIndex = 1;

        $filteredRows = [];
        foreach ($rows as $row) {
            if (isset($row[0]) && trim((string)$row[0]) !== '') {
                $filteredRows[] = $row;
            }
        }
        if (!count($filteredRows)) {
            return 0;
        }

        [$rescheduled, $updatedCount] = CitySchedule::assignDatesToRows($filteredRows, $dateColumnIndex, $batchSize);

        $updatedValues = [];
        $idx = 0;
        foreach ($rows as $row) {
            if (isset($row[0]) && trim((string)$row[0]) !== '') {
                $newDate = $rescheduled[$idx][1] ?? '';
                $updatedValues[] = [$row[0], $newDate];
                $idx++;
            } else {
                $updatedValues[] = [$row[0] ?? '', $row[1] ?? ''];
            }
        }

        $rangeToUpdate = $this->expandRange($range, count($updatedValues));
        $this->sheets->updateValues($spreadsheetId, $rangeToUpdate, $updatedValues);
        return $updatedCount;
    }

    /**
     * @param array<int, array<int, string>> $rows
     * @return array<int, array<int, string>>
     */
    private function normaliseRowWidths(array $rows, int $minColumns): array
    {
        foreach ($rows as &$row) {
            while (count($row) < $minColumns) {
                $row[] = '';
            }
        }
        unset($row);
        return $rows;
    }

    private function expandRange(string $range, int $rowsCount): string
    {
        if ($rowsCount <= 0) return $range;

        $sheetPrefix = '';
        $rangePart = $range;
        if (strpos($range, '!') !== false) {
            [$sheet, $rangePart] = explode('!', $range, 2);
            $sheetPrefix = trim($sheet) !== '' ? trim($sheet) . '!' : '';
            $rangePart = ltrim($rangePart);
        }

        if (preg_match('/^([A-Z]+)(\d+):([A-Z]+)(\d+)$/i', $rangePart, $m)) {
            $startCol = strtoupper($m[1]);
            $startRow = (int)$m[2];
            $endCol = strtoupper($m[3]);
            $endRow = $startRow + $rowsCount - 1;
            return $sheetPrefix . "{$startCol}{$startRow}:{$endCol}{$endRow}";
        }

        if (preg_match('/^([A-Z]+)(\d+):([A-Z]+)$/i', $rangePart, $m)) {
            $startCol = strtoupper($m[1]);
            $startRow = (int)$m[2];
            $endCol = strtoupper($m[3]);
            $endRow = $startRow + $rowsCount - 1;
            return $sheetPrefix . "{$startCol}{$startRow}:{$endCol}{$endRow}";
        }

        if (preg_match('/^([A-Z]+)(\d+)$/i', $rangePart, $m)) {
            $startCol = strtoupper($m[1]);
            $startRow = (int)$m[2];
            $endRow = $startRow + $rowsCount - 1;
            return $sheetPrefix . "{$startCol}{$startRow}:{$startCol}{$endRow}";
        }

        if (preg_match('/^([A-Z]+):([A-Z]+)$/i', $rangePart, $m)) {
            $startCol = strtoupper($m[1]);
            $endCol = strtoupper($m[2]);
            $startRow = 1;
            $endRow = $rowsCount;
            return $sheetPrefix . "{$startCol}{$startRow}:{$endCol}{$endRow}";
        }

        // default fallback
        if ($sheetPrefix !== '') {
            return $range;
        }
        return "E2:F" . (1 + $rowsCount);
    }
}

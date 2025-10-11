<?php
namespace App\LeadSwift;

use DateInterval;
use DateTimeImmutable;

class CitySchedule
{
    private const DEFAULT_DATE_FORMAT = 'd/m/Y';
    private const EXCEL_EPOCH = '1899-12-30';

    public static function rescheduleCsv(string $csvPath, int $batchSize = 20): int
    {
        if (!is_readable($csvPath)) {
            throw new \RuntimeException("City CSV not readable: {$csvPath}");
        }

        $tmp = $csvPath . '.tmp.' . uniqid('', true);
        $in = fopen($csvPath, 'r');
        $out = fopen($tmp, 'w');
        if (!$in || !$out) {
            if ($in) fclose($in);
            if ($out) fclose($out);
            throw new \RuntimeException("Unable to open CSV for processing: {$csvPath}");
        }

        $header = fgetcsv($in);
        if ($header !== false) {
            fputcsv($out, $header);
        }

        $rows = [];
        while (($row = fgetcsv($in)) !== false) {
            $rows[] = $row;
        }
        fclose($in);

        $indexes = [];
        $targets = [];
        foreach ($rows as $idx => $row) {
            $leadSwift = $row[4] ?? '';
            if (trim((string)$leadSwift) === '') continue;
            $indexes[] = $idx;
            $targets[] = $row;
        }

        $updated = 0;

        if (count($targets)) {
            [$assignedTargets, $updated] = self::assignDatesToRows($targets, 5, $batchSize);
            foreach ($indexes as $offset => $rowIndex) {
                $rows[$rowIndex] = $assignedTargets[$offset];
            }
        }

        foreach ($rows as $row) {
            fputcsv($out, $row);
        }
        fclose($out);

        rename($tmp, $csvPath);
        return $updated;
    }

    /**
     * @param array<int,array<int,string>> $rows
     * @return array{0: array<int,array<int,string>>, 1: int}
     */
    public static function assignDatesToRows(array $rows, int $dateColumnIndex, int $batchSize = 20): array
    {
        $result = [];
        $updated = 0;
        $firstValid = self::findFirstValidDate($rows, $dateColumnIndex);
        $currentDate = $firstValid;
        $countInCurrent = 0;

        foreach ($rows as $row) {
            $row = self::ensureColumnCapacity($row, $dateColumnIndex);
            $existingRaw = $row[$dateColumnIndex] ?? '';
            $parsed = self::parseDateString($existingRaw);

            if ($parsed !== null) {
                if ($currentDate !== null && $parsed->format('Y-m-d') === $currentDate->format('Y-m-d')) {
                    $countInCurrent++;
                } else {
                    $currentDate = $parsed;
                    $countInCurrent = 1;
                }
                $result[] = $row;
                continue;
            }

            if ($currentDate === null) {
                $currentDate = $firstValid ?? new DateTimeImmutable('today');
                $countInCurrent = 0;
            }

            if ($countInCurrent >= $batchSize) {
                $currentDate = $currentDate->add(new DateInterval('P1D'));
                $countInCurrent = 0;
            }

            $row[$dateColumnIndex] = $currentDate->format(self::DEFAULT_DATE_FORMAT);
            $countInCurrent++;
            $updated++;

            $result[] = $row;
        }

        return [$result, $updated];
    }

    public static function parseDateString(?string $value): ?DateTimeImmutable
    {
        if ($value === null) return null;
        $trim = trim($value);
        if ($trim === '') return null;

        // Excel numeric
        if (is_numeric($trim)) {
            $days = (float)$trim;
            $epoch = new DateTimeImmutable(self::EXCEL_EPOCH);
            return $epoch->add(new DateInterval('P' . (int)$days . 'D'));
        }

        $formats = ['d/m/Y', 'm/d/Y', 'Y-m-d'];
        foreach ($formats as $fmt) {
            $dt = DateTimeImmutable::createFromFormat($fmt, $trim);
            if ($dt !== false) {
                return $dt;
            }
        }
        return null;
    }

    /**
     * @return array<int,string>
     */
    public static function generateDates(int $rowsCount, ?DateTimeImmutable $startDate = null, int $batchSize = 20): array
    {
        if ($rowsCount <= 0) return [];
        $start = $startDate ?? new DateTimeImmutable('today');
        $dates = [];

        for ($i = 0; $i < $rowsCount; $i++) {
            $offset = intdiv($i, max(1, $batchSize));
            $dates[] = $start->add(new DateInterval('P' . $offset . 'D'))->format(self::DEFAULT_DATE_FORMAT);
        }

        return $dates;
    }

    /**
     * @param array<int,string> $row
     * @return array<int,string>
     */
    private static function ensureColumnCapacity(array $row, int $column): array
    {
        $count = count($row);
        if ($column < $count) {
            return $row;
        }
        for ($i = $count; $i <= $column; $i++) {
            $row[$i] = '';
        }
        return $row;
    }

    /**
     * @param array<int,array<int,string>> $rows
     */
    private static function findFirstValidDate(array $rows, int $dateColumnIndex): ?DateTimeImmutable
    {
        foreach ($rows as $row) {
            if (!isset($row[$dateColumnIndex])) continue;
            $parsed = self::parseDateString($row[$dateColumnIndex]);
            if ($parsed !== null) {
                return $parsed;
            }
        }
        return null;
    }
}

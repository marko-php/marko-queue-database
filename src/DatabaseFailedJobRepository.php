<?php

declare(strict_types=1);

namespace Marko\Queue\Database;

use DateMalformedStringException;
use Marko\Database\Config\DatabaseTimezoneConfig;
use Marko\Database\Connection\ConnectionInterface;
use Marko\Queue\FailedJob;
use Marko\Queue\FailedJobRepositoryInterface;

/**
 * Stores failed jobs in the failed_jobs table. failed_at is written and read in
 * the database timezone (`database.timezone`, UTC by default), so the instant
 * read back is the instant stored, whatever PHP default timezone either process
 * runs in.
 */
class DatabaseFailedJobRepository implements FailedJobRepositoryInterface
{
    public function __construct(
        private ConnectionInterface $connection,
        private DatabaseTimezoneConfig $databaseTimezoneConfig,
    ) {}

    public function store(
        FailedJob $failedJob,
    ): void {
        $sql = 'INSERT INTO failed_jobs (id, queue, payload, exception, failed_at) VALUES (?, ?, ?, ?, ?)';

        $this->connection->execute($sql, [
            $failedJob->id,
            $failedJob->queue,
            $failedJob->payload,
            $failedJob->exception,
            $this->databaseTimezoneConfig->format($failedJob->failedAt),
        ]);
    }

    /**
     * @throws DateMalformedStringException
     */
    public function all(): array
    {
        $sql = 'SELECT id, queue, payload, exception, failed_at FROM failed_jobs ORDER BY failed_at DESC';
        $rows = $this->connection->query($sql);

        return array_map(fn (array $row): FailedJob => $this->hydrateFailedJob($row), $rows);
    }

    /**
     * @throws DateMalformedStringException
     */
    private function hydrateFailedJob(
        array $row,
    ): FailedJob {
        return new FailedJob(
            id: $row['id'],
            queue: $row['queue'],
            payload: $row['payload'],
            exception: $row['exception'],
            failedAt: $this->databaseTimezoneConfig->parse($row['failed_at']),
        );
    }

    /**
     * @throws DateMalformedStringException
     */
    public function find(
        string $id,
    ): ?FailedJob {
        $sql = 'SELECT id, queue, payload, exception, failed_at FROM failed_jobs WHERE id = ?';
        $rows = $this->connection->query($sql, [$id]);

        if ($rows === []) {
            return null;
        }

        return $this->hydrateFailedJob($rows[0]);
    }

    public function delete(
        string $id,
    ): bool {
        $sql = 'DELETE FROM failed_jobs WHERE id = ?';
        $affectedRows = $this->connection->execute($sql, [$id]);

        return $affectedRows > 0;
    }

    public function clear(): int
    {
        $sql = 'DELETE FROM failed_jobs';

        return $this->connection->execute($sql);
    }

    public function count(): int
    {
        $sql = 'SELECT COUNT(*) as count FROM failed_jobs';
        $rows = $this->connection->query($sql);

        return (int) $rows[0]['count'];
    }
}

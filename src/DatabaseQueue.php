<?php

declare(strict_types=1);

namespace Marko\Queue\Database;

use DateTimeImmutable;
use Marko\Database\Connection\ConnectionInterface;
use Marko\Database\Connection\TransactionInterface;
use Marko\Queue\Exceptions\SerializationException;
use Marko\Queue\FailedJob;
use Marko\Queue\FailedJobRepositoryInterface;
use Marko\Queue\JobEnvelope;
use Marko\Queue\JobInterface;
use Marko\Queue\QueueInterface;
use Random\RandomException;

/**
 * Database-backed queue.
 *
 * Attempt counting: the `attempts` column is the authoritative count. Every
 * reservation increments it, so it counts attempts *started*, including ones
 * whose worker died mid-run (fatal error, OOM, SIGKILL) and never released the
 * job. The `attempts` value inside the serialized payload lags behind and is
 * brought up to the column value whenever the job is popped or released:
 *
 * - pop() hands the worker a job whose attempts equal the reservations made
 *   before this one. The worker then increments it for the current run.
 * - release() rewrites the payload with the column value, so the count
 *   survives the round trip through the table.
 * - A popped job whose earlier reservations already reach its max attempts
 *   (job maxAttempts, or the queue default) is moved to the failed-job store
 *   instead of being returned, so a job that always crashes its worker cannot
 *   be retried forever.
 */
readonly class DatabaseQueue implements QueueInterface
{
    public function __construct(
        private ConnectionInterface $connection,
        private JobEnvelope $jobEnvelope,
        private FailedJobRepositoryInterface $failedJobRepository,
        private string $table = 'jobs',
        private string $defaultQueue = 'default',
        private int $retryAfter = 90,
        private int $maxAttempts = 3,
    ) {}

    /**
     * @throws RandomException|SerializationException
     */
    public function push(
        JobInterface $job,
        ?string $queue = null,
    ): string {
        return $this->insertJob($job, $queue, 0);
    }

    /**
     * @throws RandomException|SerializationException
     */
    public function later(
        int $delay,
        JobInterface $job,
        ?string $queue = null,
    ): string {
        return $this->insertJob($job, $queue, $delay);
    }

    /**
     * @throws RandomException|SerializationException
     */
    private function insertJob(
        JobInterface $job,
        ?string $queue,
        int $delay,
    ): string {
        $id = $this->generateId();
        $job->setId($id);

        $now = new DateTimeImmutable();
        $availableAt = $delay > 0 ? $now->modify("+$delay seconds") : $now;

        $this->connection->execute(
            "INSERT INTO $this->table (id, queue, payload, attempts, reserved_at, available_at, created_at) VALUES (:id, :queue, :payload, :attempts, :reserved_at, :available_at, :created_at)",
            [
                'id' => $id,
                'queue' => $queue ?? $this->defaultQueue,
                'payload' => $this->jobEnvelope->wrap($job->serialize()),
                'attempts' => $job->attempts,
                'reserved_at' => null,
                'available_at' => $availableAt->format('Y-m-d H:i:s'),
                'created_at' => $now->format('Y-m-d H:i:s'),
            ],
        );

        return $id;
    }

    /**
     * @throws RandomException
     */
    private function generateId(): string
    {
        return sprintf(
            '%04x%04x-%04x-%04x-%04x-%04x%04x%04x',
            random_int(0, 0xffff),
            random_int(0, 0xffff),
            random_int(0, 0xffff),
            random_int(0, 0x0fff) | 0x4000,
            random_int(0, 0x3fff) | 0x8000,
            random_int(0, 0xffff),
            random_int(0, 0xffff),
            random_int(0, 0xffff),
        );
    }

    /**
     * @throws SerializationException
     */
    public function pop(
        ?string $queue = null,
    ): ?JobInterface {
        $queueName = $queue ?? $this->defaultQueue;

        // A false result means the reserved job had exhausted its attempts through
        // crashed reservations and was moved to the failed-job store; try the next one.
        do {
            $result = $this->connection instanceof TransactionInterface
                ? $this->connection->transaction(fn (): JobInterface|false|null => $this->reserveNext($queueName))
                : $this->reserveNext($queueName);
        } while ($result === false);

        return $result;
    }

    /**
     * Reserve the next available job.
     *
     * @return JobInterface|false|null the reserved job, null when none is available, or
     *                                 false when the reserved job was moved to failed jobs
     * @throws SerializationException
     */
    private function reserveNext(
        string $queueName,
    ): JobInterface|false|null {
        $now = new DateTimeImmutable();
        $reclaimCutoff = $now->modify("-$this->retryAfter seconds");

        $skipLocked = $this->supportsSkipLocked() ? ' FOR UPDATE SKIP LOCKED' : '';

        $rows = $this->connection->query(
            "SELECT * FROM $this->table WHERE queue = :queue AND (reserved_at IS NULL OR reserved_at <= :reclaim_cutoff) AND available_at <= :now ORDER BY available_at ASC, created_at ASC LIMIT 1$skipLocked",
            [
                'queue' => $queueName,
                'now' => $now->format('Y-m-d H:i:s'),
                'reclaim_cutoff' => $reclaimCutoff->format('Y-m-d H:i:s'),
            ],
        );

        if ($rows === []) {
            return null;
        }

        $row = $rows[0];

        $affectedRows = $this->connection->execute(
            "UPDATE $this->table SET reserved_at = :reserved_at, attempts = attempts + 1 WHERE id = :id AND (reserved_at IS NULL OR reserved_at <= :reclaim_cutoff)",
            [
                'reserved_at' => $now->format('Y-m-d H:i:s'),
                'id' => $row['id'],
                'reclaim_cutoff' => $reclaimCutoff->format('Y-m-d H:i:s'),
            ],
        );

        if ($affectedRows === 0) {
            return null;
        }

        $job = $this->unwrapJob($row['payload'], (int) $row['attempts']);
        $job->setId($row['id']);

        $maxAttempts = $job->maxAttempts ?? $this->maxAttempts;

        if ($job->attempts >= $maxAttempts) {
            $this->failExhaustedJob($job, $row['queue'], $maxAttempts, $now);

            return false;
        }

        return $job;
    }

    /**
     * @throws SerializationException
     */
    private function failExhaustedJob(
        JobInterface $job,
        string $queueName,
        int $maxAttempts,
        DateTimeImmutable $now,
    ): void {
        $this->failedJobRepository->store(new FailedJob(
            id: $job->id,
            queue: $queueName,
            payload: $this->jobEnvelope->wrap($job->serialize()),
            exception: "Job $job->id exceeded max attempts after worker crash or timeout: "
                . "$job->attempts of $maxAttempts attempts were reserved without being completed or released.",
            failedAt: $now,
        ));
        $this->delete($job->id);
    }

    /**
     * Verify and unserialize a stored payload, bringing its attempt count up to the given floor.
     *
     * @throws SerializationException
     */
    private function unwrapJob(
        string $payload,
        int $attempts,
    ): JobInterface {
        /** @var JobInterface $job */
        $job = unserialize($this->jobEnvelope->verifyAndUnwrap($payload));

        while ($job->attempts < $attempts) {
            $job->incrementAttempts();
        }

        return $job;
    }

    private function supportsSkipLocked(): bool
    {
        return match ($this->connection->driverName()) {
            'mysql', 'pgsql' => true,
            default => false,
        };
    }

    public function size(
        ?string $queue = null,
    ): int {
        $queueName = $queue ?? $this->defaultQueue;
        $now = new DateTimeImmutable();

        $rows = $this->connection->query(
            "SELECT COUNT(*) as count FROM $this->table WHERE queue = :queue AND reserved_at IS NULL AND available_at <= :now",
            [
                'queue' => $queueName,
                'now' => $now->format('Y-m-d H:i:s'),
            ],
        );

        return (int) ($rows[0]['count'] ?? 0);
    }

    public function clear(
        ?string $queue = null,
    ): int {
        $queueName = $queue ?? $this->defaultQueue;

        return $this->connection->execute(
            "DELETE FROM $this->table WHERE queue = :queue",
            [
                'queue' => $queueName,
            ],
        );
    }

    public function delete(
        string $jobId,
    ): bool {
        $affectedRows = $this->connection->execute(
            "DELETE FROM $this->table WHERE id = :id",
            [
                'id' => $jobId,
            ],
        );

        return $affectedRows > 0;
    }

    /**
     * Release a reserved job back to the queue, persisting its attempt count in the payload.
     *
     * @throws SerializationException
     */
    public function release(
        string $jobId,
        int $delay = 0,
    ): bool {
        $rows = $this->connection->query(
            "SELECT payload, attempts FROM $this->table WHERE id = :id",
            [
                'id' => $jobId,
            ],
        );

        if ($rows === []) {
            return false;
        }

        $job = $this->unwrapJob($rows[0]['payload'], (int) $rows[0]['attempts']);

        $now = new DateTimeImmutable();
        $availableAt = $delay > 0 ? $now->modify("+$delay seconds") : $now;

        $affectedRows = $this->connection->execute(
            "UPDATE $this->table SET payload = :payload, attempts = :attempts, reserved_at = :reserved_at, available_at = :available_at WHERE id = :id",
            [
                'payload' => $this->jobEnvelope->wrap($job->serialize()),
                'attempts' => $job->attempts,
                'reserved_at' => null,
                'available_at' => $availableAt->format('Y-m-d H:i:s'),
                'id' => $jobId,
            ],
        );

        return $affectedRows > 0;
    }
}

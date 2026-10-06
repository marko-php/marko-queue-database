<?php

declare(strict_types=1);

use Marko\Database\PgSql\Query\PgSqlQueryBuilderFactory;
use Marko\Encryption\Config\EncryptionConfig;
use Marko\Queue\Database\DatabaseFailedJobRepository;
use Marko\Queue\Database\DatabaseQueue;
use Marko\Queue\Database\Tests\Fixtures\AlwaysFailingJob;
use Marko\Queue\Database\Tests\Fixtures\SqliteConnection;
use Marko\Queue\Database\Tests\Fixtures\TestJob;
use Marko\Queue\JobEnvelope;
use Marko\Testing\Fake\FakeConfigRepository;

function attemptsEnvelope(): JobEnvelope
{
    return new JobEnvelope(new EncryptionConfig(new FakeConfigRepository([
        'encryption.key' => 'attempts-test-key',
    ])));
}

function attemptsQueue(
    SqliteConnection $connection,
    int $maxAttempts = 3,
): DatabaseQueue {
    return new DatabaseQueue(
        connection: $connection,
        jobEnvelope: attemptsEnvelope(),
        failedJobRepository: new DatabaseFailedJobRepository($connection),
        queryBuilderFactory: new PgSqlQueryBuilderFactory($connection),
        maxAttempts: $maxAttempts,
    );
}

/**
 * Simulate a worker that died mid-run: push the reservation past retry_after.
 */
function expireReservation(
    SqliteConnection $connection,
    string $jobId,
): void {
    $connection->execute(
        'UPDATE jobs SET reserved_at = :reserved_at WHERE id = :id',
        ['reserved_at' => new DateTimeImmutable('-1 day')->format('Y-m-d H:i:s'), 'id' => $jobId],
    );
}

function jobsRow(
    SqliteConnection $connection,
    string $jobId,
): ?array {
    return $connection->query('SELECT * FROM jobs WHERE id = :id', ['id' => $jobId])[0] ?? null;
}

describe('DatabaseQueue attempt persistence', function (): void {
    it('increments the attempts column when reserving a job', function (): void {
        $connection = SqliteConnection::withQueueTables();
        $queue = attemptsQueue($connection);
        $id = $queue->push(new TestJob());

        $queue->pop();

        expect((int) jobsRow($connection, $id)['attempts'])->toBe(1);
    });

    it('rewrites the payload with the incremented attempt count on release', function (): void {
        $connection = SqliteConnection::withQueueTables();
        $queue = attemptsQueue($connection);
        $id = $queue->push(new TestJob());

        $job = $queue->pop();
        $job->incrementAttempts();
        $queue->release($id, 0);

        $row = jobsRow($connection, $id);
        $stored = unserialize(attemptsEnvelope()->verifyAndUnwrap($row['payload']));

        expect($stored->attempts)->toBe(1)
            ->and((int) $row['attempts'])->toBe(1)
            ->and($row['reserved_at'])->toBeNull()
            ->and($queue->pop()->attempts)->toBe(1);
    });

    it('syncs the popped job attempts with reservations that were never released', function (): void {
        $connection = SqliteConnection::withQueueTables();
        $queue = attemptsQueue($connection);
        $id = $queue->push(new TestJob());

        $queue->pop();
        expireReservation($connection, $id);
        $reclaimed = $queue->pop();

        expect($reclaimed)->not->toBeNull()
            ->and($reclaimed->id)->toBe($id)
            ->and($reclaimed->attempts)->toBe(1);
    });

    it('moves a job exhausted by crashed reservations to failed_jobs instead of returning it', function (): void {
        $connection = SqliteConnection::withQueueTables();
        $queue = attemptsQueue($connection, maxAttempts: 2);
        $failedJobRepository = new DatabaseFailedJobRepository($connection);
        $id = $queue->push(new TestJob('crashy'), 'emails');

        $queue->pop('emails');
        expireReservation($connection, $id);
        $queue->pop('emails');
        expireReservation($connection, $id);

        expect($queue->pop('emails'))->toBeNull()
            ->and(jobsRow($connection, $id))->toBeNull();

        $failedJob = $failedJobRepository->find($id);

        expect($failedJob)->not->toBeNull()
            ->and($failedJob->queue)->toBe('emails')
            ->and($failedJob->exception)->toContain('exceeded max attempts after worker crash or timeout')
            ->and(unserialize(attemptsEnvelope()->verifyAndUnwrap($failedJob->payload))->message)->toBe('crashy');
    });

    it('returns the next available job after failing a crash-exhausted one', function (): void {
        $connection = SqliteConnection::withQueueTables();
        $queue = attemptsQueue($connection, maxAttempts: 1);
        $crashedId = $queue->push(new TestJob('crashy'));

        $queue->pop();
        expireReservation($connection, $crashedId);
        $healthyId = $queue->push(new TestJob('healthy'));

        expect($queue->pop()?->id)->toBe($healthyId);
    });

    it('prefers the job maxAttempts over the queue default when failing crashed jobs', function (): void {
        $connection = SqliteConnection::withQueueTables();
        $queue = attemptsQueue($connection, maxAttempts: 1);
        $id = $queue->push(new AlwaysFailingJob(maxAttempts: 5));

        $queue->pop();
        expireReservation($connection, $id);

        expect($queue->pop()?->attempts)->toBe(1);
    });
});

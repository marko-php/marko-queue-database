<?php

declare(strict_types=1);

use Marko\Core\Command\Input;
use Marko\Core\Command\Output;
use Marko\Core\Container\Container;
use Marko\Core\Path\ProjectPaths;
use Marko\Database\Config\DatabaseConfig;
use Marko\Database\PgSql\Connection\PgSqlConnection;
use Marko\Database\PgSql\Query\PgSqlQueryBuilderFactory;
use Marko\Encryption\Config\EncryptionConfig;
use Marko\Queue\Command\RetryCommand;
use Marko\Queue\Database\DatabaseFailedJobRepository;
use Marko\Queue\Database\DatabaseQueue;
use Marko\Queue\Database\Migration\CreateFailedJobsTable;
use Marko\Queue\Database\Migration\CreateJobsTable;
use Marko\Queue\Database\Tests\Fixtures\PrivateStateJob;
use Marko\Queue\JobEnvelope;
use Marko\Queue\QueueConfig;
use Marko\Queue\Worker;
use Marko\Testing\Fake\FakeConfigRepository;

/*
 * These tests run against a real PostgreSQL server. They are skipped (with the reason)
 * unless DB_HOST is set and reachable. Run locally with, for example:
 *
 *   docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=secret -e POSTGRES_DB=marko_test postgres:17
 *   DB_HOST=127.0.0.1 DB_PORT=5432 DB_DATABASE=marko_test DB_USERNAME=postgres DB_PASSWORD=secret \
 *     composer test -- packages/queue-database/tests/Integration
 */

/**
 * Connect to the test server. With $migrate (the default) the jobs and failed_jobs
 * tables are dropped and recreated; pass false for a second connection to the
 * same tables.
 *
 * @return array{connection: ?PgSqlConnection, skipReason: ?string}
 */
function pgsqlQueueConnection(
    bool $migrate = true,
): array {
    $host = getenv('DB_HOST');

    if ($host === false || $host === '') {
        return [
            'connection' => null,
            'skipReason' => 'PostgreSQL integration test: set DB_HOST, DB_PORT, DB_DATABASE, DB_USERNAME and DB_PASSWORD to run it',
        ];
    }

    $config = [
        'driver' => 'pgsql',
        'host' => $host,
        'port' => (int) (getenv('DB_PORT') ?: 5432),
        'database' => getenv('DB_DATABASE') ?: 'marko_test',
        'username' => getenv('DB_USERNAME') ?: 'postgres',
        'password' => getenv('DB_PASSWORD') ?: '',
    ];

    $basePath = sys_get_temp_dir() . '/marko-queue-pgsql-' . getmypid();

    if (!is_dir("$basePath/config")) {
        mkdir("$basePath/config", recursive: true);
    }

    file_put_contents("$basePath/config/database.php", '<?php return ' . var_export($config, true) . ';');

    $connection = new PgSqlConnection(new DatabaseConfig(new ProjectPaths($basePath)));

    try {
        $connection->connect();
    } catch (Throwable $e) {
        return [
            'connection' => null,
            'skipReason' => "PostgreSQL not reachable at {$config['host']}:{$config['port']}: {$e->getMessage()}",
        ];
    }

    if (!$migrate) {
        return ['connection' => $connection, 'skipReason' => null];
    }

    $connection->execute('DROP TABLE IF EXISTS jobs');
    $connection->execute('DROP TABLE IF EXISTS failed_jobs');
    new CreateJobsTable()->up($connection);
    new CreateFailedJobsTable()->up($connection);

    return ['connection' => $connection, 'skipReason' => null];
}

function pgsqlEnvelope(): JobEnvelope
{
    return new JobEnvelope(new EncryptionConfig(new FakeConfigRepository(['encryption.key' => 'pgsql-test-key'])));
}

function pgsqlQueue(
    PgSqlConnection $connection,
): DatabaseQueue {
    return new DatabaseQueue(
        connection: $connection,
        jobEnvelope: pgsqlEnvelope(),
        failedJobRepository: new DatabaseFailedJobRepository($connection),
        queryBuilderFactory: new PgSqlQueryBuilderFactory($connection),
        maxAttempts: 1,
    );
}

describe('database queue on PostgreSQL', function (): void {
    it(
        'round-trips a job with private and protected properties through push and pop on PostgreSQL',
        function (): void {
            ['connection' => $connection, 'skipReason' => $skipReason] = pgsqlQueueConnection();

            if ($connection === null) {
                $this->markTestSkipped($skipReason);
            }

            $queue = pgsqlQueue($connection);
            $queue->push(new PrivateStateJob("se\0cret", 'shared'));

            /** @var PrivateStateJob $popped */
            $popped = $queue->pop();

            expect($popped)->toBeInstanceOf(PrivateStateJob::class)
                ->and($popped->secret())->toBe("se\0cret")
                ->and($popped->shared())->toBe('shared')
                ->and(str_contains($connection->query('SELECT payload FROM jobs')[0]['payload'], "\0"))->toBeFalse();
        },
    );

    it('round-trips a failed job through failed_jobs and queue:retry on PostgreSQL', function (): void {
        ['connection' => $connection, 'skipReason' => $skipReason] = pgsqlQueueConnection();

        if ($connection === null) {
            $this->markTestSkipped($skipReason);
        }

        $queue = pgsqlQueue($connection);
        $failedJobRepository = new DatabaseFailedJobRepository($connection);
        $id = $queue->push(new PrivateStateJob('secret', 'shared', fail: true));

        $worker = new Worker(
            $queue,
            $failedJobRepository,
            new QueueConfig(new FakeConfigRepository([
                'queue.queue' => 'default',
                'queue.max_attempts' => 1,
            ])),
            pgsqlEnvelope(),
            new Container(),
        );
        $worker->work(once: true);

        expect($failedJobRepository->find($id))->not->toBeNull();

        $retry = new RetryCommand($failedJobRepository, $queue, pgsqlEnvelope());
        $exitCode = $retry->execute(new Input(['marko', 'queue:retry', $id]), new Output(fopen('php://memory', 'r+')));

        /** @var PrivateStateJob $retried */
        $retried = $queue->pop();

        expect($exitCode)->toBe(0)
            ->and($failedJobRepository->count())->toBe(0)
            ->and($retried)->toBeInstanceOf(PrivateStateJob::class)
            ->and($retried->secret())->toBe('secret')
            ->and($retried->shared())->toBe('shared')
            ->and($retried->attempts)->toBe(0);
    });

    it('never hands the same job to two concurrent reservations on PostgreSQL', function (): void {
        ['connection' => $workerA, 'skipReason' => $skipReason] = pgsqlQueueConnection();

        if ($workerA === null) {
            $this->markTestSkipped($skipReason);
        }

        ['connection' => $workerB] = pgsqlQueueConnection(migrate: false);
        $first = pgsqlQueue($workerA)->push(new PrivateStateJob('first', 'shared'));
        $second = pgsqlQueue($workerA)->push(new PrivateStateJob('second', 'shared'));

        // Worker B fails fast instead of hanging if the SELECT ever waits on A's lock again.
        $workerB->execute("SET lock_timeout = '2s'");

        // Worker A's pop() nests in an open transaction, so its row lock is held while B pops.
        $workerA->beginTransaction();

        try {
            $poppedByA = pgsqlQueue($workerA)->pop();
            $poppedByB = pgsqlQueue($workerB)->pop();
        } finally {
            $workerA->rollback();
            $workerB->disconnect();
        }

        expect($poppedByA?->id)->toBe($first)
            ->and($poppedByB?->id)->toBe($second);
    });

    it('skips a locked job instead of waiting for it on PostgreSQL', function (): void {
        ['connection' => $workerA, 'skipReason' => $skipReason] = pgsqlQueueConnection();

        if ($workerA === null) {
            $this->markTestSkipped($skipReason);
        }

        ['connection' => $workerB] = pgsqlQueueConnection(migrate: false);
        pgsqlQueue($workerA)->push(new PrivateStateJob('only', 'shared'));
        $workerB->execute("SET lock_timeout = '2s'");

        $workerA->beginTransaction();

        try {
            $poppedByA = pgsqlQueue($workerA)->pop();
            $startedAt = microtime(true);
            $poppedByB = pgsqlQueue($workerB)->pop();
            $waited = microtime(true) - $startedAt;
        } finally {
            $workerA->rollback();
            $workerB->disconnect();
        }

        expect($poppedByA)->not->toBeNull()
            ->and($poppedByB)->toBeNull()
            ->and($waited)->toBeLessThan(1.0);
    });
})->group('integration-services');

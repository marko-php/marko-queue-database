<?php

declare(strict_types=1);

use Marko\Core\Container\ContainerInterface;
use Marko\Database\Connection\ConnectionInterface;
use Marko\Queue\Database\DatabaseFailedJobRepository;
use Marko\Queue\Database\DatabaseQueue;
use Marko\Queue\FailedJobRepositoryInterface;
use Marko\Queue\JobEnvelope;
use Marko\Queue\QueueConfig;
use Marko\Queue\QueueInterface;

return [
    'bindings' => [
        QueueInterface::class => static function (ContainerInterface $container): DatabaseQueue {
            $config = $container->get(QueueConfig::class);

            return new DatabaseQueue(
                connection: $container->get(ConnectionInterface::class),
                jobEnvelope: $container->get(JobEnvelope::class),
                failedJobRepository: $container->get(FailedJobRepositoryInterface::class),
                defaultQueue: $config->queue(),
                retryAfter: $config->retryAfter(),
                maxAttempts: $config->maxAttempts(),
            );
        },
        FailedJobRepositoryInterface::class => DatabaseFailedJobRepository::class,
    ],
];

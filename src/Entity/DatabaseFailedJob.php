<?php

declare(strict_types=1);

namespace Marko\Queue\Database\Entity;

use Marko\Database\Attributes\Column;
use Marko\Database\Attributes\Table;
use Marko\Database\Entity\Entity;

/**
 * Owns the schema of the failed_jobs table, so db:migrate creates it and db:diff reports drift.
 * DatabaseFailedJobRepository reads and writes the table with its own SQL, not through this entity.
 */
#[Table('failed_jobs')]
class DatabaseFailedJob extends Entity
{
    #[Column(type: 'varchar', length: 36, primaryKey: true)]
    public string $id;

    #[Column(type: 'varchar', length: 255)]
    public string $queue;

    #[Column(type: 'text')]
    public string $payload;

    #[Column(type: 'text')]
    public string $exception;

    #[Column(type: 'timestamp', default: 'CURRENT_TIMESTAMP')]
    public string $failedAt;
}

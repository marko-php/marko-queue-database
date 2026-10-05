<?php

declare(strict_types=1);

namespace Marko\Queue\Database\Tests\Fixtures;

use Marko\Queue\FailedJob;
use Marko\Queue\FailedJobRepositoryInterface;

class InMemoryFailedJobRepository implements FailedJobRepositoryInterface
{
    /** @var array<string, FailedJob> */
    public array $storedJobs = [];

    public function store(
        FailedJob $failedJob,
    ): void {
        $this->storedJobs[$failedJob->id] = $failedJob;
    }

    public function all(): array
    {
        return array_values($this->storedJobs);
    }

    public function find(
        string $id,
    ): ?FailedJob {
        return $this->storedJobs[$id] ?? null;
    }

    public function delete(
        string $id,
    ): bool {
        if (!isset($this->storedJobs[$id])) {
            return false;
        }

        unset($this->storedJobs[$id]);

        return true;
    }

    public function clear(): int
    {
        $count = count($this->storedJobs);
        $this->storedJobs = [];

        return $count;
    }

    public function count(): int
    {
        return count($this->storedJobs);
    }
}

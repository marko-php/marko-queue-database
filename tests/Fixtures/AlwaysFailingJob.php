<?php

declare(strict_types=1);

namespace Marko\Queue\Database\Tests\Fixtures;

use Marko\Queue\Job;
use RuntimeException;

class AlwaysFailingJob extends Job
{
    public static int $handled = 0;

    public function __construct(
        ?int $maxAttempts = null,
    ) {
        $this->maxAttempts = $maxAttempts;
    }

    public function handle(): void
    {
        self::$handled++;

        throw new RuntimeException('This job always fails');
    }
}

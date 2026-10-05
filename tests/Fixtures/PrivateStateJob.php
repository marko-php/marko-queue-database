<?php

declare(strict_types=1);

namespace Marko\Queue\Database\Tests\Fixtures;

use Marko\Queue\Job;
use RuntimeException;

/**
 * Job with private and protected state, so serialize() emits NUL bytes.
 */
class PrivateStateJob extends Job
{
    public function __construct(
        private string $secret,
        protected string $shared,
        private bool $fail = false,
    ) {}

    public function secret(): string
    {
        return $this->secret;
    }

    public function shared(): string
    {
        return $this->shared;
    }

    public function handle(): void
    {
        if ($this->fail) {
            throw new RuntimeException('PrivateStateJob failed on purpose');
        }
    }
}

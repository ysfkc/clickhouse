<?php

declare(strict_types=1);

/*
 * This file is part of the zotlo/clickhouse package.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Ysfkc\ClickHouse;

use Psr\Log\LoggerInterface;

/**
 * Singleton factory for {@see ClickHouseClient}.
 *
 * Call {@see configure()} once at application bootstrap, then obtain the shared
 * client instance via {@see getInstance()} from anywhere in your code.
 *
 * Basic usage:
 *
 *   ClickHouseClientService::configure([
 *       'host'           => 'http://clickhouse-server:8123',
 *       'database'       => 'analytics',
 *       'username'       => 'default',
 *       'password'       => 'secret',
 *       'timeout'        => 30,
 *       'connectTimeout' => 10,
 *   ]);
 *
 *   $client = ClickHouseClientService::getInstance();
 *
 * To inject a PSR-3 logger:
 *
 *   ClickHouseClientService::configure([...], $myLogger);
 */
class ClickHouseClientService
{
    /** @var array<string, mixed> Explicitly provided configuration. */
    private static array $config = [];

    /** @var LoggerInterface|null Optional PSR-3 logger injected via configure(). */
    private static ?LoggerInterface $logger = null;

    /** @var array<string, ClickHouseClient> Singleton instance pool, keyed by class name. */
    private static array $instance = [];

    private function __construct()
    {
    }

    private function __clone()
    {
    }

    /**
     * Prevents deserialization from creating a new instance.
     *
     * @throws \RuntimeException Always.
     */
    public function __wakeup(): void
    {
        throw new \RuntimeException(
            'ClickHouseClientService cannot be deserialized.'
        );
    }

    /**
     * Configures the connection settings used by {@see getInstance()}.
     *
     * Must be called before the first {@see getInstance()} call. Calling it again
     * invalidates the previously created singleton so that the next call to
     * {@see getInstance()} creates a fresh client with the new settings.
     * To reset without reconfiguring, use {@see reset()}.
     *
     * @param array{
     *     host: string,
     *     database?: string,
     *     username?: string,
     *     password?: string,
     *     timeout?: int,
     *     connectTimeout?: int
     * }                     $config Connection settings.
     * @param LoggerInterface|null $logger Optional PSR-3 logger forwarded to the client.
     */
    public static function configure(array $config, ?LoggerInterface $logger = null): void
    {
        self::$config   = $config;
        self::$logger   = $logger;
        self::$instance = [];
    }

    /**
     * Resets both the configuration and the singleton instance.
     *
     * Primarily intended for use in unit tests.
     */
    public static function reset(): void
    {
        self::$config   = [];
        self::$logger   = null;
        self::$instance = [];
    }

    /**
     * Returns the shared {@see ClickHouseClient} instance.
     *
     * The instance is created lazily on the first call and reused on subsequent calls.
     * {@see configure()} must be called before this method.
     *
     * @throws \RuntimeException When no configuration has been provided.
     *
     * @return ClickHouseClient
     */
    public static function getInstance(): ClickHouseClient
    {
        $key = ClickHouseClient::class;

        if (isset(self::$instance[$key])) {
            return self::$instance[$key];
        }

        if (! empty(self::$config)) {
            self::$instance[$key] = new ClickHouseClient(
                self::$config['host'],
                self::$config['database']       ?? 'default',
                self::$config['username']       ?? '',
                self::$config['password']       ?? '',
                (int) (self::$config['timeout'] ?? 30),
                (int) (self::$config['connectTimeout'] ?? 10),
                self::$logger,
            );

            return self::$instance[$key];
        }

        throw new \RuntimeException(
            'ClickHouseClientService: no configuration found. ' .
            'Call ClickHouseClientService::configure([...]) before using getInstance().'
        );
    }
}


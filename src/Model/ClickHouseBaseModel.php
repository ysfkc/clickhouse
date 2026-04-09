<?php

declare(strict_types=1);

/*
 * This file is part of the zotlo/clickhouse package.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Ysfkc\ClickHouse\Model;

use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Ysfkc\ClickHouse\ClickHouseClient;
use Ysfkc\ClickHouse\ClickHouseClientService;
use Ysfkc\ClickHouse\ClickHouseResponse;
use Ysfkc\ClickHouse\Exception\ClickHouseException;
use Ysfkc\ClickHouse\QueryBuilder;

/**
 * ORM-style base model for ClickHouse tables.
 *
 * Extend this class (or a project-level subclass) to add typed access, custom
 * query helpers, and INSERT/SELECT support to any ClickHouse table.
 *
 * Typical inheritance chain:
 *
 *   ClickHouseBaseModel  ←  AppBaseModel  ←  MetricSnapshot, OrderEvent, …
 *
 * Minimal model definition:
 *
 *   class MetricSnapshot extends ClickHouseBaseModel
 *   {
 *       protected static string $_tableName = 'metric_snapshots';
 *
 *       protected static array $_columns = [
 *           'uuid'        => 'String',
 *           'source'      => 'LowCardinality(String)',
 *           'metric_name' => 'String',
 *           'value'       => 'Float64',
 *           'status'      => 'LowCardinality(String)',
 *           'event_date'  => 'Date',
 *           'recorded_at' => 'DateTime',
 *       ];
 *   }
 *
 * This class has no dependency on any particular PHP framework.
 * Inject a PSR-3 logger once at application bootstrap via {@see setDefaultLogger()}.
 */
abstract class ClickHouseBaseModel implements \JsonSerializable
{
    /**
     * ClickHouse table name. Must be overridden in every concrete model.
     *
     * @var string
     */
    protected static string $_tableName;

    /**
     * Column-to-type map: ['column_name' => 'ClickHouseType'].
     *
     * Serves as a whitelist for INSERT and SELECT operations, and as the source
     * of truth for ClickHouse type resolution in query bindings.
     *
     * Example:
     *   ['user_id' => 'Int32', 'event_name' => 'String', 'created_at' => 'DateTime']
     *
     * @var array<string, string>
     */
    protected static array $_columns = [];

    /**
     * PSR-3 logger used by this model instance.
     *
     * @var LoggerInterface
     */
    protected LoggerInterface $logger;

    /**
     * Application-wide default logger, set once via {@see setDefaultLogger()}.
     *
     * @var LoggerInterface|null
     */
    private static ?LoggerInterface $defaultLogger = null;

    /**
     * Hydrated row data — accessed via magic __get / __set.
     *
     * @var array<string, mixed>
     */
    protected array $attributes = [];

    /**
     * @param array<string, mixed> $attributes Initial attribute values (used during hydration).
     */
    public function __construct(array $attributes = [])
    {
        $this->attributes = $attributes;
        $this->logger     = self::resolveLogger();
    }

    /**
     * Sets the PSR-3 logger used by all model instances.
     *
     * Call this once at application bootstrap (e.g. in a service provider or bootstrap file):
     *
     *   ClickHouseBaseModel::setDefaultLogger($container->get(LoggerInterface::class));
     *
     * When omitted, a {@see NullLogger} is used and all log output is silently discarded.
     *
     * @param LoggerInterface $logger
     */
    public static function setDefaultLogger(LoggerInterface $logger): void
    {
        self::$defaultLogger = $logger;
    }

    /**
     * Resolves the logger for the current instance.
     *
     * Resolution order:
     *   1. Logger provided via {@see setDefaultLogger()}.
     *   2. NullLogger (silent fallback).
     *
     * @return LoggerInterface
     */
    private static function resolveLogger(): LoggerInterface
    {
        return self::$defaultLogger ?? new NullLogger();
    }

    // ─────────────────────────────────────────────
    // Static Factory & Query Methods
    // ─────────────────────────────────────────────

    /**
     * Returns a new {@see QueryBuilder} pre-configured for this model's table.
     *
     * @return QueryBuilder
     */
    public static function query(): QueryBuilder
    {
        return QueryBuilder::table(static::$_tableName);
    }

    /**
     * Returns all rows up to $limit (default 1000).
     *
     * ⚠️ Avoid calling this without a limit on large tables.
     *
     * @param int $limit Maximum number of rows to fetch.
     *
     * @throws ClickHouseException On query failure.
     *
     * @return ClickHouseCollection
     */
    public static function all(int $limit = 1000): ClickHouseCollection
    {
        $response = static::query()
            ->select(static::getSelectColumns())
            ->limit($limit)
            ->get();

        return static::hydrateCollection($response);
    }

    /**
     * Finds rows matching the given equality conditions.
     *
     * Column keys may be provided in camelCase or snake_case; both are resolved
     * against the model's column map. Array values produce an IN condition.
     *
     * @param array<string, mixed> $conditions  Filter conditions (e.g. ['userId' => 1] or ['user_id' => 1]).
     * @param int                  $limit       Maximum rows to return (default 1000).
     * @param string               $orderByColumn    Column to order by (camelCase or snake_case).
     * @param string               $orderByDirection Sort direction ("ASC" or "DESC").
     *
     * @throws ClickHouseException On query failure.
     *
     * @return ClickHouseCollection
     */
    public static function find(
        array $conditions = [],
        int $limit = 1000,
        string $orderByColumn = '',
        string $orderByDirection = 'DESC'
    ): ClickHouseCollection {
        $qb = static::query()->select(static::getSelectColumns());

        foreach ($conditions as $column => $value) {
            $dbColumn = static::toDbKey($column);
            $type     = static::getColumnType($dbColumn);

            if (\is_array($value)) {
                $qb->where($dbColumn, 'IN', $value, $type);
            } else {
                $qb->where($dbColumn, '=', $value, $type);
            }
        }

        if (! empty($orderByColumn)) {
            $qb->orderBy(static::toDbKey($orderByColumn), $orderByDirection);
        }

        $qb->limit($limit);

        return static::hydrateCollection($qb->get());
    }

    /**
     * Returns the first row matching the given conditions, or null when none is found.
     *
     * Column keys may be provided in camelCase or snake_case.
     *
     * @param array<string, mixed> $conditions Filter conditions.
     *
     * @throws ClickHouseException On query failure.
     *
     * @return static|null
     */
    public static function findFirst(array $conditions = []): ?static
    {
        $qb = static::query()->select(static::getSelectColumns());

        foreach ($conditions as $column => $value) {
            $dbColumn = static::toDbKey($column);
            $type     = static::getColumnType($dbColumn);

            if (\is_array($value)) {
                $qb->where($dbColumn, 'IN', $value, $type);
            } else {
                $qb->where($dbColumn, '=', $value, $type);
            }
        }

        return static::hydrateFirst($qb->limit(1)->get());
    }

    /**
     * Returns the number of rows matching the given conditions.
     *
     * @param array<string, mixed> $conditions Filter conditions (camelCase or snake_case keys).
     * @param string               $column     Column to count (default: "*").
     *
     * @throws ClickHouseException On query failure.
     *
     * @return int
     */
    public static function countBy(array $conditions = [], string $column = '*'): int
    {
        $qb = static::query();

        foreach ($conditions as $col => $value) {
            $dbCol = static::toDbKey($col);
            $type  = static::getColumnType($dbCol);

            if (\is_array($value)) {
                $qb->where($dbCol, 'IN', $value, $type);
            } else {
                $qb->where($dbCol, '=', $value, $type);
            }
        }

        return $qb->count($column);
    }

    /**
     * Returns a {@see QueryBuilder} pre-filtered with a BETWEEN date range.
     *
     * The column type is resolved from the model's column map.
     *
     * @param string $startDate  Lower bound date (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS).
     * @param string $endDate    Upper bound date.
     * @param string $dateColumn Column to filter on (default: "event_date").
     *
     * @return QueryBuilder
     */
    public static function queryBetweenDates(
        string $startDate,
        string $endDate,
        string $dateColumn = 'event_date'
    ): QueryBuilder {
        $type = static::getColumnType($dateColumn);

        return static::query()
            ->select(static::getSelectColumns())
            ->whereBetween($dateColumn, $startDate, $endDate, $type);
    }

    /**
     * Inserts a single row into the model's table.
     *
     * Column types are resolved automatically from the model's $_columns map.
     *
     * @param array<string, mixed> $data Associative array of column → value pairs.
     *
     * @throws ClickHouseException On query failure.
     *
     * @return ClickHouseResponse
     */
    public static function insertRow(array $data): ClickHouseResponse
    {
        $insertData = [];
        foreach ($data as $column => $value) {
            $insertData[$column] = [
                'value' => $value,
                'type'  => static::getColumnType($column),
            ];
        }

        return static::query()->insertData($insertData);
    }

    /**
     * Inserts multiple rows in a single INSERT … VALUES statement.
     *
     * Column types are resolved automatically from the model's $_columns map.
     *
     * @param string[]          $columns Column names (e.g. ['user_id', 'event_name']).
     * @param array<int, array> $rows    Rows to insert, each an indexed array matching $columns.
     *
     * @throws ClickHouseException On query failure.
     *
     * @return ClickHouseResponse
     */
    public static function insertBatch(array $columns, array $rows): ClickHouseResponse
    {
        $types = array_map(fn (string $col) => static::getColumnType($col), $columns);

        return static::query()->insertBatch($columns, $rows, $types);
    }

    /**
     * Executes a raw parameterized SELECT query.
     *
     * ⚠️ Security: $query must be a hardcoded string. Never pass user-supplied input
     * as the query string — use $params for all dynamic values.
     *
     * Correct:
     *   MetricSnapshot::rawQuery(
     *       'SELECT * FROM metric_snapshots WHERE user_id = {userId:Int32}',
     *       ['userId' => $userInput]
     *   );
     *
     * Incorrect:
     *   MetricSnapshot::rawQuery('SELECT * FROM metric_snapshots WHERE user_id = ' . $userInput);
     *
     * @param string               $query  Parameterized SQL string with {name:Type} placeholders.
     * @param array<string, mixed> $params Bound parameter values.
     *
     * @throws ClickHouseException When parameters are provided but no placeholder is found,
     *                             or on query failure.
     *
     * @return ClickHouseResponse
     */
    public static function rawQuery(string $query, array $params = []): ClickHouseResponse
    {
        if (! empty($params) && ! preg_match('/\{[a-zA-Z_][a-zA-Z0-9_]*:[a-zA-Z]/', $query)) {
            throw new ClickHouseException(
                'rawQuery: parameters were provided but no {name:Type} placeholder was found in the query. ' .
                'You may have embedded values directly into the SQL string instead of using $params.'
            );
        }

        return QueryBuilder::raw($query, $params)->get();
    }

    /**
     * Executes a raw DDL or administrative command.
     *
     * ⚠️ Security: $query must be a hardcoded string. DDL statements (ALTER, DROP, etc.)
     * are permitted, but SSRF/exfiltration vectors (url(), file(), remote(), s3(), etc.)
     * are blocked by {@see assertCommandSafe()}.
     *
     * @param string               $query  DDL/admin command to execute.
     * @param array<string, mixed> $params Optional bound parameters.
     *
     * @throws ClickHouseException When a dangerous pattern is detected or on query failure.
     *
     * @return ClickHouseResponse
     */
    public static function rawCommand(string $query, array $params = []): ClickHouseResponse
    {
        if (! empty($params) && ! preg_match('/\{[a-zA-Z_][a-zA-Z0-9_]*:[a-zA-Z]/', $query)) {
            throw new ClickHouseException(
                'rawCommand: parameters were provided but no {name:Type} placeholder was found in the query.'
            );
        }

        static::assertCommandSafe($query);

        return static::resolveClient()->command($query, $params);
    }


    // ─────────────────────────────────────────────
    // Attribute Access
    // ─────────────────────────────────────────────

    /**
     * Returns the value of an attribute by name, or null when not set.
     *
     * @param string $propertyName Attribute name (camelCase, as hydrated).
     *
     * @return mixed
     */
    public function __get(string $propertyName): mixed
    {
        return $this->attributes[$propertyName] ?? null;
    }

    /**
     * Sets an attribute value.
     *
     * @param string $name
     * @param mixed  $value
     */
    public function __set(string $name, mixed $value): void
    {
        $this->attributes[$name] = $value;
    }

    /**
     * Returns true when the attribute exists (even if its value is null).
     *
     * @param string $name
     *
     * @return bool
     */
    public function __isset(string $name): bool
    {
        return array_key_exists($name, $this->attributes);
    }

    /**
     * Returns all attributes as a camelCase associative array.
     *
     * Internal state (logger, etc.) is excluded.
     *
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        return $this->attributes;
    }

    /**
     * Returns all attributes as a snake_case associative array (matching DB column names).
     *
     * @return array<string, mixed>
     */
    public function toSnakeArray(): array
    {
        $map    = static::columnMap();
        $result = [];

        foreach ($this->attributes as $camelKey => $value) {
            $result[$map[$camelKey] ?? $camelKey] = $value;
        }

        return $result;
    }

    /**
     * Returns attributes for json_encode(). Internal state is excluded.
     *
     * @return array<string, mixed>
     */
    public function jsonSerialize(): array
    {
        return $this->attributes;
    }

    /**
     * Returns attributes for var_dump(). Internal state is excluded.
     *
     * @return array<string, mixed>
     */
    public function __debugInfo(): array
    {
        return $this->attributes;
    }

    /**
     * Returns the model's table name.
     *
     * @return string
     */
    public static function getTableName(): string
    {
        return static::$_tableName;
    }

    // ─────────────────────────────────────────────
    // Date Helpers
    // ─────────────────────────────────────────────

    /**
     * Returns the default start date for a date-range query (6 days ago, midnight UTC).
     *
     * @param string $format Date format string (default: "Y-m-d 00:00:00").
     *
     * @return string
     */
    public static function createDefaultIntervalStartDate(string $format = 'Y-m-d 00:00:00'): string
    {
        return gmdate($format, strtotime('-6 days'));
    }

    /**
     * Returns the default end date for a date-range query (today, end of day UTC).
     *
     * @param string $format Date format string (default: "Y-m-d 23:59:59").
     *
     * @return string
     */
    public static function createDefaultIntervalEndDate(string $format = 'Y-m-d 23:59:59'): string
    {
        return gmdate($format);
    }

    /**
     * Validates that a date string matches one of the accepted ClickHouse date formats.
     *
     * Accepted formats:
     *   - Y-m-d                  (e.g. "2026-01-15")
     *   - Y-m-d H:i:s            (e.g. "2026-01-15 10:30:00")
     *   - Y-m-d H:i:s.u          (e.g. "2026-01-15 10:30:00.123456")
     *
     * Useful in subclasses to validate date values before passing them to
     * {@see QueryBuilder::whereBetween()} or {@see QueryBuilder::where()}.
     *
     * @param string $date
     *
     * @return bool
     */
    protected static function isValidDateValue(string $date): bool
    {
        return (bool) preg_match(
            '/^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])( ([01]\d|2[0-3]):[0-5]\d:[0-5]\d(\.\d+)?)?$/',
            $date
        );
    }

    /**
     * Handles an invalid date value in custom query methods.
     *
     * Throws in development (for fast feedback) and logs a warning in production
     * so that the invalid filter is silently skipped rather than breaking the query.
     *
     * Usage in subclasses:
     *
     *   if (!static::isValidDateValue($startDate)) {
     *       $this->handleInvalidDate('startDate', $startDate);
     *       return;
     *   }
     *
     * @param string $field  Field name (e.g. "startDate").
     * @param mixed  $value  The invalid value.
     *
     * @throws \InvalidArgumentException In development environments (APPLICATION_ENV=development).
     */
    protected function handleInvalidDate(string $field, mixed $value): void
    {
        $message = sprintf(
            "Invalid date format for '%s' = '%s'. Expected Y-m-d or Y-m-d H:i:s. Table: '%s'.",
            $field,
            $value,
            static::$_tableName
        );

        if (getenv('APPLICATION_ENV') === 'development') {
            throw new \InvalidArgumentException($message);
        }

        $this->logger->warning('ClickHouseBaseModel: ' . $message . ' Date filter was not applied.');
    }

    // ─────────────────────────────────────────────
    // Column Map (camelCase ↔ snake_case)
    // ─────────────────────────────────────────────

    /**
     * Returns a map of camelCase attribute names to snake_case database column names.
     *
     * @return array<string, string>  ['camelKey' => 'snake_key']
     */
    protected static function columnMap(): array
    {
        $map = [];
        foreach (array_keys(static::$_columns) as $snake) {
            $camel       = lcfirst(str_replace('_', '', ucwords($snake, '_')));
            $map[$camel] = $snake;
        }

        return $map;
    }

    /**
     * Resolves a camelCase or snake_case key to the corresponding DB column name.
     *
     * When the key already matches a snake_case column, it is returned as-is.
     * Otherwise the camelCase → snake_case map is consulted.
     *
     * @param string $key camelCase (e.g. "userId") or snake_case (e.g. "user_id").
     *
     * @return string snake_case column name.
     */
    protected static function toDbKey(string $key): string
    {
        if (array_key_exists($key, static::$_columns)) {
            return $key;
        }

        return static::columnMap()[$key] ?? $key;
    }

    /**
     * Maps a snake_case DB row to camelCase attribute keys.
     *
     * @param array<string, mixed> $row Raw row from ClickHouse.
     *
     * @return array<string, mixed> Hydrated camelCase attribute map.
     */
    protected static function mapRowToCamel(array $row): array
    {
        $reverseMap = array_flip(static::columnMap());
        $result     = [];

        foreach ($row as $dbKey => $value) {
            $result[$reverseMap[$dbKey] ?? $dbKey] = $value;
        }

        return $result;
    }

    // ─────────────────────────────────────────────
    // Internal
    // ─────────────────────────────────────────────

    /**
     * Returns the shared {@see ClickHouseClient} instance.
     *
     * @return ClickHouseClient
     */
    public function getClient(): ClickHouseClient
    {
        return ClickHouseClientService::getInstance();
    }

    /**
     * Returns the shared {@see ClickHouseClient} in a static context.
     *
     * @return ClickHouseClient
     */
    protected static function resolveClient(): ClickHouseClient
    {
        return ClickHouseClientService::getInstance();
    }

    /**
     * Returns the SELECT column list for this model.
     *
     * Uses the keys of $_columns when defined, otherwise falls back to ["*"].
     *
     * @return string[]
     */
    protected static function getSelectColumns(): array
    {
        return ! empty(static::$_columns) ? array_keys(static::$_columns) : ['*'];
    }

    /**
     * Returns the ClickHouse type for the given column name.
     *
     * Falls back to "String" when the column is not in $_columns.
     *
     * @param string $column Column name.
     *
     * @return string ClickHouse type string.
     */
    protected static function getColumnType(string $column): string
    {
        return static::$_columns[$column] ?? 'String';
    }

    /**
     * Hydrates a {@see ClickHouseCollection} from a {@see ClickHouseResponse}.
     *
     * Returns an empty collection when the response is unsuccessful or contains no data.
     *
     * @param ClickHouseResponse $response
     *
     * @return ClickHouseCollection
     */
    protected static function hydrateCollection(ClickHouseResponse $response): ClickHouseCollection
    {
        if (! $response->isSuccess() || $response->isEmpty()) {
            return new ClickHouseCollection();
        }

        $models = [];
        foreach ($response->getData() as $row) {
            $models[] = new static(static::mapRowToCamel($row));
        }

        return new ClickHouseCollection($models);
    }

    /**
     * Hydrates the first row of a {@see ClickHouseResponse} into a model instance.
     *
     * Returns null when the response is unsuccessful or empty.
     *
     * @param ClickHouseResponse $response
     *
     * @return static|null
     */
    protected static function hydrateFirst(ClickHouseResponse $response): ?static
    {
        if (! $response->isSuccess() || $response->isEmpty()) {
            return null;
        }

        $first = $response->first();

        return $first ? new static(static::mapRowToCamel($first)) : null;
    }

    // ─────────────────────────────────────────────
    // Security
    // ─────────────────────────────────────────────

    /**
     * Guards {@see rawCommand()} against SSRF, LFI, and data-exfiltration vectors.
     *
     * DDL statements (ALTER, DROP, CREATE, SYSTEM, etc.) are permitted.
     * Network and file-system access patterns are always blocked.
     * The pattern list is intentionally kept in sync with
     * {@see ClickHouseClient::assertCommandSafe()}.
     *
     * @param string $query Command to inspect.
     *
     * @throws ClickHouseException When a dangerous pattern is detected.
     */
    private static function assertCommandSafe(string $query): void
    {
        $patterns = [
            '/;/',
            '/--/',
            '/\/\*/',
            '/\burl\s*\(/i',
            '/\bfile\s*\(/i',
            '/\bremoteSecure?\s*\(/i',
            '/\bmysql\s*\(/i',
            '/\bpostgresql\s*\(/i',
            '/\bjdbc\s*\(/i',
            '/\bodbc\s*\(/i',
            '/\bINTO\s+OUTFILE\b/i',
            '/\bINTO\s+DUMPFILE\b/i',
            '/\bclusterAllReplicas\s*\(/i',
            '/\bdictGet\s*\(/i',
            '/\bFORMAT\s+[A-Z]/i',
            '/\bs3Cluster\s*\(/i',
            '/\bs3\s*\(/i',
            '/\bhdfs\s*\(/i',
            '/\binput\s*\(/i',
        ];

        foreach ($patterns as $pattern) {
            if (preg_match($pattern, $query)) {
                throw new ClickHouseException(
                    'rawCommand: dangerous SQL pattern detected. This command is blocked by the security policy.'
                );
            }
        }
    }
}


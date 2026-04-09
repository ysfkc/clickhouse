<?php

declare(strict_types=1);

/*
 * This file is part of the zotlo/clickhouse package.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Ysfkc\ClickHouse;

use Ysfkc\ClickHouse\Exception\ClickHouseException;

/**
 * Fluent query builder for ClickHouse with built-in SQL injection protection.
 *
 * All user-supplied values are bound via ClickHouse's native parameterized query
 * mechanism ({name:Type} placeholders), so they are never interpolated into the
 * SQL string. Identifiers, operators, and types are validated against strict
 * allowlists before use.
 *
 * Basic usage:
 *
 *   $response = QueryBuilder::table('events')
 *       ->select(['user_id', 'event_name', 'COUNT() as cnt'])
 *       ->where('user_id', '=', 123, 'Int32')
 *       ->where('event_date', '>=', '2026-01-01', 'Date')
 *       ->groupBy(['user_id', 'event_name'])
 *       ->orderBy('cnt', 'DESC')
 *       ->limit(100)
 *       ->get();
 */
class QueryBuilder
{
    // ─────────────────────────────────────────────
    // Security Constants
    // ─────────────────────────────────────────────

    /** @var string[] Allowed SQL comparison operators. */
    private const ALLOWED_OPERATORS = [
        '=', '!=', '<>', '>', '<', '>=', '<=',
        'LIKE', 'NOT LIKE', 'ILIKE', 'NOT ILIKE',
        'IN', 'NOT IN',
    ];

    /** @var string[] Allowed JOIN types. */
    private const ALLOWED_JOIN_TYPES = [
        'INNER', 'LEFT', 'RIGHT', 'FULL', 'CROSS',
        'ANY', 'ALL', 'ASOF',
        'LEFT ANY', 'LEFT ALL', 'LEFT ASOF',
        'RIGHT ANY', 'RIGHT ALL',
        'INNER ANY', 'INNER ALL',
        'FULL ANY', 'FULL ALL',
    ];

    /** @var string[] Allowed ORDER BY directions. */
    private const ALLOWED_DIRECTIONS = ['ASC', 'DESC'];

    /**
     * Regex patterns used to detect dangerous SQL constructs in raw expressions.
     *
     * Note: \bSELECT\b is intentionally absent so that raw() / rawQuery() can accept
     * top-level SELECT statements. Subquery injection is caught separately via
     * the \(\s*SELECT\b pattern (parenthesised SELECT).
     *
     * @var string[]
     */
    private const INJECTION_PATTERNS = [
        '/;/',
        '/--/',
        '/\/\*/',
        '/\bUNION\b/i',
        '/\(\s*SELECT\b/i',
        '/\bEXISTS\s*\(/i',
        '/\bDROP\b/i',
        '/\bALTER\b/i',
        '/\bCREATE\b/i',
        '/\bTRUNCATE\b/i',
        '/\bINSERT\s+INTO\b/i',
        '/\bDELETE\s+FROM\b/i',
        '/\bUPDATE\s+\S+\s+SET\b/i',
        '/\bEXEC(UTE)?\s*\(/i',
        '/\bINTO\s+OUTFILE\b/i',
        '/\bINTO\s+DUMPFILE\b/i',
        '/\bLOAD\s+DATA\b/i',
        '/\bSLEEP\s*\(/i',
        '/\bBENCHMARK\s*\(/i',
        '/\bSYSTEM\b/i',
        '/\bATTACH\b/i',
        '/\bDETACH\b/i',
        '/\bRENAME\s+TABLE\b/i',
        '/\bKILL\s+(QUERY|MUTATION)\b/i',
        '/\bGRANT\b/i',
        '/\bREVOKE\b/i',
        '/\bOPTIMIZE\s+TABLE\b/i',
        '/\bSHOW\s+(TABLES|DATABASES|CREATE)\b/i',
        '/\bDESCRIBE\b/i',
        '/\burl\s*\(/i',
        '/\bfile\s*\(/i',
        '/\bremoteSecure?\s*\(/i',
        '/\bmysql\s*\(/i',
        '/\bpostgresql\s*\(/i',
        '/\bjdbc\s*\(/i',
        '/\bodbc\s*\(/i',
        '/\bclusterAllReplicas\s*\(/i',
        '/\bdictGet\s*\(/i',
        '/\bneighbor\s*\(/i',
        '/\bFORMAT\s+[A-Z]/i',
        '/\bs3Cluster\s*\(/i',
        '/\bs3\s*\(/i',
        '/\bhdfs\s*\(/i',
        '/\binput\s*\(/i',
    ];

    // ─────────────────────────────────────────────
    // Properties
    // ─────────────────────────────────────────────

    /** @var string Target table name. */
    private string $table;

    /** @var string[] SELECT column list. */
    private array $columns = ['*'];

    /** @var string[] Compiled WHERE clause fragments. */
    private array $wheres = [];

    /** @var array<string, mixed> Bound parameter values. */
    private array $params = [];

    /** @var string[] GROUP BY columns. */
    private array $groupByColumns = [];

    /** @var string[] ORDER BY fragments. */
    private array $orderByColumns = [];

    /** @var string[] HAVING clause fragments. */
    private array $havingConditions = [];

    /** @var int|null LIMIT value. */
    private ?int $limitValue = null;

    /** @var int|null OFFSET value. */
    private ?int $offsetValue = null;

    /** @var int Auto-incrementing index used to generate unique parameter names. */
    private int $paramIndex = 0;

    /** @var string|null When set, bypasses the fluent builder and executes this raw SQL directly. */
    private ?string $rawQuery = null;

    /** @var string[] Compiled JOIN fragments. */
    private array $joins = [];

    /** @var ClickHouseClient|null Explicitly injected client (optional). */
    private ?ClickHouseClient $client = null;

    /**
     * @param string $table Target table. May be empty when using raw().
     *
     * @throws ClickHouseException When an invalid table name is provided.
     */
    private function __construct(string $table)
    {
        if ($table !== '') {
            self::validateIdentifier($table, 'table name');
        }

        $this->table = $table;
    }

    // ─────────────────────────────────────────────
    // Static Entry Points
    // ─────────────────────────────────────────────

    /**
     * Creates a new QueryBuilder targeting the given table.
     *
     * @param string $table Table name (e.g. "events" or "db.events").
     *
     * @throws ClickHouseException When the table name contains invalid characters.
     *
     * @return self
     */
    public static function table(string $table): self
    {
        return new self($table);
    }

    /**
     * Creates a QueryBuilder that executes the given raw parameterized SQL.
     *
     * The query string is scanned for injection patterns before being stored.
     * All dynamic values must be passed via $params using {name:Type} placeholders.
     *
     * @param string               $query  Parameterized SQL (e.g. "SELECT … WHERE user_id = {id:Int32}").
     * @param array<string, mixed> $params Bound parameter values.
     *
     * @throws ClickHouseException When a dangerous SQL pattern is detected.
     *
     * @return self
     */
    public static function raw(string $query, array $params = []): self
    {
        self::assertNoSqlInjection($query, 'raw query');

        $instance           = new self('');
        $instance->rawQuery = $query;
        $instance->params   = $params;

        return $instance;
    }

    // ─────────────────────────────────────────────
    // Builder Methods
    // ─────────────────────────────────────────────

    /**
     * Sets the client to use when executing the query.
     *
     * Optional — when omitted, the client is resolved via {@see ClickHouseClientService::getInstance()}.
     *
     * @param ClickHouseClient $client
     *
     * @return $this
     */
    public function setClient(ClickHouseClient $client): self
    {
        $this->client = $client;

        return $this;
    }

    /**
     * Specifies the columns to SELECT.
     *
     * Accepts plain column names, aggregate expressions (COUNT, SUM, AVG, etc.),
     * and aliases. Each expression is validated against the injection pattern list.
     * Pass ['*'] or omit this call to select all columns.
     *
     * @param string[] $columns List of column expressions.
     *
     * @throws ClickHouseException When a dangerous expression is detected.
     *
     * @return $this
     */
    public function select(array $columns): self
    {
        foreach ($columns as $column) {
            if ($column === '*') {
                continue;
            }

            self::validateSelectExpression($column);
        }

        $this->columns = $columns;

        return $this;
    }

    /**
     * Adds a parameterized WHERE condition.
     *
     * The value is bound via a generated {name:Type} placeholder and never
     * interpolated into the SQL string, making injection impossible.
     *
     * @param string $column   Column identifier (e.g. "user_id", "db.table.col").
     * @param string $operator Comparison operator. Must be one of: {@see ALLOWED_OPERATORS}.
     * @param mixed  $value    Value to bind. Arrays are required for IN / NOT IN.
     * @param string $type     ClickHouse data type for the placeholder (e.g. "Int32", "String", "Date").
     *
     * @throws ClickHouseException When the column name, operator, or type is invalid.
     *
     * @return $this
     */
    public function where(string $column, string $operator, mixed $value, string $type = 'String'): self
    {
        self::validateIdentifier($column, 'WHERE column');
        self::validateType($type);

        $upperOperator = strtoupper(trim($operator));

        if (! \in_array($upperOperator, self::ALLOWED_OPERATORS, true)) {
            throw new ClickHouseException(
                sprintf(
                    'Invalid SQL operator: "%s". Allowed operators: %s',
                    $operator,
                    implode(', ', self::ALLOWED_OPERATORS)
                )
            );
        }

        $paramName = $this->generateParamName($column);

        if (\in_array($upperOperator, ['IN', 'NOT IN'], true)) {
            $this->wheres[] = $column . ' ' . $upperOperator . ' {' . $paramName . ':Array(' . $type . ')}';
        } else {
            $this->wheres[] = $column . ' ' . $upperOperator . ' {' . $paramName . ':' . $type . '}';
        }

        $this->params[$paramName] = $value;

        return $this;
    }

    /**
     * Adds a WHERE … BETWEEN … AND … condition.
     *
     * Both boundary values are bound as typed parameters.
     *
     * @param string $column Column identifier.
     * @param mixed  $from   Lower bound (inclusive).
     * @param mixed  $to     Upper bound (inclusive).
     * @param string $type   ClickHouse data type for both bounds (e.g. "Date", "DateTime", "Int32").
     *
     * @throws ClickHouseException When the column name or type is invalid.
     *
     * @return $this
     */
    public function whereBetween(string $column, mixed $from, mixed $to, string $type = 'String'): self
    {
        self::validateIdentifier($column, 'WHERE BETWEEN column');
        self::validateType($type);

        $paramFrom = $this->generateParamName($column . '_from');
        $paramTo   = $this->generateParamName($column . '_to');

        $this->wheres[]           = $column . ' BETWEEN {' . $paramFrom . ':' . $type . '} AND {' . $paramTo . ':' . $type . '}';
        $this->params[$paramFrom] = $from;
        $this->params[$paramTo]   = $to;

        return $this;
    }

    /**
     * Adds a WHERE column IS NULL condition.
     *
     * @param string $column Column identifier.
     *
     * @throws ClickHouseException When the column name is invalid.
     *
     * @return $this
     */
    public function whereNull(string $column): self
    {
        self::validateIdentifier($column, 'WHERE NULL column');

        $this->wheres[] = $column . ' IS NULL';

        return $this;
    }

    /**
     * Adds a WHERE column IS NOT NULL condition.
     *
     * @param string $column Column identifier.
     *
     * @throws ClickHouseException When the column name is invalid.
     *
     * @return $this
     */
    public function whereNotNull(string $column): self
    {
        self::validateIdentifier($column, 'WHERE NOT NULL column');

        $this->wheres[] = $column . ' IS NOT NULL';

        return $this;
    }

    /**
     * Adds a raw WHERE condition string.
     *
     * ⚠️ The condition is scanned against the injection pattern list, but this
     * is a blacklist-based defence. Prefer the structured where() and whereBetween()
     * methods whenever possible. Never embed user-supplied values directly in
     * $rawCondition — pass them through QueryBuilder::raw() with explicit placeholders.
     *
     * @param string $rawCondition Raw SQL condition (e.g. "toYear(create_date) = toYear(today())").
     *
     * @throws ClickHouseException When a dangerous SQL pattern is detected.
     *
     * @return $this
     */
    public function whereRaw(string $rawCondition): self
    {
        self::assertNoSqlInjection($rawCondition, 'whereRaw condition');

        $this->wheres[] = $rawCondition;

        return $this;
    }

    /**
     * Adds a JOIN clause.
     *
     * The table name and join type are validated against strict allowlists.
     * The join condition is scanned for injection patterns.
     *
     * @param string $table     Table to join (e.g. "sessions" or "db.sessions").
     * @param string $condition ON condition (e.g. "events.session_id = sessions.id").
     * @param string $type      Join type. Must be one of: {@see ALLOWED_JOIN_TYPES}.
     *
     * @throws ClickHouseException When the table name, join type, or condition is invalid.
     *
     * @return $this
     */
    public function join(string $table, string $condition, string $type = 'INNER'): self
    {
        self::validateIdentifier($table, 'JOIN table name');
        self::assertNoSqlInjection($condition, 'JOIN condition');

        $upperType = strtoupper(trim($type));

        if (! \in_array($upperType, self::ALLOWED_JOIN_TYPES, true)) {
            throw new ClickHouseException(
                sprintf(
                    'Invalid JOIN type: "%s". Allowed types: %s',
                    $type,
                    implode(', ', self::ALLOWED_JOIN_TYPES)
                )
            );
        }

        $this->joins[] = $upperType . ' JOIN ' . $table . ' ON ' . $condition;

        return $this;
    }

    /**
     * Shorthand for a LEFT JOIN.
     *
     * Equivalent to join($table, $condition, 'LEFT').
     *
     * @param string $table     Table to join.
     * @param string $condition ON condition.
     *
     * @throws ClickHouseException When the table name or condition is invalid.
     *
     * @return $this
     */
    public function leftJoin(string $table, string $condition): self
    {
        return $this->join($table, $condition, 'LEFT');
    }

    /**
     * Sets the GROUP BY columns.
     *
     * Each column name is validated as a safe SQL identifier.
     *
     * @param string[] $columns Column names to group by.
     *
     * @throws ClickHouseException When any column name is invalid.
     *
     * @return $this
     */
    public function groupBy(array $columns): self
    {
        foreach ($columns as $column) {
            self::validateIdentifier($column, 'GROUP BY column');
        }

        $this->groupByColumns = $columns;

        return $this;
    }

    /**
     * Adds a HAVING condition.
     *
     * When $value is provided, a typed parameter placeholder is appended to $condition
     * automatically (e.g. "cnt >" → "cnt > {having_0:Int64}").
     * When $value is omitted, $condition is used as-is after injection scanning.
     *
     * @param string $condition HAVING expression or the left-hand side of a comparison
     *                          (e.g. "cnt >", "SUM(amount) >", or "cnt > 0").
     * @param mixed  $value     Optional right-hand side value to bind as a parameter.
     * @param string $type      ClickHouse type for the bound value (default: "Int64").
     *
     * @throws ClickHouseException When a dangerous pattern is detected, the type is invalid,
     *                             or $condition already contains a placeholder while $value is set.
     *
     * @return $this
     */
    public function having(string $condition, mixed $value = null, string $type = 'Int64'): self
    {
        self::assertNoSqlInjection($condition, 'HAVING condition');

        if (null !== $value) {
            self::validateType($type);

            $paramName = $this->generateParamName('having');

            if (preg_match('/\{[a-zA-Z_][a-zA-Z0-9_]*:[a-zA-Z][a-zA-Z0-9_(), ]*}/', $condition)) {
                throw new ClickHouseException(
                    'having(): $condition already contains a placeholder. ' .
                    'When passing a value via $value, do not include a placeholder in $condition ' .
                    '(e.g. use "cnt >" instead of "cnt > {p:Int64}").'
                );
            }

            $this->havingConditions[]   = $condition . ' {' . $paramName . ':' . $type . '}';
            $this->params[$paramName]   = $value;
        } else {
            $this->havingConditions[] = $condition;
        }

        return $this;
    }

    /**
     * Adds an ORDER BY column.
     *
     * Can be called multiple times to sort by multiple columns.
     *
     * @param string $column    Column to sort by.
     * @param string $direction Sort direction — "ASC" or "DESC".
     *
     * @throws ClickHouseException When the column name or direction is invalid.
     *
     * @return $this
     */
    public function orderBy(string $column, string $direction = 'ASC'): self
    {
        self::validateIdentifier($column, 'ORDER BY column');

        $upperDirection = strtoupper(trim($direction));

        if (! \in_array($upperDirection, self::ALLOWED_DIRECTIONS, true)) {
            throw new ClickHouseException(
                sprintf('Invalid sort direction: "%s". Allowed values: ASC, DESC', $direction)
            );
        }

        $this->orderByColumns[] = $column . ' ' . $upperDirection;

        return $this;
    }

    /**
     * Sets the maximum number of rows to return.
     *
     * @param int $limit Must be a positive integer.
     *
     * @return $this
     */
    public function limit(int $limit): self
    {
        $this->limitValue = $limit;

        return $this;
    }

    /**
     * Sets the number of rows to skip before returning results.
     *
     * Typically used together with limit() for pagination.
     *
     * @param int $offset
     *
     * @return $this
     */
    public function offset(int $offset): self
    {
        $this->offsetValue = $offset;

        return $this;
    }

    // ─────────────────────────────────────────────
    // Execution Methods
    // ─────────────────────────────────────────────

    /**
     * Builds and executes the SELECT query, returning the full result set.
     *
     * @param ClickHouseClient|null $client Optional client override. When omitted, the client is
     *                                      resolved via {@see ClickHouseClientService::getInstance()}.
     *
     * @throws ClickHouseException On query build failure or HTTP error.
     *
     * @return ClickHouseResponse
     */
    public function get(?ClickHouseClient $client = null): ClickHouseResponse
    {
        $resolvedClient = $this->resolveClient($client);

        if (null !== $this->rawQuery) {
            return $resolvedClient->select($this->rawQuery, $this->params);
        }

        return $resolvedClient->select($this->buildSelectQuery(), $this->params);
    }

    /**
     * Executes the query with LIMIT 1 and returns the first row, or null when empty.
     *
     * @param ClickHouseClient|null $client Optional client override.
     *
     * @throws ClickHouseException On query build failure or HTTP error.
     *
     * @return array<string, mixed>|null
     */
    public function first(?ClickHouseClient $client = null): ?array
    {
        $this->limitValue = 1;

        return $this->get($client)->first();
    }

    /**
     * Executes a COUNT query and returns the integer result.
     *
     * Temporarily replaces the SELECT column list with COUNT($column) as cnt,
     * then restores the original list after execution.
     *
     * @param string                $column Column to count. Defaults to "*" (all rows).
     * @param ClickHouseClient|null $client Optional client override.
     *
     * @throws ClickHouseException On invalid column name, query build failure, or HTTP error.
     *
     * @return int
     */
    public function count(string $column = '*', ?ClickHouseClient $client = null): int
    {
        if ($column !== '*') {
            self::validateIdentifier($column, 'COUNT column');
        }

        $originalColumns  = $this->columns;
        $this->columns    = ['COUNT(' . $column . ') as cnt'];

        $response         = $this->get($client);
        $this->columns    = $originalColumns;

        $first = $response->first();

        return $first ? (int) $first['cnt'] : 0;
    }

    /**
     * Executes a single-row INSERT statement.
     *
     * Each entry in $data must specify the value and its ClickHouse type:
     *
     *   $qb->insertData([
     *       'user_id'    => ['value' => 123,     'type' => 'Int32'],
     *       'event_name' => ['value' => 'click', 'type' => 'String'],
     *   ]);
     *
     * @param array<string, array{value: mixed, type: string}> $data  Column definitions.
     * @param ClickHouseClient|null                            $client Optional client override.
     *
     * @throws ClickHouseException On invalid column/type or HTTP error.
     *
     * @return ClickHouseResponse
     */
    public function insertData(array $data, ?ClickHouseClient $client = null): ClickHouseResponse
    {
        $resolvedClient    = $this->resolveClient($client);
        $columns           = [];
        $valuePlaceholders = [];
        $params            = [];

        foreach ($data as $column => $definition) {
            self::validateIdentifier($column, 'INSERT column');

            $type = $definition['type'] ?? 'String';
            self::validateType($type);

            $columns[]   = $column;
            $paramName   = $this->generateParamName($column);
            $valuePlaceholders[] = '{' . $paramName . ':' . $type . '}';
            $params[$paramName]  = $definition['value'];
        }

        $query = 'INSERT INTO ' . $this->table
            . ' (' . implode(', ', $columns) . ')'
            . ' VALUES (' . implode(', ', $valuePlaceholders) . ')';

        return $resolvedClient->insert($query, $params);
    }

    /**
     * Executes a multi-row INSERT statement.
     *
     * @param string[]              $columns Column names (e.g. ['user_id', 'event_name']).
     * @param array<int, array>     $rows    Rows to insert, each an indexed array matching $columns.
     * @param string[]              $types   ClickHouse types per column, in the same order as $columns.
     * @param ClickHouseClient|null $client  Optional client override.
     *
     * @throws ClickHouseException On invalid column/type or HTTP error.
     *
     * @return ClickHouseResponse
     */
    public function insertBatch(array $columns, array $rows, array $types, ?ClickHouseClient $client = null): ClickHouseResponse
    {
        $resolvedClient = $this->resolveClient($client);

        foreach ($columns as $colIndex => $column) {
            self::validateIdentifier($column, 'INSERT BATCH column');
            self::validateType($types[$colIndex] ?? 'String');
        }

        $allValuePlaceholders = [];
        $params               = [];

        foreach ($rows as $rowIndex => $row) {
            $rowPlaceholders = [];
            foreach ($columns as $colIndex => $column) {
                $paramName             = 'r' . $rowIndex . '_' . $column;
                $type                  = $types[$colIndex] ?? 'String';
                $rowPlaceholders[]     = '{' . $paramName . ':' . $type . '}';
                $params[$paramName]    = $row[$colIndex];
            }
            $allValuePlaceholders[] = '(' . implode(', ', $rowPlaceholders) . ')';
        }

        $query = 'INSERT INTO ' . $this->table
            . ' (' . implode(', ', $columns) . ')'
            . ' VALUES ' . implode(', ', $allValuePlaceholders);

        return $resolvedClient->insert($query, $params);
    }

    // ─────────────────────────────────────────────
    // Debug
    // ─────────────────────────────────────────────

    /**
     * Returns the compiled SQL query and bound parameters without executing the query.
     *
     * Useful for debugging and testing.
     *
     * @return array{query: string, params: array<string, mixed>}
     */
    public function toSql(): array
    {
        if (null !== $this->rawQuery) {
            return ['query' => $this->rawQuery, 'params' => $this->params];
        }

        return ['query' => $this->buildSelectQuery(), 'params' => $this->params];
    }

    // ─────────────────────────────────────────────
    // Internal Helpers
    // ─────────────────────────────────────────────

    /**
     * Resolves the ClickHouseClient instance to use for the current query.
     *
     * Resolution order:
     *   1. The $client argument passed directly to the execution method.
     *   2. The client injected via {@see setClient()}.
     *   3. The singleton from {@see ClickHouseClientService::getInstance()}.
     *
     * @param ClickHouseClient|null $client Directly provided client, or null.
     *
     * @throws ClickHouseException When no client can be resolved.
     *
     * @return ClickHouseClient
     */
    private function resolveClient(?ClickHouseClient $client = null): ClickHouseClient
    {
        if ($client !== null) {
            return $client;
        }

        if ($this->client !== null) {
            return $this->client;
        }

        return ClickHouseClientService::getInstance();
    }

    /**
     * Assembles the SELECT SQL statement from the current builder state.
     *
     * @return string
     */
    private function buildSelectQuery(): string
    {
        $query = 'SELECT ' . implode(', ', $this->columns);
        $query .= ' FROM ' . $this->table;

        if (! empty($this->joins)) {
            $query .= ' ' . implode(' ', $this->joins);
        }

        if (! empty($this->wheres)) {
            $query .= ' WHERE ' . implode(' AND ', $this->wheres);
        }

        if (! empty($this->groupByColumns)) {
            $query .= ' GROUP BY ' . implode(', ', $this->groupByColumns);
        }

        if (! empty($this->havingConditions)) {
            $query .= ' HAVING ' . implode(' AND ', $this->havingConditions);
        }

        if (! empty($this->orderByColumns)) {
            $query .= ' ORDER BY ' . implode(', ', $this->orderByColumns);
        }

        if (null !== $this->limitValue) {
            $query .= ' LIMIT ' . $this->limitValue;
        }

        if (null !== $this->offsetValue) {
            $query .= ' OFFSET ' . $this->offsetValue;
        }

        return $query;
    }

    /**
     * Generates a unique parameter name derived from a column identifier.
     *
     * Non-alphanumeric characters (e.g. dots in "db.col") are replaced with underscores,
     * and an auto-incrementing index is appended to prevent name collisions.
     *
     * @param string $column Source identifier.
     *
     * @return string Unique parameter name safe for use in a {name:Type} placeholder.
     */
    private function generateParamName(string $column): string
    {
        $clean = preg_replace('/[^a-zA-Z0-9_]/', '_', $column);

        return $clean . '_' . $this->paramIndex++;
    }

    // ─────────────────────────────────────────────
    // Validation Methods
    // ─────────────────────────────────────────────

    /**
     * Validates a SQL identifier (table or column name) against a safe character allowlist.
     *
     * Accepted format: a letter or underscore, followed by letters/digits/underscores,
     * with an optional single dot separator (e.g. "db.table", "table.column").
     *
     * Valid:   "user_id", "events", "db.events", "col1"
     * Invalid: "events; DROP TABLE events", "1=1 --", "a.b.c"
     *
     * @param string $value   The identifier to validate.
     * @param string $context Human-readable context for the error message (e.g. "table name").
     *
     * @throws ClickHouseException When the identifier contains disallowed characters.
     */
    private static function validateIdentifier(string $value, string $context = 'identifier'): void
    {
        if (! preg_match('/^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?$/', $value)) {
            throw new ClickHouseException(
                sprintf(
                    'Invalid %s: "%s". Identifiers may only contain letters, digits, and underscores, ' .
                    'optionally separated by a single dot (e.g. "db.table").',
                    $context,
                    $value
                )
            );
        }
    }

    /**
     * Validates a SELECT expression.
     *
     * Delegates to {@see assertNoSqlInjection()} to block subqueries and other
     * dangerous constructs while allowing aggregate functions and aliases.
     *
     * Valid:   "user_id", "COUNT() as cnt", "SUM(amount) as total", "toDate(created_at) as dt"
     * Invalid: "(SELECT password FROM users) as leak"
     *
     * @param string $expression The SELECT expression to validate.
     *
     * @throws ClickHouseException When a dangerous pattern is detected.
     */
    private static function validateSelectExpression(string $expression): void
    {
        self::assertNoSqlInjection($expression, 'SELECT expression');
    }

    /**
     * Validates a ClickHouse data type string used in parameterized placeholders.
     *
     * Ensures the type contains only characters that are safe to embed in a
     * {name:Type} placeholder. Prevents type-field injection attacks.
     *
     * Valid:   "String", "Int32", "Array(String)", "Nullable(Int64)", "DateTime64(3)"
     * Invalid: "String} UNION SELECT", "Int32); DROP TABLE --"
     *
     * @param string $type The ClickHouse type string to validate.
     *
     * @throws ClickHouseException When the type contains disallowed characters.
     */
    private static function validateType(string $type): void
    {
        if (! preg_match('/^[a-zA-Z][a-zA-Z0-9_(), ]*$/', $type)) {
            throw new ClickHouseException(
                sprintf(
                    'Invalid ClickHouse type: "%s". Types may only contain letters, digits, ' .
                    'underscores, parentheses, commas, and spaces.',
                    $type
                )
            );
        }

        self::assertNoSqlInjection($type, 'ClickHouse type');
    }

    /**
     * Scans a raw SQL expression for known injection patterns.
     *
     * Raises an exception on the first match. The matched input is deliberately
     * excluded from the exception message to avoid aiding blind-bypass attempts.
     *
     * @param string $input   The SQL string to inspect.
     * @param string $context Human-readable context for the error message.
     *
     * @throws ClickHouseException When a dangerous pattern is matched.
     */
    private static function assertNoSqlInjection(string $input, string $context = 'SQL expression'): void
    {
        foreach (self::INJECTION_PATTERNS as $pattern) {
            if (preg_match($pattern, $input)) {
                throw new ClickHouseException(
                    sprintf('Security violation: %s contains a dangerous SQL pattern.', $context)
                );
            }
        }
    }
}

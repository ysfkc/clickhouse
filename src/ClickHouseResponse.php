<?php

declare(strict_types=1);

/*
 * This file is part of the zotlo/clickhouse package.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Ysfkc\ClickHouse;

/**
 * Immutable value object wrapping a ClickHouse HTTP response.
 *
 * Returned by {@see ClickHouseClient::select()}, {@see ClickHouseClient::insert()},
 * {@see ClickHouseClient::command()}, and all QueryBuilder execution methods.
 *
 * Typical usage:
 *
 *   $response = QueryBuilder::table('events')->where(...)->get();
 *
 *   if ($response->isSuccess()) {
 *       foreach ($response->getData() as $row) { ... }
 *   }
 */
class ClickHouseResponse
{
    /** @var int HTTP status code of the response. */
    private int $httpStatus = 400;

    /** @var bool Whether the request completed successfully (2xx). */
    private bool $success = false;

    /** @var array<int, array<string, string>> Column metadata returned by ClickHouse. */
    private array $meta = [];

    /** @var array<int, array<string, mixed>> Result rows. */
    private array $data = [];

    /** @var array<string, mixed> Query execution statistics (elapsed, rows_read, bytes_read). */
    private array $statistics = [];

    /** @var int Number of rows actually read by the server. */
    private int $rowsRead = 0;

    /** @var int Total rows matching the query before the LIMIT was applied. */
    private int $rowsBeforeLimitAtLeast = 0;

    /**
     * The raw query string, stored for debug purposes.
     * Parameter values are kept separately and never mixed into this string.
     *
     * @var string
     */
    private string $requestQuery = '';

    /**
     * Bound parameter values.
     *
     * Exposed only in development environments to prevent accidental leakage of PII
     * (e.g. IP addresses, user agents) in production logs or API responses.
     *
     * @var array<string, mixed>
     */
    private array $requestParams = [];

    /**
     * Returns the HTTP status code of the response.
     *
     * @return int
     */
    public function getHttpStatus(): int
    {
        return $this->httpStatus;
    }

    /**
     * Sets the HTTP status code.
     *
     * @param int $httpStatus
     *
     * @return $this
     */
    public function setHttpStatus(int $httpStatus): self
    {
        $this->httpStatus = $httpStatus;

        return $this;
    }

    /**
     * Returns true when the request completed with a 2xx status code.
     *
     * @return bool
     */
    public function isSuccess(): bool
    {
        return $this->success;
    }

    /**
     * Marks the response as successful or failed.
     *
     * @param bool $success
     *
     * @return $this
     */
    public function setSuccess(bool $success): self
    {
        $this->success = $success;

        return $this;
    }

    /**
     * Returns the column metadata array returned by ClickHouse (name + type per column).
     *
     * @return array<int, array<string, string>>
     */
    public function getMeta(): array
    {
        return $this->meta;
    }

    /**
     * Sets the column metadata.
     *
     * @param array<int, array<string, string>> $meta
     *
     * @return $this
     */
    public function setMeta(array $meta): self
    {
        $this->meta = $meta;

        return $this;
    }

    /**
     * Returns all result rows as an array of associative arrays.
     *
     * @return array<int, array<string, mixed>>
     */
    public function getData(): array
    {
        return $this->data;
    }

    /**
     * Sets the result rows.
     *
     * @param array<int, array<string, mixed>> $data
     *
     * @return $this
     */
    public function setData(array $data): self
    {
        $this->data = $data;

        return $this;
    }

    /**
     * Returns the query execution statistics provided by ClickHouse
     * (elapsed time, rows read, bytes read, etc.).
     *
     * @return array<string, mixed>
     */
    public function getStatistics(): array
    {
        return $this->statistics;
    }

    /**
     * Sets the query execution statistics.
     *
     * @param array<string, mixed> $statistics
     *
     * @return $this
     */
    public function setStatistics(array $statistics): self
    {
        $this->statistics = $statistics;

        return $this;
    }

    /**
     * Returns the number of rows read by the ClickHouse server for this query.
     *
     * @return int
     */
    public function getRowsRead(): int
    {
        return $this->rowsRead;
    }

    /**
     * Sets the rows-read counter.
     *
     * @param int $rowsRead
     *
     * @return $this
     */
    public function setRowsRead(int $rowsRead): self
    {
        $this->rowsRead = $rowsRead;

        return $this;
    }

    /**
     * Returns the total number of matching rows before the LIMIT clause was applied.
     *
     * Useful for pagination — corresponds to ClickHouse's rows_before_limit_at_least field.
     *
     * @return int
     */
    public function getRowsBeforeLimitAtLeast(): int
    {
        return $this->rowsBeforeLimitAtLeast;
    }

    /**
     * Sets the pre-limit row count.
     *
     * @param int $rowsBeforeLimitAtLeast
     *
     * @return $this
     */
    public function setRowsBeforeLimitAtLeast(int $rowsBeforeLimitAtLeast): self
    {
        $this->rowsBeforeLimitAtLeast = $rowsBeforeLimitAtLeast;

        return $this;
    }

    /**
     * Returns debug information about the originating query.
     *
     * ⚠️ Security: In production environments only the query template is returned.
     * Bound parameter values are omitted to prevent accidental exposure of PII.
     * In development environments (APPLICATION_ENV=development) both are included.
     *
     * @return array{query: string, params?: array<string, mixed>}
     */
    public function getRequestParameters(): array
    {
        if (getenv('APPLICATION_ENV') === 'development') {
            return [
                'query'  => $this->requestQuery,
                'params' => $this->requestParams,
            ];
        }

        return ['query' => $this->requestQuery];
    }

    /**
     * Stores the originating query template and bound parameter values.
     *
     * @param array{query: string, params?: array<string, mixed>} $requestParameters
     *
     * @return $this
     */
    public function setRequestParameters(array $requestParameters): self
    {
        $this->requestQuery  = (string) ($requestParameters['query'] ?? '');
        $this->requestParams = (array) ($requestParameters['params'] ?? []);

        return $this;
    }

    /**
     * Returns the first row of the result set, or null when there are no rows.
     *
     * @return array<string, mixed>|null
     */
    public function first(): ?array
    {
        return $this->data[0] ?? null;
    }

    /**
     * Returns the total number of rows in the result set.
     *
     * @return int
     */
    public function count(): int
    {
        return \count($this->data);
    }

    /**
     * Extracts all values of a single column from the result set.
     *
     * Equivalent to array_column($response->getData(), $column).
     *
     * @param string $column Column name to pluck.
     *
     * @return array<int, mixed>
     */
    public function pluck(string $column): array
    {
        return array_column($this->data, $column);
    }

    /**
     * Returns true when the result set contains no rows.
     *
     * @return bool
     */
    public function isEmpty(): bool
    {
        return empty($this->data);
    }
}


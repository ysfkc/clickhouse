<?php

declare(strict_types=1);

/*
 * This file is part of the ysfkc/clickhouse package.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Ysfkc\ClickHouse;

use GuzzleHttp\Client;
use GuzzleHttp\RequestOptions;
use GuzzleHttp\Exception\RequestException;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Ysfkc\ClickHouse\Exception\ClickHouseException;

/**
 * HTTP transport layer for the ClickHouse HTTP interface.
 *
 * Sends parameterized queries to ClickHouse via POST requests using the
 * native HTTP API (param_* query string binding). A single Guzzle client
 * instance is reused for every request.
 *
 * @see https://clickhouse.com/docs/en/interfaces/http
 */
class ClickHouseClient
{
    /** @var string Base URI of the ClickHouse HTTP endpoint, e.g. "http://clickhouse:8123". */
    private string $baseUri;

    /** @var string Target database name. */
    private string $database;

    /** @var string HTTP Basic Auth username. */
    private string $username;

    /** @var string HTTP Basic Auth password. */
    private string $password;

    /** @var LoggerInterface PSR-3 logger instance. */
    private LoggerInterface $logger;

    /** @var int Request timeout in seconds. */
    private int $timeout;

    /** @var int TCP connect timeout in seconds. */
    private int $connectTimeout;

    /** @var Client Shared Guzzle HTTP client — created once, reused per query. */
    private Client $httpClient;

    /**
     * @param string               $baseUri        Base URL of the ClickHouse HTTP endpoint.
     * @param string               $database       Target database (default: "default").
     * @param string               $username       Basic Auth username.
     * @param string               $password       Basic Auth password.
     * @param int                  $timeout        Request timeout in seconds (default: 30).
     * @param int                  $connectTimeout TCP connection timeout in seconds (default: 10).
     * @param LoggerInterface|null $logger         PSR-3 logger. Falls back to NullLogger when omitted.
     */
    public function __construct(
        string $baseUri,
        string $database = 'default',
        string $username = '',
        string $password = '',
        int $timeout = 30,
        int $connectTimeout = 10,
        ?LoggerInterface $logger = null
    ) {
        $this->baseUri        = rtrim($baseUri, '/');
        $this->database       = $database;
        $this->username       = $username;
        $this->password       = $password;
        $this->timeout        = $timeout;
        $this->connectTimeout = $connectTimeout;
        $this->logger         = $logger ?? new NullLogger();

        $sslVerify = getenv('APPLICATION_ENV') !== 'development';

        if (! $sslVerify) {
            $this->logger->warning(
                'ClickHouseClient: SSL certificate verification is DISABLED (development environment). ' .
                'Ensure this setting is enabled before deploying to production.'
            );
        }

        $this->httpClient = new Client([
            RequestOptions::VERIFY => $sslVerify,
        ]);
    }

    /**
     * Executes a parameterized SELECT query and returns the response.
     *
     * Example:
     *   $client->select(
     *       'SELECT * FROM events WHERE user_id = {userId:Int32} AND event_date >= {startDate:Date}',
     *       ['userId' => 123, 'startDate' => '2026-01-01']
     *   );
     *
     * @param string               $query  ClickHouse parameterized query using {name:Type} placeholders.
     * @param array<string, mixed> $params Key-value pairs bound to the placeholders.
     *
     * @throws ClickHouseException On HTTP failure or unexpected error.
     *
     * @return ClickHouseResponse
     */
    public function select(string $query, array $params = []): ClickHouseResponse
    {
        return $this->executeQuery($query, $params);
    }

    /**
     * Executes a parameterized INSERT statement and returns the response.
     *
     * Example:
     *   $client->insert(
     *       'INSERT INTO events (user_id, event_name, created_at)
     *        VALUES ({userId:Int32}, {eventName:String}, {createdAt:DateTime})',
     *       ['userId' => 123, 'eventName' => 'click', 'createdAt' => '2026-01-01 00:00:00']
     *   );
     *
     * @param string               $query  Parameterized INSERT statement.
     * @param array<string, mixed> $params Bound parameter values.
     *
     * @throws ClickHouseException On HTTP failure or unexpected error.
     *
     * @return ClickHouseResponse
     */
    public function insert(string $query, array $params = []): ClickHouseResponse
    {
        return $this->executeQuery($query, $params, false);
    }

    /**
     * Executes a DDL or administrative command (ALTER, CREATE, DROP, OPTIMIZE, etc.).
     *
     * ⚠️ Security: SSRF and data-exfiltration vectors (url(), file(), remote(), s3(), etc.)
     * are automatically blocked by {@see assertCommandSafe()}.
     * Only hardcoded, static query strings should be passed as $query.
     *
     * @param string               $query  DDL/admin command to execute.
     * @param array<string, mixed> $params Optional bound parameters.
     *
     * @throws ClickHouseException On a blocked pattern, HTTP failure, or unexpected error.
     *
     * @return ClickHouseResponse
     */
    public function command(string $query, array $params = []): ClickHouseResponse
    {
        $this->assertCommandSafe($query);

        return $this->executeQuery($query, $params, false);
    }

    /**
     * Sends the query to the ClickHouse HTTP API and wraps the response.
     *
     * ClickHouse HTTP interface binds parameters via query string: param_<name>=<value>.
     * SELECT responses are requested in JSON format for structured parsing.
     *
     * @param string               $query         SQL query with optional {name:Type} placeholders.
     * @param array<string, mixed> $params        Parameter values indexed by placeholder name.
     * @param bool                 $expectResult  When true, the response body is decoded as JSON.
     *
     * @throws ClickHouseException On unrecoverable HTTP or runtime errors.
     *
     * @return ClickHouseResponse
     */
    private function executeQuery(string $query, array $params = [], bool $expectResult = true): ClickHouseResponse
    {
        try {
            $queryParams = ['database' => $this->database];

            if ($expectResult) {
                $queryParams['default_format'] = 'JSON';
            }

            foreach ($params as $key => $value) {
                $queryParams['param_' . $key] = $this->castParamValue($value);
            }

            $requestOptions = [
                RequestOptions::TIMEOUT         => $this->timeout,
                RequestOptions::CONNECT_TIMEOUT => $this->connectTimeout,
                RequestOptions::HEADERS         => ['Content-Type' => 'text/plain'],
                RequestOptions::QUERY           => $queryParams,
                RequestOptions::BODY            => $query,
            ];

            if (! empty($this->username)) {
                $requestOptions[RequestOptions::AUTH] = [$this->username, $this->password];
            }

            $httpResponse = $this->httpClient->request('POST', $this->baseUri, $requestOptions);

            $responseBody = $httpResponse->getBody()->getContents();
            $httpCode     = $httpResponse->getStatusCode();

            $response = new ClickHouseResponse();
            $response->setHttpStatus($httpCode);
            $response->setRequestParameters(['query' => $query, 'params' => $params]);

            if ($httpCode >= 200 && $httpCode <= 299) {
                $response->setSuccess(true);

                if ($expectResult && ! empty($responseBody)) {
                    $decoded = json_decode($responseBody, true);

                    if (json_last_error() === JSON_ERROR_NONE && isset($decoded['data'])) {
                        $response->setData($decoded['data']);
                        $response->setMeta($decoded['meta'] ?? []);
                        $response->setStatistics($decoded['statistics'] ?? []);
                        $response->setRowsRead($decoded['rows'] ?? 0);
                        $response->setRowsBeforeLimitAtLeast($decoded['rows_before_limit_at_least'] ?? 0);
                    }
                }

                return $response;
            }

            throw new ClickHouseException('ClickHouse: query failed with HTTP ' . $httpCode);
        } catch (RequestException $e) {
            $errorBody = '';

            if ($e->hasResponse()) {
                $rawBody   = $e->getResponse()->getBody()->getContents();
                $errorBody = getenv('APPLICATION_ENV') === 'development'
                    ? $rawBody
                    : (strlen($rawBody) > 200 ? substr($rawBody, 0, 200) . '...[truncated]' : $rawBody);
            }

            $this->logger->error(
                sprintf(
                    'ClickHouseClient RequestException — %s in %s:%d',
                    $e->getMessage(),
                    $e->getFile(),
                    $e->getLine()
                )
            );

            $response = new ClickHouseResponse();
            $response->setHttpStatus($e->hasResponse() ? $e->getResponse()->getStatusCode() : 500);
            $response->setSuccess(false);
            $response->setRequestParameters(['query' => $query, 'params' => $params]);
            $response->setMeta(['error' => $e->getMessage(), 'body' => $errorBody]);

            return $response;
        } catch (ClickHouseException $e) {
            throw $e;
        } catch (\Throwable $e) {
            $this->logger->error(
                sprintf(
                    'ClickHouseClient Exception — %s in %s:%d',
                    $e->getMessage(),
                    $e->getFile(),
                    $e->getLine()
                )
            );

            throw new ClickHouseException('ClickHouse request error: ' . $e->getMessage(), 0, $e);
        }
    }

    /**
     * Casts a PHP value to the string format expected by the ClickHouse HTTP param binding.
     *
     * Handles null (→ \N sentinel), bool, float, array, and scalar types.
     *
     * @param mixed $value The parameter value to cast.
     *
     * @throws ClickHouseException When a non-finite float (NaN / Infinity) is provided.
     *
     * @return string
     */
    private function castParamValue(mixed $value): string
    {
        if (is_null($value)) {
            // ClickHouse NULL sentinel — must be used with a Nullable(T) column type.
            return '\N';
        }

        if (is_bool($value)) {
            return $value ? '1' : '0';
        }

        if (is_float($value)) {
            if (is_nan($value) || is_infinite($value)) {
                throw new ClickHouseException(
                    sprintf(
                        'Invalid float value: %s. ClickHouse does not support NaN or Infinity.',
                        var_export($value, true)
                    )
                );
            }

            return sprintf('%.17G', $value);
        }

        if (is_array($value)) {
            $escaped = array_map(static function ($v): string {
                if (is_null($v)) {
                    return '\N';
                }
                if (is_string($v)) {
                    return "'" . self::escapeClickHouseString($v) . "'";
                }

                return (string) $v;
            }, $value);

            return '[' . implode(',', $escaped) . ']';
        }

        return (string) $value;
    }

    /**
     * Guards command() against SSRF, LFI, and data-exfiltration vectors.
     *
     * Blocks network access functions (url, remote, s3, hdfs, mysql, postgresql, etc.),
     * file-system access (file, INTO OUTFILE), and other dangerous patterns.
     * The pattern list is intentionally kept in sync with QueryBuilder::INJECTION_PATTERNS.
     *
     * @param string $query The command string to inspect.
     *
     * @throws ClickHouseException When a dangerous pattern is detected.
     */
    private function assertCommandSafe(string $query): void
    {
        $patterns = [
            '/;/',                                   // Statement chaining
            '/--/',                                  // Line comment
            '/\/\*/',                                // Block comment
            '/\burl\s*\(/i',                         // SSRF — external URL fetch
            '/\bfile\s*\(/i',                        // LFI — local file read
            '/\bremoteSecure?\s*\(/i',               // External ClickHouse connection
            '/\bmysql\s*\(/i',                       // External MySQL connection
            '/\bpostgresql\s*\(/i',                  // External PostgreSQL connection
            '/\bjdbc\s*\(/i',                        // JDBC connection
            '/\bodbc\s*\(/i',                        // ODBC connection
            '/\bINTO\s+OUTFILE\b/i',                 // Write data to file
            '/\bINTO\s+DUMPFILE\b/i',                // Dump write
            '/\bclusterAllReplicas\s*\(/i',          // Cluster exfiltration
            '/\bdictGet\s*\(/i',                     // Dictionary-based data read
            '/\bFORMAT\s+[A-Z]/i',                   // FORMAT clause injection
            '/\bs3Cluster\s*\(/i',                   // S3 cluster SSRF/exfiltration
            '/\bs3\s*\(/i',                          // S3 storage access
            '/\bhdfs\s*\(/i',                        // HDFS access
            '/\binput\s*\(/i',                       // stdin read
        ];

        foreach ($patterns as $pattern) {
            if (preg_match($pattern, $query)) {
                throw new ClickHouseException(
                    'ClickHouseClient::command(): dangerous SQL pattern detected. ' .
                    'This command is blocked by the security policy.'
                );
            }
        }
    }

    /**
     * Escapes a string value according to ClickHouse string literal rules.
     *
     * Uses ClickHouse-specific escape sequences instead of addslashes(),
     * which does not know ClickHouse's quoting rules.
     *
     * Escape map:
     *   \\  →  \\\\    '  →  \'    \n  →  \\n    \t  →  \\t    \r  →  \\r
     *   \0  →  \\0    \b  →  \\b   \f  →  \\f    \v  →  \\v    \a  →  \\a
     *
     * @param string $value The raw string value to escape.
     *
     * @return string The escaped string, safe for embedding in a ClickHouse string literal.
     */
    private static function escapeClickHouseString(string $value): string
    {
        return str_replace(
            ['\\',    "'",   "\n",   "\t",   "\r",   "\0",   "\x08", "\x0C", "\x0B", "\x07"],
            ['\\\\',  "\'",  '\\n',  '\\t',  '\\r',  '\\0',  '\\b',  '\\f',  '\\v',  '\\a'],
            $value
        );
    }
}


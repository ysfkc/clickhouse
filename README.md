# ysfkc/clickhouse

A lightweight, framework-agnostic ClickHouse HTTP client for PHP 8.1+ with a fluent QueryBuilder and an ORM-style model layer.

[![PHP](https://img.shields.io/badge/PHP-8.1%2B-blue)](https://www.php.net/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

## Table of Contents

- [Architecture](#architecture)
- [Installation](#installation)
- [Configuration](#configuration)
  - [Step 1 — Configure](#step-1--configure-once-at-bootstrap)
  - [Step 2 — getInstance()](#step-2--obtain-the-client-via-getinstance)
  - [Step 3 — Usage](#step-3--use-the-client-directly-or-via-querybuilder--model)
- [QueryBuilder](#querybuilder)
  - [SELECT](#select)
  - [WHERE Conditions](#where-conditions)
  - [JOIN](#join)
  - [GROUP BY / HAVING](#group-by--having)
  - [ORDER BY / LIMIT / OFFSET](#order-by--limit--offset)
  - [INSERT](#insert)
  - [Raw Query — raw()](#raw-query--raw)
- [Model System (ORM)](#model-system-orm)
  - [Defining a Model](#defining-a-model)
  - [Querying Records](#querying-records)
  - [Inserting Records](#inserting-records)
  - [rawQuery / rawCommand](#rawquery--rawcommand)
- [ClickHouseResponse](#clickhouseresponse)
- [ClickHouseCollection](#clickhousecollection)
- [Direct Client Usage](#direct-client-usage)
- [Security](#security)
- [ClickHouse Type Reference](#clickhouse-type-reference)
- [Phalcon Integration](#phalcon-integration-optional)

---

## Architecture

```
ClickHouseClientService   →  Singleton factory — creates and holds the client
ClickHouseClient          →  HTTP layer (Guzzle), sends parameterized queries
QueryBuilder              →  Fluent API — builds safe SQL queries
ClickHouseResponse        →  Value object wrapping the HTTP response
ClickHouseCollection      →  Typed model list (ArrayAccess, Countable, foreach)

ClickHouseBaseModel  ←  AppBaseModel  ←  MetricSnapshot, OrderEvent, …
```

---

## Installation

```bash
composer require ysfkc/clickhouse
```

**Requirements:** PHP 8.1+, `guzzlehttp/guzzle ^7.0`, `psr/log ^1|^2|^3`

---

## Configuration

### Step 1 — Configure once at bootstrap

Call `ClickHouseClientService::configure()` **once** at application startup (service provider, bootstrap file, etc.):

```php
use Ysfkc\ClickHouse\ClickHouseClientService;

ClickHouseClientService::configure([
    'host'           => 'http://clickhouse-server:8123',
    'database'       => 'analytics',
    'username'       => 'default',
    'password'       => 'secret',
    'timeout'        => 30,   // seconds
    'connectTimeout' => 10,   // seconds
]);
```

**With a PSR-3 logger:**

```php
ClickHouseClientService::configure([...], $myPsrLogger);
```

---

### Step 2 — Obtain the client via getInstance()

After `configure()` is called, retrieve the shared `ClickHouseClient` instance from anywhere in your application:

```php
$client = ClickHouseClientService::getInstance();
```

`getInstance()` creates the client **lazily on the first call** and returns the same instance on every subsequent call. Calling `configure()` again invalidates the old instance and forces a fresh one on the next `getInstance()` call.

> ⚠️ Calling `getInstance()` before `configure()` throws a `\RuntimeException`.

---

### Step 3 — Use the client directly or via QueryBuilder / Model

Once configured, you can use any of the three layers — they all resolve the client internally via `getInstance()`:

```php
use Ysfkc\ClickHouse\ClickHouseClientService;
use Ysfkc\ClickHouse\QueryBuilder;

// Direct client
$client   = ClickHouseClientService::getInstance();
$response = $client->select('SELECT 1');

// QueryBuilder — no manual getInstance() needed
$response = QueryBuilder::table('events')
    ->where('user_id', '=', 123, 'Int32')
    ->get();

// ORM Model — no manual getInstance() needed
$snapshots = MetricSnapshot::find(['status' => 'ok']);
```

---

### Optional — Inject a PSR-3 logger for models

```php
use Ysfkc\ClickHouse\Model\ClickHouseBaseModel;

ClickHouseBaseModel::setDefaultLogger($myPsrLogger);
```

---

## QueryBuilder

`QueryBuilder` is started via the static `table()` method. All methods support fluent chaining.

### SELECT

```php
use Ysfkc\ClickHouse\QueryBuilder;

// All columns
$response = QueryBuilder::table('events')->get();

// Specific columns
$response = QueryBuilder::table('events')
    ->select(['user_id', 'event_type', 'created_at'])
    ->get();

// Aggregate functions and aliases
$response = QueryBuilder::table('events')
    ->select([
        'user_id',
        'event_type',
        'COUNT()          as total',
        'uniqExact(uuid)  as unique_users',
        'SUM(duration)    as total_duration',
        'toDate(created_at) as day',
    ])
    ->get();
```

---

### WHERE Conditions

#### Basic Equality

```php
$response = QueryBuilder::table('events')
    ->where('user_id',    '=', 123,     'Int32')
    ->where('event_type', '=', 'click', 'String')
    ->get();
```

#### Comparison Operators

```php
// Allowed: =  !=  <>  >  <  >=  <=  LIKE  NOT LIKE  ILIKE  NOT ILIKE  IN  NOT IN
$response = QueryBuilder::table('events')
    ->where('user_id',      '>',    100,        'Int32')
    ->where('browser',      'LIKE', '%Chrome%', 'String')
    ->where('country_code', '!=',   'XX',       'String')
    ->get();
```

#### IN / NOT IN

```php
$response = QueryBuilder::table('events')
    ->where('event_type', 'IN',     ['click', 'scroll', 'view'], 'String')
    ->where('user_id',    'NOT IN', [0, -1],                     'Int32')
    ->get();
```

#### BETWEEN (Date Range)

```php
$response = QueryBuilder::table('events')
    ->whereBetween('event_date', '2026-01-01', '2026-03-31', 'Date')
    ->get();

// DateTime column
$response = QueryBuilder::table('events')
    ->whereBetween('created_at', '2026-01-01 00:00:00', '2026-01-31 23:59:59', 'DateTime')
    ->get();
```

#### NULL Checks

```php
QueryBuilder::table('events')->whereNull('referrer')->get();
QueryBuilder::table('events')->whereNotNull('utm_source')->get();
```

#### whereRaw — Raw Condition

> ⚠️ Use only when `where()` cannot express the condition.
> Never embed user input directly — use `QueryBuilder::raw()` with parameters instead.

```php
// ✅ Safe — no user input, column expression only
QueryBuilder::table('events')
    ->whereRaw('toYear(event_date) = toYear(today())')
    ->get();

// ✅ Safe — raw + parameterized
QueryBuilder::raw(
    'SELECT * FROM events WHERE toYear(event_date) = {yr:Int32}',
    ['yr' => 2026]
)->get();

// ❌ FORBIDDEN
QueryBuilder::table('events')
    ->whereRaw('user_id = ' . $userInput)
    ->get();
```

---

### JOIN

```php
// INNER JOIN
$response = QueryBuilder::table('orders')
    ->select(['orders.user_id', 'users.name', 'COUNT() as cnt'])
    ->join('users', 'orders.user_id = users.id', 'INNER')
    ->where('orders.user_id', '=', 123, 'Int32')
    ->groupBy(['orders.user_id', 'users.name'])
    ->get();

// LEFT JOIN (shorthand)
$response = QueryBuilder::table('orders')
    ->select(['orders.uuid', 'sessions.started_at'])
    ->leftJoin('sessions', 'orders.session_id = sessions.id')
    ->get();

// ClickHouse ANY JOIN
$response = QueryBuilder::table('orders')
    ->join('sessions', 'orders.session_id = sessions.id', 'LEFT ANY')
    ->get();
```

---

### GROUP BY / HAVING

```php
// GROUP BY
$response = QueryBuilder::table('events')
    ->select(['user_id', 'event_type', 'COUNT() as cnt'])
    ->where('event_date', '>=', '2026-01-01', 'Date')
    ->groupBy(['user_id', 'event_type'])
    ->get();

// HAVING — parameterized (recommended)
$response = QueryBuilder::table('events')
    ->select(['user_id', 'COUNT() as cnt'])
    ->groupBy(['user_id'])
    ->having('cnt >', 100, 'Int64')   // → HAVING cnt > {having_0:Int64}
    ->get();

// HAVING — raw (injection-scanned)
$response = QueryBuilder::table('events')
    ->select(['user_id', 'COUNT() as cnt'])
    ->groupBy(['user_id'])
    ->having('cnt > 0')
    ->get();
```

---

### ORDER BY / LIMIT / OFFSET

```php
$response = QueryBuilder::table('events')
    ->select(['user_id', 'event_date', 'event_type'])
    ->where('user_id', '=', 123, 'Int32')
    ->orderBy('event_date', 'DESC')
    ->orderBy('event_type', 'ASC')
    ->limit(50)
    ->offset(100)
    ->get();
```

#### First Row

```php
$row = QueryBuilder::table('events')
    ->where('user_id', '=', 123, 'Int32')
    ->orderBy('event_date', 'DESC')
    ->first();  // array|null, LIMIT 1 added automatically
```

#### COUNT

```php
$total    = QueryBuilder::table('events')->where('user_id', '=', 123, 'Int32')->count();
$distinct = QueryBuilder::table('events')->where('user_id', '=', 123, 'Int32')->count('uuid');
```

---

### INSERT

#### Single Row

```php
$response = QueryBuilder::table('events')
    ->insertData([
        'uuid'       => ['value' => 'a1b2c3d4-...', 'type' => 'String'],
        'user_id'    => ['value' => 123,             'type' => 'Int32'],
        'event_type' => ['value' => 'click',         'type' => 'String'],
        'event_date' => ['value' => '2026-04-06',    'type' => 'Date'],
    ]);
```

#### Batch INSERT

```php
$columns = ['uuid', 'user_id', 'event_type', 'event_date'];
$types   = ['String', 'Int32', 'String', 'Date'];
$rows    = [
    ['uuid-1', 123, 'click',  '2026-04-06'],
    ['uuid-2', 456, 'scroll', '2026-04-06'],
];

$response = QueryBuilder::table('events')
    ->insertBatch($columns, $rows, $types);
```

---

### Raw Query — raw()

For complex queries that cannot be expressed with the structured methods:

```php
$response = QueryBuilder::raw(
    'SELECT
        toDate(event_date)  AS day,
        COUNT()             AS total,
        uniqExact(uuid)     AS unique_users
     FROM events
     WHERE user_id    = {userId:Int32}
       AND event_date BETWEEN {start:Date} AND {end:Date}
     GROUP BY day
     ORDER BY day ASC',
    ['userId' => 123, 'start' => '2026-01-01', 'end' => '2026-03-31']
)->get();
```

#### Debug — toSql()

```php
$debug = QueryBuilder::table('events')
    ->select(['user_id', 'COUNT() as cnt'])
    ->where('user_id', '=', 123, 'Int32')
    ->groupBy(['user_id'])
    ->toSql();

// [
//   'query'  => 'SELECT user_id, COUNT() as cnt FROM events WHERE user_id = {user_id_0:Int32} GROUP BY user_id',
//   'params' => ['user_id_0' => 123],
// ]
```

---

## Model System (ORM)

### Defining a Model

```php
<?php

use Ysfkc\ClickHouse\Model\ClickHouseBaseModel;

class MetricSnapshot extends ClickHouseBaseModel
{
    protected static string $_tableName = 'metric_snapshots';

    protected static array $_columns = [
        'uuid'        => 'String',
        'source'      => 'LowCardinality(String)',
        'metric_name' => 'String',
        'value'       => 'Float64',
        'status'      => 'LowCardinality(String)',
        'event_date'  => 'Date',
        'recorded_at' => 'DateTime',
    ];

    public function getAverageBySource(string $startDate, string $endDate): array
    {
        return static::query()
            ->select(['source', 'AVG(value) as avg_value', 'COUNT() as cnt'])
            ->whereBetween('event_date', $startDate, $endDate, 'Date')
            ->groupBy(['source'])
            ->orderBy('avg_value', 'DESC')
            ->get()
            ->getData();
    }
}
```

---

### Querying Records

#### all() — All Records

```php
$snapshots = MetricSnapshot::all();       // up to 1 000 rows (default)
$snapshots = MetricSnapshot::all(500);

foreach ($snapshots as $snapshot) {
    echo $snapshot->source;
    echo $snapshot->metricName;
    echo $snapshot->recordedAt;
}
```

#### find() — Conditional Query

```php
// camelCase or snake_case keys are both accepted
$snapshots = MetricSnapshot::find(['source' => 'api']);

$snapshots = MetricSnapshot::find(
    conditions:       ['source' => 'api', 'status' => 'ok'],
    limit:            200,
    orderByColumn:    'recordedAt',
    orderByDirection: 'DESC'
);

// Array value → IN condition
$snapshots = MetricSnapshot::find(['status' => ['ok', 'warning', 'error']]);

$data = $snapshots->toArray();             // camelCase keys
$data = $snapshots->toArray(snake: true);  // snake_case keys
```

#### findFirst() — First Record

```php
$snapshot = MetricSnapshot::findFirst(['source' => 'api', 'status' => 'error']);

if ($snapshot !== null) {
    echo $snapshot->uuid;
    $json = json_encode($snapshot); // logger/DI never leaks
}
```

#### countBy() — Count

```php
$total    = MetricSnapshot::countBy(['source' => 'api']);
$filtered = MetricSnapshot::countBy(['source' => 'api', 'status' => 'error']);
```

#### query() — Advanced Fluent Query

```php
$response = MetricSnapshot::query()
    ->select(['source', 'metric_name', 'AVG(value) as avg_value', 'COUNT() as cnt'])
    ->where('event_date', '>=', '2026-01-01', 'Date')
    ->where('event_date', '<=', '2026-03-31', 'Date')
    ->where('status',     '=',  'ok',         'LowCardinality(String)')
    ->groupBy(['source', 'metric_name'])
    ->orderBy('avg_value', 'DESC')
    ->limit(100)
    ->get();
```

#### queryBetweenDates() — Date Range Shorthand

```php
$response = MetricSnapshot::queryBetweenDates('2026-01-01', '2026-03-31', 'event_date')
    ->select(['source', 'AVG(value) as avg_value'])
    ->where('status', '=', 'ok', 'LowCardinality(String)')
    ->groupBy(['source'])
    ->get();
```

---

### Inserting Records

#### insertRow() — Single Row

```php
$response = MetricSnapshot::insertRow([
    'uuid'        => 'a1b2c3d4-e5f6-...',
    'source'      => 'api',
    'metric_name' => 'response_time',
    'value'       => 142.5,
    'status'      => 'ok',
    'event_date'  => '2026-04-06',
    'recorded_at' => '2026-04-06 12:00:00',
]);
```

#### insertBatch() — Bulk Insert

```php
$columns = ['uuid', 'source', 'metric_name', 'value', 'status', 'event_date'];
$rows    = [
    ['uuid-1', 'api',  'response_time', 142.5, 'ok',      '2026-04-06'],
    ['uuid-2', 'web',  'load_time',     320.0, 'warning', '2026-04-06'],
    ['uuid-3', 'cron', 'job_duration',   58.3, 'ok',      '2026-04-06'],
];

// Types are resolved from $_columns automatically
$response = MetricSnapshot::insertBatch($columns, $rows);
```

---

### rawQuery / rawCommand

#### rawQuery — Custom SELECT

> ⚠️ Pass only hardcoded strings as `$query`. All dynamic values must go through `$params`.

```php
// ✅ Correct
$response = MetricSnapshot::rawQuery(
    'SELECT toDate(recorded_at) AS day, source, AVG(value) AS avg_value
     FROM metric_snapshots
     WHERE status     = {status:String}
       AND event_date BETWEEN {start:Date} AND {end:Date}
     GROUP BY day, source ORDER BY avg_value DESC LIMIT {lim:Int32}',
    ['status' => 'ok', 'start' => '2026-01-01', 'end' => '2026-03-31', 'lim' => 50]
);

// ❌ Incorrect
$response = MetricSnapshot::rawQuery('SELECT * FROM metric_snapshots WHERE source = ' . $userInput);
```

#### rawCommand — DDL Commands

```php
MetricSnapshot::rawCommand('OPTIMIZE TABLE metric_snapshots FINAL');

MetricSnapshot::rawCommand(
    'ALTER TABLE metric_snapshots DROP PARTITION {part:String}',
    ['part' => '202601']
);
```

---

## ClickHouseResponse

```php
$response = MetricSnapshot::query()->where('status', '=', 'ok', 'LowCardinality(String)')->get();

$response->isSuccess();                 // bool
$response->getData();                   // array[] — all rows
$response->first();                     // array|null
$response->isEmpty();                   // bool
$response->count();                     // int
$response->pluck('metric_name');        // ['response_time', 'load_time', ...]
$response->getRowsRead();               // int
$response->getRowsBeforeLimitAtLeast(); // int — total before LIMIT
$response->getStatistics();             // ['elapsed' => ..., ...]
$response->getHttpStatus();             // int
$response->getRequestParameters();     // ['query' => '...', 'params' => [...]]
```

---

## ClickHouseCollection

```php
$snapshots = MetricSnapshot::find(['source' => 'api']);

count($snapshots);                       // int
$snapshots->isEmpty();                   // bool
$snapshots->first();                     // ClickHouseBaseModel|null

foreach ($snapshots as $snapshot) {
    echo $snapshot->metricName;
}

$snapshots[0]->uuid;
$snapshots->toArray();              // camelCase keys
$snapshots->toArray(snake: true);   // snake_case keys
json_encode($snapshots);            // logger/DI never leaks
```

---

## Direct Client Usage

```php
use Ysfkc\ClickHouse\ClickHouseClientService;

$client = ClickHouseClientService::getInstance();

$response = $client->select(
    'SELECT source, AVG(value) as avg FROM metric_snapshots WHERE status = {s:String} GROUP BY source',
    ['s' => 'ok']
);

$response = $client->insert(
    'INSERT INTO metric_snapshots (uuid, source, metric_name, value, event_date) VALUES ({u:String},{s:String},{m:String},{v:Float64},{d:Date})',
    ['u' => 'some-uuid', 's' => 'api', 'm' => 'response_time', 'v' => 142.5, 'd' => '2026-04-06']
);

// SSRF/LFI/exfiltration vectors are automatically blocked
$response = $client->command('OPTIMIZE TABLE metric_snapshots FINAL');
```

---

## Security

### Parameterized Queries

All user-supplied values are sent as `param_*` query string parameters, never interpolated into the SQL body:

```
POST http://clickhouse:8123/?database=analytics&param_userId=123&default_format=JSON
Body: SELECT * FROM events WHERE user_id = {userId:Int32}
```

### Blocked Vectors

| Category | Examples |
|---|---|
| Statement chaining | `;` separator |
| DDL | `DROP`, `ALTER`, `CREATE`, `TRUNCATE` |
| DML (external) | `INSERT INTO`, `DELETE FROM` |
| SSRF | `url()`, `remote()`, `s3()`, `hdfs()` |
| LFI | `file()` |
| Schema discovery | `SHOW TABLES`, `DESCRIBE` |
| Exfiltration | `INTO OUTFILE`, `clusterAllReplicas()`, `dictGet()` |
| Time-based | `sleep()`, `benchmark()` |
| Subquery (whereRaw/having/join) | `(SELECT …)` |

### Rule: whereRaw / having / join Conditions

```php
// ✅ Preferred
->where('user_id', '=', $userId, 'Int32')

// ⚠️ Last resort — column expressions only, no user input
->whereRaw('toYear(event_date) = toYear(today())')
```

---

## ClickHouse Type Reference

| Type | Description | Example |
|---|---|---|
| `String` | UTF-8 text | `'click'` |
| `Int8` / `Int16` / `Int32` / `Int64` | Signed integer | `123` |
| `UInt8` / `UInt16` / `UInt32` / `UInt64` | Unsigned integer | `456` |
| `Float32` / `Float64` | Floating-point | `3.14` |
| `Date` | Date `YYYY-MM-DD` | `'2026-04-06'` |
| `DateTime` | Datetime `YYYY-MM-DD HH:MM:SS` | `'2026-04-06 12:00:00'` |
| `DateTime64(3)` | Millisecond-precision datetime | `'2026-04-06 12:00:00.123'` |
| `Nullable(Int32)` | Nullable Int32 | `null` or `123` |
| `Array(String)` | String array — for `IN` | `['a', 'b']` |
| `Array(Int32)` | Int32 array — for `IN` | `[1, 2, 3]` |
| `LowCardinality(String)` | Low-cardinality string | `'ok'` |

> **Note:** The `Array(…)` wrapper is added automatically for `IN` / `NOT IN`.
> `->where('status', 'IN', ['ok', 'warning', 'error'], 'String')` →
> `status IN {p:Array(String)}`

---

## Phalcon Integration (Optional)

If you use this package inside a Phalcon application, bridge the Phalcon logger to PSR-3
using the included optional adapter:

```php
use Ysfkc\ClickHouse\Logger\PhalconLoggerAdapter;
use Ysfkc\ClickHouse\ClickHouseClientService;
use Ysfkc\ClickHouse\Model\ClickHouseBaseModel;

$adapter = new PhalconLoggerAdapter($di->get('logger'));

ClickHouseClientService::configure(
    $di->getConfig()->clickhouse->toArray(),
    $adapter
);

ClickHouseBaseModel::setDefaultLogger($adapter);
```

---

## License

MIT

<?php

declare(strict_types=1);

/*
 * This file is part of the ysfkc/clickhouse package.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Ysfkc\ClickHouse\Exception;

/**
 * Base exception thrown by the ClickHouse client, QueryBuilder, and model layer.
 *
 * Catch this exception to handle any error originating from this package:
 *
 *   try {
     *       $response = MetricSnapshot::query()->where(...)->get();
 *   } catch (\Ysfkc\ClickHouse\Exception\ClickHouseException $e) {
 *       // query validation error, HTTP failure, etc.
 *   }
 */
class ClickHouseException extends \RuntimeException
{
}

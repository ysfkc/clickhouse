<?php

declare(strict_types=1);

/*
 * This file is part of the ysfkc/clickhouse package.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Ysfkc\ClickHouse\Logger;

use Psr\Log\AbstractLogger;

/**
 * Optional PSR-3 bridge for Phalcon's logger.
 *
 * Phalcon\Logger\Logger does not implement PSR-3's LoggerInterface directly.
 * This adapter wraps a Phalcon logger instance so it can be injected anywhere
 * a PSR-3 LoggerInterface is expected — including {@see \Ysfkc\ClickHouse\Model\ClickHouseBaseModel::setDefaultLogger()}.
 *
 * This class is **optional**. It is only needed when using this package inside a
 * Phalcon application. There is no dependency on phalcon/phalcon in composer.json;
 * the Phalcon classes are referenced by name only and resolved at runtime.
 *
 * Usage:
 *
 *   use Ysfkc\ClickHouse\Logger\PhalconLoggerAdapter;
 *   use Ysfkc\ClickHouse\Model\ClickHouseBaseModel;
 *
 *   ClickHouseBaseModel::setDefaultLogger(
 *       new PhalconLoggerAdapter($di->get('logger'))
 *   );
 *
 * Supported Phalcon log levels (matched by method name):
 *   emergency, alert, critical, error, warning, notice, info, debug
 */
class PhalconLoggerAdapter extends AbstractLogger
{
    /**
     * @var object The inner Phalcon\Logger\Logger (or any object with the same method signatures).
     */
    private object $inner;

    /**
     * @param object $phalconLogger A Phalcon\Logger\Logger instance.
     */
    public function __construct(object $phalconLogger)
    {
        $this->inner = $phalconLogger;
    }

    /**
     * Forwards a PSR-3 log call to the Phalcon logger.
     *
     * Phalcon logger method names match PSR-3 level names exactly, so the level
     * string is used directly as the method name. Falls back to info() when the
     * level method does not exist on the inner logger.
     *
     * @param mixed                $level   PSR-3 log level (e.g. "error", "warning").
     * @param string|\Stringable   $message Log message, may contain {placeholder} tokens.
     * @param array<string, mixed> $context Context values for placeholder interpolation.
     */
    public function log($level, string|\Stringable $message, array $context = []): void
    {
        $interpolated = $this->interpolate((string) $message, $context);

        if (method_exists($this->inner, $level)) {
            $this->inner->{$level}($interpolated);

            return;
        }

        if (method_exists($this->inner, 'info')) {
            $this->inner->info($interpolated);
        }
    }

    /**
     * Replaces {placeholder} tokens in the message with values from $context.
     *
     * Follows the PSR-3 message interpolation specification.
     *
     * @param string               $message Raw log message.
     * @param array<string, mixed> $context Context values indexed by placeholder name.
     *
     * @return string Interpolated message.
     */
    private function interpolate(string $message, array $context): string
    {
        if (empty($context) || strpos($message, '{') === false) {
            return $message;
        }

        $replace = [];
        foreach ($context as $key => $val) {
            if (is_null($val) || is_scalar($val) || (is_object($val) && method_exists($val, '__toString'))) {
                $replace['{' . $key . '}'] = $val;
            }
        }

        return strtr($message, $replace);
    }
}

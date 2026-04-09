<?php

declare(strict_types=1);

/*
 * This file is part of the zotlo/clickhouse package.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Ysfkc\ClickHouse\Model;

/**
 * Typed collection returned by ORM query methods (find(), all(), etc.).
 *
 * Implements ArrayAccess, Countable, IteratorAggregate, and JsonSerializable,
 * so it can be iterated, accessed by index, counted, and JSON-encoded without
 * any extra conversion step.
 *
 * Internal state (loggers, DI containers, etc.) is never serialized or exposed.
 *
 * Usage:
 *
 *   $snapshots = MetricSnapshot::find(['source' => 'api']);
 *
 *   count($events);                      // int
 *   $events->isEmpty();                  // bool
 *   $events->first();                    // ClickHouseBaseModel|null
 *   $events->toArray();                  // camelCase key array
 *   $events->toArray(snake: true);       // snake_case key array
 *   foreach ($events as $event) { ... }
 *   $events[0]->uuid;
 *   json_encode($events);
 */
class ClickHouseCollection implements \ArrayAccess, \Countable, \IteratorAggregate, \JsonSerializable
{
    /** @var ClickHouseBaseModel[] */
    private array $items;

    /**
     * @param ClickHouseBaseModel[] $items Model instances to wrap.
     */
    public function __construct(array $items = [])
    {
        $this->items = array_values($items);
    }

    // ─────────────────────────────────────────────
    // Conversion
    // ─────────────────────────────────────────────

    /**
     * Returns all models as a plain array.
     *
     * Internal state (logger, etc.) is never included.
     *
     * @param bool $snake When true, keys are snake_case DB column names;
     *                    when false (default), keys are camelCase attribute names.
     *
     * @return array<int, array<string, mixed>>
     */
    public function toArray(bool $snake = false): array
    {
        return array_map(
            fn (ClickHouseBaseModel $model) => $snake ? $model->toSnakeArray() : $model->toArray(),
            $this->items
        );
    }

    // ─────────────────────────────────────────────
    // Convenience
    // ─────────────────────────────────────────────

    /**
     * Returns the first model in the collection, or null when the collection is empty.
     *
     * @return ClickHouseBaseModel|null
     */
    public function first(): ?ClickHouseBaseModel
    {
        return $this->items[0] ?? null;
    }

    /**
     * Returns true when the collection contains no items.
     *
     * @return bool
     */
    public function isEmpty(): bool
    {
        return empty($this->items);
    }

    // ─────────────────────────────────────────────
    // ArrayAccess
    // ─────────────────────────────────────────────

    /** {@inheritDoc} */
    public function offsetExists(mixed $offset): bool
    {
        return isset($this->items[$offset]);
    }

    /** {@inheritDoc} */
    public function offsetGet(mixed $offset): mixed
    {
        return $this->items[$offset] ?? null;
    }

    /** {@inheritDoc} */
    public function offsetSet(mixed $offset, mixed $value): void
    {
        if ($offset === null) {
            $this->items[] = $value;
        } else {
            $this->items[$offset] = $value;
        }
    }

    /** {@inheritDoc} */
    public function offsetUnset(mixed $offset): void
    {
        unset($this->items[$offset]);
        $this->items = array_values($this->items);
    }

    // ─────────────────────────────────────────────
    // Countable
    // ─────────────────────────────────────────────

    /**
     * Returns the number of models in the collection.
     *
     * @return int
     */
    public function count(): int
    {
        return count($this->items);
    }

    // ─────────────────────────────────────────────
    // IteratorAggregate
    // ─────────────────────────────────────────────

    /**
     * Returns an iterator over the model instances.
     *
     * @return \ArrayIterator<int, ClickHouseBaseModel>
     */
    public function getIterator(): \ArrayIterator
    {
        return new \ArrayIterator($this->items);
    }

    // ─────────────────────────────────────────────
    // JsonSerializable
    // ─────────────────────────────────────────────

    /**
     * Returns the camelCase array representation for json_encode().
     *
     * Internal state is never included in the output.
     *
     * @return array<int, array<string, mixed>>
     */
    public function jsonSerialize(): array
    {
        return $this->toArray();
    }
}


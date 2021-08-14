<?php

namespace Amp\Postgres;

use Amp\Promise;
use Amp\Sql\FailureException;
use Amp\Success;

final class PgSqlResultSet implements ResultSet
{
    /** @var resource PostgreSQL result resource. */
    private $handle;

    /** @var array<int, array{string, string}> */
    private $types;

    /** @var int */
    private $position = 0;

    /** @var mixed Last row emitted. */
    private $currentRow;

    /** @var int[] */
    private $fieldTypes = [];

    /** @var string[] */
    private $fieldNames = [];

    /** @var Internal\ArrayParser */
    private $parser;

    /**
     * @param resource $handle PostgreSQL result resource.
     * @param array<int, array{string, string}> $types
     */
    public function __construct($handle, array $types = [])
    {
        $this->handle = $handle;
        $this->types = $types;

        $numFields = \pg_num_fields($this->handle);
        for ($i = 0; $i < $numFields; ++$i) {
            $this->fieldNames[] = \pg_field_name($this->handle, $i);
            $this->fieldTypes[] = \pg_field_type_oid($this->handle, $i);
        }

        $this->parser = new Internal\ArrayParser;
    }

    /**
     * Frees the result resource.
     */
    public function __destruct()
    {
        \pg_free_result($this->handle);
    }

    /**
     * {@inheritdoc}
     */
    public function advance(): Promise
    {
        $this->currentRow = null;

        if (++$this->position > \pg_num_rows($this->handle)) {
            return new Success(false);
        }

        return new Success(true);
    }

    /**
     * {@inheritdoc}
     */
    public function getCurrent(): array
    {
        if ($this->currentRow !== null) {
            return $this->currentRow;
        }

        if ($this->position > \pg_num_rows($this->handle)) {
            throw new \Error("No more rows remain in the result set");
        }

        $result = \pg_fetch_array($this->handle, null, \PGSQL_NUM);

        if ($result === false) {
            $message = \pg_result_error($this->handle);
            \pg_free_result($this->handle);
            throw new FailureException($message);
        }

        $columnCount = \count($result);
        for ($column = 0; $column < $columnCount; ++$column) {
            if ($result[$column] === null) {
                continue;
            }

            $result[$column] = $this->cast($column, $result[$column]);
        }

        return $this->currentRow = \array_combine($this->fieldNames, $result);
    }

    /**
     * @see https://github.com/postgres/postgres/blob/REL_14_STABLE/src/include/catalog/pg_type.dat for OID types.
     *
     * @param int $column
     * @param string $value
     *
     * @return array|bool|float|int Cast value.
     *
     * @throws ParseException
     */
    private function cast(int $column, string $value)
    {
        $oid = $this->fieldTypes[$column];

        switch ($oid) {
            case 16: // bool
                return $value === 't';

            case 20: // int8
            case 21: // int2
            case 23: // int4
            case 26: // oid
            case 27: // tid
            case 28: // xid
                return (int) $value;

            case 700: // real
            case 701: // double-precision
                return (float) $value;

            case 1000: // boolean[]
                return $this->parser->parse($value, function (string $value): bool {
                    return $value === 't';
                });

            case 1005: // int2[]
            case 1007: // int4[]
            case 1010: // tid[]
            case 1011: // xid[]
            case 1016: // int8[]
            case 1028: // oid[]
                return $this->parser->parse($value, function (string $value): int {
                    return (int) $value;
                });

            case 1021: // real[]
            case 1022: // double-precision[]
                return $this->parser->parse($value, function (string $value): float {
                    return (float) $value;
                });

            default:
                [$type, $delimiter] = $this->types[$oid] ?? ['S', ','];

                if ($type === 'A') {
                    return $this->parser->parse($value, null, $delimiter);
                }

                return $value;
        }
    }

    /**
     * @return int Number of rows in the result set.
     */
    public function numRows(): int
    {
        return \pg_num_rows($this->handle);
    }

    /**
     * @return int Number of fields in each row.
     */
    public function getFieldCount(): int
    {
        return \pg_num_fields($this->handle);
    }

    /**
     * @param int $fieldNum
     *
     * @return string Column name at index $fieldNum
     *
     * @throws \Error If the field number does not exist in the result.
     */
    public function getFieldName(int $fieldNum): string
    {
        if (0 > $fieldNum || $this->getFieldCount() <= $fieldNum) {
            throw new \Error(\sprintf('No field with index %d in result', $fieldNum));
        }

        return \pg_field_name($this->handle, $fieldNum);
    }

    /**
     * @param string $fieldName
     *
     * @return int Index of field with given name.
     *
     * @throws \Error If the field name does not exist in the result.
     */
    public function getFieldIndex(string $fieldName): int
    {
        $result = \pg_field_num($this->handle, $fieldName);

        if (-1 === $result) {
            throw new \Error(\sprintf('No field with name "%s" in result', $fieldName));
        }

        return $result;
    }
}

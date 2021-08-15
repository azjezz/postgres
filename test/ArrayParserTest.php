<?php

namespace Amp\Postgres\Test;

use Amp\Postgres\Internal\ArrayParser;
use Amp\Postgres\ParseException;
use PHPUnit\Framework\TestCase;

class ArrayParserTest extends TestCase
{
    /** @var ArrayParser */
    private $parser;

    public function setUp(): void
    {
        $this->parser = new ArrayParser;
    }

    protected function getDefaultCastFunction(): callable
    {
        return function (string $value) {
            return $value;
        };
    }

    public function testSingleDimensionalArray(): void
    {
        $array = ["one", "two", "three"];
        $string = '{' . \implode(',', $array) . '}';

        $this->assertSame($array, $this->parser->parse($string, $this->getDefaultCastFunction()));
    }

    public function testMultiDimensionalArray(): void
    {
        $array = ["one", "two", ["three", "four"], "five"];
        $string = '{one, two, {three, four}, five}';

        $this->assertSame($array, $this->parser->parse($string, $this->getDefaultCastFunction()));
    }

    public function testQuotedStrings(): void
    {
        $array = ["one", "two", ["three", "four"], "five"];
        $string = '{"one", "two", {"three", "four"}, "five"}';

        $this->assertSame($array, $this->parser->parse($string, $this->getDefaultCastFunction()));
    }

    public function testAlternateDelimiter(): void
    {
        $array = ["1,2,3", "3,4,5"];
        $string = '{1,2,3;3,4,5}';

        $this->assertSame($array, $this->parser->parse($string, $this->getDefaultCastFunction(), ';'));
    }

    public function testEscapedQuoteDelimiter(): void
    {
        $array = ['va"lue1', 'value"2'];
        $string = '{"va\\"lue1", "value\\"2"}';

        $this->assertSame($array, $this->parser->parse($string, $this->getDefaultCastFunction(), ','));
    }

    public function testNullValue(): void
    {
        $array = ["one", null, "three"];
        $string = '{one, NULL, three}';

        $this->assertSame($array, $this->parser->parse($string, $this->getDefaultCastFunction()));
    }

    public function testQuotedNullValue(): void
    {
        $array = ["one", "NULL", "three"];
        $string = '{one, "NULL", three}';

        $this->assertSame($array, $this->parser->parse($string, $this->getDefaultCastFunction()));
    }

    public function testCast(): void
    {
        $array = [1, 2, 3];
        $string = '{' . \implode(',', $array) . '}';

        $cast = function (string $value): int {
            return (int) $value;
        };

        $this->assertSame($array, $this->parser->parse($string, $cast));
    }

    public function testCastWithNull(): void
    {
        $array = [1, 2, null, 3];
        $string = '{1,2,NULL,3}';

        $cast = function (string $value): int {
            return (int) $value;
        };

        $this->assertSame($array, $this->parser->parse($string, $cast));
    }

    public function testCastWithMultidimensionalArray(): void
    {
        $array = [1, 2, [3, 4], [5], 6, 7, [[8, 9], 10]];
        $string = '{1,2,{3,4},{5},6,7,{{8,9},10}}';

        $cast = function (string $value): int {
            return (int) $value;
        };

        $this->assertSame($array, $this->parser->parse($string, $cast));
    }

    public function testRandomWhitespace(): void
    {
        $array = [1, 2, [3, 4], [5], 6, 7, [[8, 9], 10]];
        $string = " {1, 2, { 3 ,\r 4 },{ 5} \n\t ,6 , 7, { {8,\t 9}, 10} }  \n";

        $cast = function (string $value): int {
            return (int) $value;
        };

        $this->assertSame($array, $this->parser->parse($string, $cast));
    }

    public function testEscapedBackslashesInQuotedValue(): void
    {
        $array = ["test\\ing", "esca\\ped\\"];
        $string = '{"test\\\\ing", "esca\\\\ped\\\\"}';

        $this->assertSame($array, $this->parser->parse($string, $this->getDefaultCastFunction()));
    }

    public function testEmptyArray(): void
    {
        $array = [];
        $string = '{}';

        $this->assertSame($array, $this->parser->parse($string, $this->getDefaultCastFunction()));
    }

    public function testArrayContainingEmptyArray(): void
    {
        $array = [[], [1], []];
        $string = '{{},{1},{}}';

        $cast = function (string $value): int {
            return (int) $value;
        };

        $this->assertSame($array, $this->parser->parse($string, $cast));
    }

    public function testArrayWithEmptyString(): void
    {
        $array = [''];
        $string = '{""}';

        $this->assertSame($array, $this->parser->parse($string, $this->getDefaultCastFunction()));
    }

    public function testMalformedNestedArray(): void
    {
        $this->expectException(ParseException::class);
        $this->expectExceptionMessage('Unexpected end of data');

        $string = '{{}';
        $this->parser->parse($string, $this->getDefaultCastFunction());
    }

    public function testEmptyString(): void
    {
        $this->expectException(ParseException::class);
        $this->expectExceptionMessage('Unexpected end of data');

        $string = ' ';
        $this->parser->parse($string, $this->getDefaultCastFunction());
    }

    public function testNoOpeningBracket(): void
    {
        $this->expectException(ParseException::class);
        $this->expectExceptionMessage('Missing opening bracket');

        $string = '"one", "two"}';
        $this->parser->parse($string, $this->getDefaultCastFunction());
    }

    public function testNoClosingBracket(): void
    {
        $this->expectException(ParseException::class);
        $this->expectExceptionMessage('Unexpected end of data');

        $string = '{"one", "two"';
        $this->parser->parse($string, $this->getDefaultCastFunction());
    }

    public function testExtraClosingBracket(): void
    {
        $this->expectException(ParseException::class);
        $this->expectExceptionMessage('Data left in buffer after parsing');

        $string = '{"one", "two"}}';
        $this->parser->parse($string, $this->getDefaultCastFunction());
    }

    public function testTrailingData(): void
    {
        $this->expectException(ParseException::class);
        $this->expectExceptionMessage('Data left in buffer after parsing');

        $string = '{"one", "two"} data}';
        $this->parser->parse($string, $this->getDefaultCastFunction());
    }

    public function testMissingQuote(): void
    {
        $this->expectException(ParseException::class);
        $this->expectExceptionMessage('Could not find matching quote in quoted value');

        $string = '{"one", "two}';
        $this->parser->parse($string, $this->getDefaultCastFunction());
    }

    public function testInvalidDelimiter(): void
    {
        $this->expectException(ParseException::class);
        $this->expectExceptionMessage('Invalid delimiter');

        $string = '{"one"; "two"}';
        $this->parser->parse($string, $this->getDefaultCastFunction());
    }
}

<?php

namespace Doctrine\Tests\DBAL\Types;

use Doctrine\DBAL\Types\DateTimeType;
use Doctrine\DBAL\Types\Type;

class DateTimeTest extends BaseDateTypeTestCase
{
    /**
     * {@inheritDoc}
     */
    protected function setUp()
    {
        parent::setUp();
        $this->type = new DateTimeType($this->platform);
    }

    public function testDateTimeConvertsToDatabaseValue()
    {
        $date = new \DateTime('1985-09-01 10:10:10');

        $expected = $date->format($this->platform->getDateTimeTzFormatString());
        $actual = $this->type->convertToDatabaseValue($date);

        $this->assertEquals($expected, $actual);
    }

    public function testDateTimeConvertsToPHPValue()
    {
        // Birthday of jwage and also birthday of Doctrine. Send him a present ;)
        $date = $this->type->convertToPHPValue('1985-09-01 00:00:00');
        $this->assertInstanceOf('DateTime', $date);
        $this->assertEquals('1985-09-01 00:00:00', $date->format('Y-m-d H:i:s'));
    }

    public function testInvalidDateTimeFormatConversion()
    {
        $this->setExpectedException('Doctrine\DBAL\Types\ConversionException');
        $this->type->convertToPHPValue('abcdefg');
    }

    public function testConvertsNonMatchingFormatToPhpValueWithParser()
    {
        $date = '1985/09/01 10:10:10.12345';

        $actual = $this->type->convertToPHPValue($date);

        $this->assertEquals('1985-09-01 10:10:10', $actual->format('Y-m-d H:i:s'));
    }
}

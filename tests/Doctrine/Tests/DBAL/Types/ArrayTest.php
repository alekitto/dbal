<?php

namespace Doctrine\Tests\DBAL\Types;

use Doctrine\DBAL\Types\ArrayType;
use Doctrine\DBAL\Types\Type;
use Doctrine\Tests\DBAL\Mocks\MockPlatform;

class ArrayTest extends \Doctrine\Tests\DbalTestCase
{
    protected
        $_platform,
        $_type;

    protected function setUp()
    {
        $this->_platform = new MockPlatform();
        $this->_type = new ArrayType($this->_platform);
    }

    protected function tearDown()
    {
        error_reporting(-1); // reactive all error levels
    }


    public function testArrayConvertsToDatabaseValue()
    {
        $this->assertTrue(
            is_string($this->_type->convertToDatabaseValue(array()))
        );
    }

    public function testArrayConvertsToPHPValue()
    {
        $this->assertTrue(
            is_array($this->_type->convertToPHPValue(serialize(array())))
        );
    }

    public function testConversionFailure()
    {
        error_reporting( (E_ALL | E_STRICT) - \E_NOTICE );
        $this->setExpectedException('Doctrine\DBAL\Types\ConversionException');
        $this->_type->convertToPHPValue('abcdefg');
    }

    public function testNullConversion()
    {
        $this->assertNull($this->_type->convertToPHPValue(null));
    }

    /**
     * @group DBAL-73
     */
    public function testFalseConversion()
    {
        $this->assertFalse($this->_type->convertToPHPValue(serialize(false)));
    }
}

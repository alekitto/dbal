<?php

namespace Doctrine\Tests\DBAL\Types;

use Doctrine\DBAL\Types\ObjectType;
use Doctrine\DBAL\Types\Type;
use Doctrine\Tests\DBAL\Mocks\MockPlatform;

class ObjectTest extends \Doctrine\Tests\DbalTestCase
{
    protected
        $_platform,
        $_type;

    protected function setUp()
    {
        $this->_platform = new MockPlatform();
        $this->_type = new ObjectType($this->_platform);
    }

    protected function tearDown()
    {
        error_reporting(-1); // reactive all error levels
    }

    public function testObjectConvertsToDatabaseValue()
    {
        $this->assertInternalType('string', $this->_type->convertToDatabaseValue(new \stdClass()));
    }

    public function testObjectConvertsToPHPValue()
    {
        $this->assertInternalType('object', $this->_type->convertToPHPValue(serialize(new \stdClass)));
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

<?php

namespace Doctrine\Tests\DBAL\Types;

use Doctrine\DBAL\Types\FloatType;
use Doctrine\DBAL\Types\Type;
use Doctrine\Tests\DBAL\Mocks\MockPlatform;

class FloatTest extends \Doctrine\Tests\DbalTestCase
{
    protected $_platform, $_type;

    protected function setUp()
    {
        $this->_platform = new MockPlatform();
        $this->_type = new FloatType($this->_platform);
    }

    public function testFloatConvertsToPHPValue()
    {
        $this->assertInternalType('float', $this->_type->convertToPHPValue('5.5'));
    }

    public function testFloatNullConvertsToPHPValue()
    {
        $this->assertNull($this->_type->convertToPHPValue(null));
    }

    public function testFloatConvertToDatabaseValue()
    {
        $this->assertInternalType('float', $this->_type->convertToDatabaseValue(5.5));
    }

    public function testFloatNullConvertToDatabaseValue()
    {
        $this->assertNull($this->_type->convertToDatabaseValue(null, $this->_platform));
    }
}

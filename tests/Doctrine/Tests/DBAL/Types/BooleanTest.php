<?php

namespace Doctrine\Tests\DBAL\Types;

use Doctrine\DBAL\Types\BooleanType;
use Doctrine\DBAL\Types\Type;
use Doctrine\Tests\DBAL\Mocks\MockPlatform;

class BooleanTest extends \Doctrine\Tests\DbalTestCase
{
    protected
        $_platform,
        $_type;

    protected function setUp()
    {
        $this->_platform = new MockPlatform();
        $this->_type = new BooleanType($this->_platform);
    }

    public function testBooleanConvertsToDatabaseValue()
    {
        $this->assertInternalType('integer', $this->_type->convertToDatabaseValue(1));
    }

    public function testBooleanConvertsToPHPValue()
    {
        $this->assertInternalType('bool', $this->_type->convertToPHPValue(0));
    }

    public function testBooleanNullConvertsToPHPValue()
    {
        $this->assertNull($this->_type->convertToPHPValue(null));
    }
}

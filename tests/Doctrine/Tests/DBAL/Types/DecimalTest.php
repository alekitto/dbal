<?php

namespace Doctrine\Tests\DBAL\Types;

use Doctrine\DBAL\Types\DecimalType;
use Doctrine\DBAL\Types\Type;
use Doctrine\Tests\DBAL\Mocks\MockPlatform;

class DecimalTest extends \Doctrine\Tests\DbalTestCase
{
    protected
        $_platform,
        $_type;

    protected function setUp()
    {
        $this->_platform = new MockPlatform();
        $this->_type = new DecimalType($this->_platform);
    }

    public function testDecimalConvertsToPHPValue()
    {
        $this->assertInternalType('string', $this->_type->convertToPHPValue('5.5'));
    }

    public function testDecimalNullConvertsToPHPValue()
    {
        $this->assertNull($this->_type->convertToPHPValue(null));
    }
}

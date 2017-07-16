<?php

namespace Doctrine\Tests\DBAL\Types;

use Doctrine\DBAL\Types\GuidType;
use Doctrine\DBAL\Types\Type;
use Doctrine\Tests\DBAL\Mocks\MockPlatform;

class GuidTest extends \Doctrine\Tests\DbalTestCase
{
    protected
        $_platform,
        $_type;

    protected function setUp()
    {
        $this->_platform = new MockPlatform();
        $this->_type = new GuidType($this->_platform);
    }

    public function testConvertToPHPValue()
    {
        $this->assertInternalType("string", $this->_type->convertToPHPValue("foo"));
        $this->assertInternalType("string", $this->_type->convertToPHPValue(""));
    }

    public function testNullConversion()
    {
        $this->assertNull($this->_type->convertToPHPValue(null));
    }

    public function testNativeGuidSupport()
    {
        $this->assertTrue($this->_type->requiresSQLCommentHint());

        $mock = $this->createMock(get_class($this->_platform));
        $mock->expects($this->any())
             ->method('hasNativeGuidType')
             ->will($this->returnValue(true));

        $this->_type = new GuidType($mock);
        $this->assertFalse($this->_type->requiresSQLCommentHint());
    }
}

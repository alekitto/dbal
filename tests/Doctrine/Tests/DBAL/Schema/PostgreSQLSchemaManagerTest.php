<?php

namespace Doctrine\Tests\DBAL\Schema;

use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Driver;
use Doctrine\DBAL\Platforms\PostgreSqlPlatform;
use Doctrine\DBAL\Schema\PostgreSqlSchemaManager;
use Doctrine\DBAL\Schema\Sequence;
use Prophecy\Argument;
use Prophecy\Prophecy\ObjectProphecy;

class PostgreSQLSchemaManagerTest extends \PHPUnit_Framework_TestCase
{
    /**
     * @var \Doctrine\DBAL\Schema\PostgreSQLSchemaManager
     */
    private $schemaManager;

    /**
     * @var \Doctrine\DBAL\Connection|ObjectProphecy
     */
    private $connection;

    protected function setUp()
    {
        $platform = $this->prophesize(PostgreSqlPlatform::class);

        $this->connection = $this->prophesize(Connection::class);
        $this->schemaManager = new PostgreSqlSchemaManager($this->connection->reveal(), $platform->reveal());
    }

    /**
     * @group DBAL-474
     */
    public function testFiltersSequences()
    {
        $configuration = new Configuration();
        $configuration->setFilterSchemaAssetsExpression('/^schema/');

        $sequences = array(
            array('relname' => 'foo', 'schemaname' => 'schema'),
            array('relname' => 'bar', 'schemaname' => 'schema'),
            array('relname' => 'baz', 'schemaname' => ''),
            array('relname' => 'bloo', 'schemaname' => 'bloo_schema'),
        );

        $this->connection->getConfiguration()->willReturn($configuration);
        $this->connection->fetchAll(Argument::cetera())
            ->willReturn(
                $sequences,
                array(array('min_value' => 1, 'increment_by' => 1)),
                array(array('min_value' => 2, 'increment_by' => 2))
            )
            ->shouldBeCalledTimes(3);

        $this->assertEquals(
            array(
                new Sequence('schema.foo', 2, 2),
                new Sequence('schema.bar', 1, 1),
            ),
            $this->schemaManager->listSequences('database')
        );
    }
}

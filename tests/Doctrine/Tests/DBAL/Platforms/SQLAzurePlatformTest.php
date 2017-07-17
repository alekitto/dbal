<?php

namespace Doctrine\Tests\DBAL\Platforms;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Driver;
use Doctrine\DBAL\Types\Type;
use Doctrine\Tests\DbalTestCase;
use Prophecy\Argument;

/**
 * @group DBAL-222
 */
class SQLAzurePlatformTest extends DbalTestCase
{
    private $platform;

    protected function setUp()
    {
        $this->platform = new \Doctrine\DBAL\Platforms\SQLAzurePlatform();

        $connection = new Connection(
            array('platform' => $this->platform),
            $this->prophesize(Driver::class)->reveal()
        );

        $this->platform->setConnection($connection);
    }

    public function testCreateFederatedOnTable()
    {
        $table = new \Doctrine\DBAL\Schema\Table("tbl");
        $table->addColumn("id", "integer");
        $table->addOption('azure.federatedOnDistributionName', 'TblId');
        $table->addOption('azure.federatedOnColumnName', 'id');

        $this->assertEquals(array('CREATE TABLE tbl (id INT NOT NULL) FEDERATED ON (TblId = id)'), $this->platform->getCreateTableSQL($table));
    }
}


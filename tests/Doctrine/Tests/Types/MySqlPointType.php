<?php

namespace Doctrine\Tests\Types;

use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Types\Type;

class MySqlPointType extends Type
{
    public function getName()
    {
        return 'point';
    }

    public function getSQLDeclaration(array $fieldDeclaration)
    {
        return strtoupper($this->getName());
    }

    public function getMappedDatabaseTypes()
    {
        return array('point');
    }
}

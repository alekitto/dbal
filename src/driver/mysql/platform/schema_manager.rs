use super::mysql;
use crate::driver::mysql::platform::MySQLVariant;
use crate::platform::default;
use crate::schema::{
    Column, ColumnData, Comparator, ForeignKeyConstraint, GenericComparator, Identifier, Index,
    SchemaManager, TableDiff, TableOptions,
};
use crate::{Connection, Result, Row};

pub struct MySQLSchemaManager<'a> {
    variant: MySQLVariant,
    connection: &'a Connection,
}

impl<'a> MySQLSchemaManager<'a> {
    pub fn new(connection: &'a Connection, variant: MySQLVariant) -> Self {
        Self {
            variant,
            connection,
        }
    }
}

pub trait AbstractMySQLSchemaManager: SchemaManager {
    /// Build SQL for table options
    fn build_table_options(&self, options: &TableOptions) -> Result<String> {
        mysql::build_table_options(self, options)
    }

    /// Build SQL for partition options
    fn build_partition_options(&self, options: &TableOptions) -> String {
        mysql::build_partition_options(options)
    }
}

impl AbstractMySQLSchemaManager for MySQLSchemaManager<'_> {}
impl<'a> SchemaManager for MySQLSchemaManager<'a> {
    fn get_connection(&self) -> &'a Connection {
        self.connection
    }

    #[inline]
    fn get_rename_index_sql(
        &self,
        old_index_name: &Identifier,
        index: &Index,
        table_name: &Identifier,
    ) -> Result<Vec<String>> {
        match self.variant {
            MySQLVariant::MySQL | MySQLVariant::MySQL80 => {
                mysql::get_rename_index_sql(self.get_platform()?, old_index_name, index, table_name)
            }
            _ => default::get_rename_index_sql(self, old_index_name, index, table_name),
        }
    }

    fn get_portable_table_column_definition(&self, table_column: &Row) -> Result<Column> {
        todo!()
    }

    #[inline]
    fn get_list_databases_sql(&self) -> Result<String> {
        mysql::get_list_databases_sql()
    }

    #[inline]
    fn get_list_views_sql(&self, database: &str) -> Result<String> {
        mysql::get_list_views_sql(self, database)
    }

    #[inline]
    fn get_drop_index_sql(&self, index: &Identifier, table: &Identifier) -> Result<String> {
        mysql::get_drop_index_sql(self.get_platform()?, index, table)
    }

    #[inline]
    fn get_drop_unique_constraint_sql(
        &self,
        name: &Identifier,
        table_name: &Identifier,
    ) -> Result<String> {
        mysql::get_drop_unique_constraint_sql(self, name, table_name)
    }

    /// MySQL commits a transaction implicitly when DROP TABLE is executed, however not
    /// if DROP TEMPORARY TABLE is executed.
    #[inline]
    fn get_drop_temporary_table_sql(&self, table: &Identifier) -> Result<String> {
        mysql::get_drop_temporary_table_sql(self.get_platform()?, table)
    }

    #[inline]
    fn get_alter_table_sql(&self, diff: &mut TableDiff) -> Result<Vec<String>>
    where
        Self: Sync,
    {
        mysql::get_alter_table_sql(self, diff)
    }

    #[inline]
    fn _get_create_table_sql(
        &self,
        name: &Identifier,
        columns: &[ColumnData],
        options: &TableOptions,
    ) -> Result<Vec<String>> {
        mysql::_get_create_table_sql(self, name, columns, options)
    }

    #[inline]
    fn get_create_index_sql_flags(&self, index: &Index) -> String {
        mysql::get_create_index_sql_flags(index)
    }

    #[inline]
    fn get_pre_alter_table_index_foreign_key_sql(&self, diff: &mut TableDiff) -> Result<Vec<String>>
    where
        Self: Sync,
    {
        mysql::get_pre_alter_table_index_foreign_key_sql(self, diff)
    }

    #[inline]
    fn get_column_collation_declaration_sql(&self, collation: &str) -> Result<String> {
        mysql::get_column_collation_declaration_sql(self.get_platform()?, collation)
    }

    #[inline]
    fn get_advanced_foreign_key_options_sql(
        &self,
        foreign_key: &ForeignKeyConstraint,
    ) -> Result<String> {
        mysql::get_advanced_foreign_key_options_sql(self, foreign_key)
    }

    fn create_comparator(&self) -> Box<dyn Comparator + Send + '_> {
        Box::new(GenericComparator::new(self))
    }
}

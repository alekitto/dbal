use super::postgresql;
use crate::schema::{
    Column, ColumnData, Comparator, ForeignKeyConstraint, GenericComparator, Identifier, Index,
    IntoIdentifier, SchemaManager, Sequence, TableDiff, TableOptions,
};
use crate::{Connection, Result, Row};

pub struct PostgreSQLSchemaManager<'a> {
    connection: &'a Connection,
}

impl<'a> PostgreSQLSchemaManager<'a> {
    pub fn new(connection: &'a Connection) -> Self {
        Self { connection }
    }
}

pub trait AbstractPostgreSQLSchemaManager: SchemaManager {}

impl AbstractPostgreSQLSchemaManager for PostgreSQLSchemaManager<'_> {}
impl<'a> SchemaManager for PostgreSQLSchemaManager<'a> {
    #[inline]
    fn get_list_databases_sql(&self) -> Result<String> {
        postgresql::get_list_databases_sql()
    }

    #[inline]
    fn get_list_sequences_sql(&self, database: &str) -> Result<String> {
        postgresql::get_list_sequences_sql(self, database)
    }

    #[inline]
    fn get_list_table_columns_sql(&self, table: &str, _: Option<&str>) -> Result<String> {
        postgresql::get_list_table_columns_sql(self, table)
    }

    #[inline]
    fn get_list_table_indexes_sql(&self, table: &str, _: Option<&str>) -> Result<String> {
        postgresql::get_list_table_indexes_sql(self, table)
    }

    #[inline]
    fn get_list_tables_sql(&self) -> Result<String> {
        postgresql::get_list_tables_sql()
    }

    #[inline]
    fn get_list_views_sql(&self, _: &str) -> Result<String> {
        postgresql::get_list_views_sql()
    }

    #[inline]
    fn get_list_table_foreign_keys_sql(&self, table: &str) -> Result<String> {
        postgresql::get_list_table_foreign_keys_sql(self, table)
    }

    #[inline]
    fn get_alter_table_sql(&self, diff: &mut TableDiff) -> Result<Vec<String>>
    where
        Self: Sync,
    {
        postgresql::get_alter_table_sql(self, diff)
    }

    #[inline]
    fn get_comment_on_column_sql(
        &self,
        table_name: &Identifier,
        column: &Column,
        comment: &str,
    ) -> Result<String> {
        postgresql::get_comment_on_column_sql(self.get_platform()?, table_name, column, comment)
    }

    #[inline]
    fn get_column_collation_declaration_sql(&self, collation: &str) -> Result<String> {
        postgresql::get_column_collation_declaration_sql(self.get_platform()?, collation)
    }

    #[inline]
    fn get_create_sequence_sql(&self, sequence: &Sequence) -> Result<String> {
        postgresql::get_create_sequence_sql(self.get_platform()?, sequence)
    }

    #[inline]
    fn get_alter_sequence_sql(&self, sequence: &Sequence) -> Result<String> {
        postgresql::get_alter_sequence_sql(self.get_platform()?, sequence)
    }

    #[inline]
    fn get_drop_sequence_sql(&self, sequence: &dyn IntoIdentifier) -> Result<String> {
        postgresql::get_drop_sequence_sql(self.get_platform()?, sequence)
    }

    #[inline]
    fn get_sequence_next_val_sql(&self, sequence: &str) -> Result<String> {
        postgresql::get_sequence_next_val_sql(sequence)
    }

    #[inline]
    fn get_advanced_foreign_key_options_sql(
        &self,
        foreign_key: &ForeignKeyConstraint,
    ) -> Result<String> {
        postgresql::get_advanced_foreign_key_options_sql(self, foreign_key)
    }

    #[inline]
    fn get_list_table_constraints_sql(&self, table: &str) -> Result<String> {
        postgresql::get_list_table_constraints_sql(self, table)
    }

    fn get_drop_foreign_key_sql(
        &self,
        foreign_key: &dyn IntoIdentifier,
        table_name: &dyn IntoIdentifier,
    ) -> Result<String> {
        postgresql::get_drop_foreign_key_sql(self, foreign_key, table_name)
    }

    fn _get_create_table_sql(
        &self,
        name: &Identifier,
        columns: &[ColumnData],
        options: &TableOptions,
    ) -> Result<Vec<String>> {
        postgresql::_get_create_table_sql(self, name, columns, options)
    }

    fn get_rename_index_sql(
        &self,
        old_index_name: &Identifier,
        index: &Index,
        table_name: &Identifier,
    ) -> Result<Vec<String>> {
        postgresql::get_rename_index_sql(self, old_index_name, index, table_name)
    }

    fn get_connection(&self) -> &'a Connection {
        self.connection
    }

    fn get_portable_table_column_definition(&self, table_column: &Row) -> Result<Column> {
        todo!()
    }

    fn create_comparator(&self) -> Box<dyn Comparator + Send + '_> {
        Box::new(GenericComparator::new(self))
    }
}

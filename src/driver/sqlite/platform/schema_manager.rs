use super::sqlite;
use crate::platform::CreateFlags;
use crate::schema::{
    Column, ColumnData, Comparator, ForeignKeyConstraint, GenericComparator, Identifier, Index,
    IntoIdentifier, SchemaManager, Table, TableDiff, TableOptions,
};
use crate::{Connection, Error, Result, Row};

pub struct SQLiteSchemaManager<'a> {
    connection: &'a Connection,
}

impl<'a> SQLiteSchemaManager<'a> {
    pub fn new(connection: &'a Connection) -> Self {
        Self { connection }
    }
}

pub trait AbstractSQLiteSchemaManager: SchemaManager {}
impl AbstractSQLiteSchemaManager for SQLiteSchemaManager<'_> {}

impl<'a> SchemaManager for SQLiteSchemaManager<'a> {
    #[inline(always)]
    fn get_list_table_columns_sql(&self, table: &str, _: Option<&str>) -> Result<String> {
        sqlite::get_list_table_columns_sql(self, table)
    }

    #[inline(always)]
    fn get_list_tables_sql(&self) -> Result<String> {
        sqlite::get_list_tables_sql()
    }

    #[inline(always)]
    fn get_list_views_sql(&self, _: &str) -> Result<String> {
        sqlite::get_list_views_sql()
    }

    #[inline(always)]
    fn get_list_table_foreign_keys_sql(&self, table: &str) -> Result<String> {
        sqlite::get_list_table_foreign_keys_sql(self, table)
    }

    #[inline(always)]
    fn get_drop_tables_sql(&self, tables: &[Table]) -> Result<Vec<String>> {
        sqlite::get_drop_tables_sql(self, tables)
    }

    #[inline(always)]
    fn get_create_table_sql(
        &self,
        table: &Table,
        create_flags: Option<CreateFlags>,
    ) -> Result<Vec<String>>
    where
        Self: Sync,
    {
        sqlite::get_create_table_sql(self, table, create_flags)
    }

    #[inline(always)]
    fn get_alter_table_sql(&self, diff: &mut TableDiff) -> Result<Vec<String>>
    where
        Self: Sync,
    {
        sqlite::get_alter_table_sql(self, diff)
    }

    fn get_list_table_constraints_sql(&self, table: &str) -> Result<String> {
        sqlite::get_list_table_constraints_sql(self, table)
    }

    fn _get_create_table_sql(
        &self,
        name: &Identifier,
        columns: &[ColumnData],
        options: &TableOptions,
    ) -> Result<Vec<String>> {
        sqlite::_get_create_table_sql(self, name, columns, options)
    }

    fn get_inline_column_comment_sql(&self, comment: &str) -> Result<String> {
        sqlite::get_inline_column_comment_sql(comment)
    }

    fn get_create_tables_sql(&self, tables: &[Table]) -> Result<Vec<String>> {
        sqlite::get_create_tables_sql(self, tables)
    }

    fn get_create_primary_key_sql(&self, _: &Index, _: &Identifier) -> Result<String> {
        Err(Error::platform_feature_unsupported(
            "Sqlite platform does not support alter primary key.",
        ))
    }

    fn get_drop_foreign_key_sql(
        &self,
        _: &dyn IntoIdentifier,
        _: &dyn IntoIdentifier,
    ) -> Result<String> {
        Err(Error::platform_feature_unsupported(
            "Sqlite platform does not support alter foreign key.",
        ))
    }
    fn get_pre_alter_table_index_foreign_key_sql(&self, _: &mut TableDiff) -> Result<Vec<String>> {
        sqlite::get_pre_alter_table_index_foreign_key_sql()
    }

    fn get_post_alter_table_index_foreign_key_sql(&self, diff: &TableDiff) -> Result<Vec<String>> {
        sqlite::get_post_alter_table_index_foreign_key_sql(self, diff)
    }

    fn get_advanced_foreign_key_options_sql(
        &self,
        foreign_key: &ForeignKeyConstraint,
    ) -> Result<String> {
        sqlite::get_advanced_foreign_key_options_sql(self, foreign_key)
    }

    fn get_create_foreign_key_sql(
        &self,
        _: &ForeignKeyConstraint,
        _: &Identifier,
    ) -> Result<String> {
        Err(Error::platform_feature_unsupported(
            "Sqlite platform does not support alter foreign key.",
        ))
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

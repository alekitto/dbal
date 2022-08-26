use super::mysql;
use crate::driver::mysql::platform::{mariadb, MySQLVariant};
use crate::platform::{default, DatabasePlatform, DateIntervalUnit, KeywordList};
use crate::r#type::{
    BigintType, BinaryType, BlobType, DateTimeType, DateType, DecimalType, FloatType, IntegerType,
    JsonType, SimpleArrayType, StringType, TextType, TimeType,
};
use crate::schema::{ColumnData, ForeignKeyConstraint, Identifier, Index, TableDiff, TableOptions};
use crate::{platform_debug, Error};
use crate::{EventDispatcher, Result, TransactionIsolationLevel};
use dashmap::DashMap;
use std::any::TypeId;
use std::sync::Arc;

pub const LENGTH_LIMIT_TINYTEXT: usize = 255;
pub const LENGTH_LIMIT_TEXT: usize = 65535;
pub const LENGTH_LIMIT_MEDIUMTEXT: usize = 16777215;

pub const LENGTH_LIMIT_TINYBLOB: usize = 255;
pub const LENGTH_LIMIT_BLOB: usize = 65535;
pub const LENGTH_LIMIT_MEDIUMBLOB: usize = 16777215;

pub trait AbstractMySQLPlatform: DatabasePlatform {
    /// Build SQL for table options
    fn build_table_options(&self, options: &TableOptions) -> String {
        mysql::build_table_options(self, options)
    }

    /// Build SQL for partition options
    fn build_partition_options(&self, options: &TableOptions) -> String {
        mysql::build_partition_options(options)
    }
}

platform_debug!(MySQLPlatform);
pub(crate) struct MySQLPlatform {
    variant: MySQLVariant,
    ev: Arc<EventDispatcher>,
    type_mappings: DashMap<String, TypeId>,
}

impl MySQLPlatform {
    pub fn new(variant: MySQLVariant, ev: Arc<EventDispatcher>) -> Self {
        Self {
            variant,
            ev,
            type_mappings: DashMap::default(),
        }
    }
}

impl AbstractMySQLPlatform for MySQLPlatform {}

impl DatabasePlatform for MySQLPlatform {
    fn get_event_manager(&self) -> Arc<EventDispatcher> {
        self.ev.clone()
    }

    #[inline(always)]
    fn get_boolean_type_declaration_sql(&self, _: &ColumnData) -> Result<String> {
        mysql::get_boolean_type_declaration_sql()
    }

    #[inline(always)]
    fn get_json_type_declaration_sql(&self, _: &ColumnData) -> Result<String> {
        match self.variant {
            MySQLVariant::MariaDB => mariadb::get_json_type_declaration_sql(),
            _ => mysql::get_json_type_declaration_sql(),
        }
    }

    #[inline(always)]
    fn get_integer_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        mysql::get_integer_type_declaration_sql(column)
    }

    #[inline(always)]
    fn get_bigint_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        mysql::get_bigint_type_declaration_sql(column)
    }

    #[inline(always)]
    fn get_smallint_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        mysql::get_smallint_type_declaration_sql(column)
    }

    #[inline(always)]
    fn get_varchar_type_declaration_sql_snippet(
        &self,
        length: Option<usize>,
        fixed: bool,
    ) -> Result<String> {
        mysql::get_varchar_type_declaration_sql_snippet(length, fixed)
    }

    #[inline(always)]
    fn get_binary_type_declaration_sql_snippet(
        &self,
        length: Option<usize>,
        fixed: bool,
    ) -> Result<String> {
        mysql::get_binary_type_declaration_sql_snippet(length, fixed)
    }

    /// Gets the SQL snippet used to declare a CLOB column type.
    ///     TINYTEXT   : 2 ^  8 - 1 = 255
    ///     TEXT       : 2 ^ 16 - 1 = 65535
    ///     MEDIUMTEXT : 2 ^ 24 - 1 = 16777215
    ///     LONGTEXT   : 2 ^ 32 - 1 = 4294967295
    #[inline(always)]
    fn get_clob_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        mysql::get_clob_type_declaration_sql(column)
    }

    #[inline(always)]
    fn get_blob_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        mysql::get_blob_type_declaration_sql(column)
    }

    fn get_name(&self) -> String {
        "mysql".to_string()
    }

    #[inline(always)]
    fn get_regexp_expression(&self) -> Result<String> {
        mysql::get_regexp_expression()
    }

    #[inline(always)]
    fn get_length_expression(&self, column: &str) -> Result<String> {
        mysql::get_length_expression(column)
    }

    #[inline(always)]
    fn get_concat_expression(&self, strings: Vec<&str>) -> Result<String> {
        mysql::get_concat_expression(strings)
    }

    #[inline(always)]
    fn get_date_diff_expression(&self, date1: &str, date2: &str) -> Result<String> {
        mysql::get_date_diff_expression(date1, date2)
    }

    #[inline(always)]
    fn get_date_arithmetic_interval_expression(
        &self,
        date: &str,
        operator: &str,
        interval: i64,
        unit: DateIntervalUnit,
    ) -> Result<String> {
        mysql::get_date_arithmetic_interval_expression(date, operator, interval, unit)
    }

    #[inline(always)]
    fn get_current_database_expression(&self) -> String {
        mysql::get_current_database_expression()
    }

    #[inline(always)]
    fn get_read_lock_sql(&self) -> Result<String> {
        mysql::get_read_lock_sql()
    }

    /// MySQL commits a transaction implicitly when DROP TABLE is executed, however not
    /// if DROP TEMPORARY TABLE is executed.
    #[inline(always)]
    fn get_drop_temporary_table_sql(&self, table: &Identifier) -> Result<String>
    where
        Self: Sized + Sync,
    {
        mysql::get_drop_temporary_table_sql(self, table)
    }

    #[inline(always)]
    fn get_drop_index_sql(&self, index: &Identifier, table: &Identifier) -> Result<String> {
        mysql::get_drop_index_sql(self, index, table)
    }

    #[inline(always)]
    fn get_drop_unique_constraint_sql(
        &self,
        name: Identifier,
        table_name: &Identifier,
    ) -> Result<String> {
        mysql::get_drop_unique_constraint_sql(self, name, table_name)
    }

    #[inline(always)]
    fn _get_create_table_sql(
        &self,
        name: &Identifier,
        columns: &[ColumnData],
        options: &TableOptions,
    ) -> Result<Vec<String>>
    where
        Self: Sized,
    {
        mysql::_get_create_table_sql(self, name, columns, options)
    }

    #[inline(always)]
    fn get_create_index_sql_flags(&self, index: &Index) -> String {
        mysql::get_create_index_sql_flags(index)
    }

    #[inline(always)]
    fn quote_string_literal(&self, str: &str) -> String {
        mysql::quote_string_literal(self, str)
    }

    #[inline(always)]
    fn get_alter_table_sql(&self, diff: &mut TableDiff) -> Result<Vec<String>>
    where
        Self: Sized + Sync,
    {
        mysql::get_alter_table_sql(self, diff)
    }

    #[inline(always)]
    fn get_pre_alter_table_index_foreign_key_sql(&self, diff: &mut TableDiff) -> Result<Vec<String>>
    where
        Self: Sized + Sync,
    {
        mysql::get_pre_alter_table_index_foreign_key_sql(self, diff)
    }

    #[inline(always)]
    fn get_rename_index_sql(
        &self,
        old_index_name: &Identifier,
        index: &Index,
        table_name: &Identifier,
    ) -> Result<Vec<String>> {
        match self.variant {
            MySQLVariant::MySQL | MySQLVariant::MySQL80 => {
                mysql::get_rename_index_sql(self, old_index_name, index, table_name)
            }
            _ => default::get_rename_index_sql(self, old_index_name, index, table_name),
        }
    }

    #[inline(always)]
    fn get_decimal_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        mysql::get_decimal_type_declaration_sql(column)
    }

    #[inline(always)]
    fn get_default_value_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        mysql::get_default_value_declaration_sql(self, column)
    }

    #[inline(always)]
    fn get_advanced_foreign_key_options_sql(
        &self,
        foreign_key: &ForeignKeyConstraint,
    ) -> Result<String> {
        mysql::get_advanced_foreign_key_options_sql(self, foreign_key)
    }

    #[inline(always)]
    fn get_column_charset_declaration_sql(&self, charset: &str) -> String {
        mysql::get_column_charset_declaration_sql(charset)
    }

    #[inline(always)]
    fn get_column_collation_declaration_sql(&self, collation: &str) -> String {
        mysql::get_column_collation_declaration_sql(self, collation)
    }

    #[inline(always)]
    fn get_list_databases_sql(&self) -> Result<String> {
        mysql::get_list_databases_sql()
    }

    #[inline(always)]
    fn get_list_views_sql(&self, database: &str) -> Result<String> {
        mysql::get_list_views_sql(self, database)
    }

    #[inline(always)]
    fn get_set_transaction_isolation_sql(
        &self,
        level: TransactionIsolationLevel,
    ) -> Result<String> {
        mysql::get_set_transaction_isolation_sql(self, level)
    }

    #[inline(always)]
    fn get_date_time_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        mysql::get_date_time_type_declaration_sql(column)
    }

    #[inline(always)]
    fn get_date_type_declaration_sql(&self, _: &ColumnData) -> Result<String> {
        mysql::get_date_type_declaration_sql()
    }

    #[inline(always)]
    fn get_time_type_declaration_sql(&self, _: &ColumnData) -> Result<String> {
        mysql::get_time_type_declaration_sql()
    }

    #[inline(always)]
    fn get_float_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        mysql::get_float_declaration_sql(column)
    }

    #[inline(always)]
    fn get_default_transaction_isolation_level(&self) -> TransactionIsolationLevel {
        mysql::get_default_transaction_isolation_level()
    }

    /**
     * {@inheritDoc}
     */
    fn _initialize_type_mappings(&self) {
        self._add_type_mapping("bigint", TypeId::of::<BigintType>());
        self._add_type_mapping("binary", TypeId::of::<BinaryType>());
        self._add_type_mapping("blob", TypeId::of::<BlobType>());
        self._add_type_mapping("char", TypeId::of::<StringType>());
        self._add_type_mapping("date", TypeId::of::<DateType>());
        self._add_type_mapping("datetime", TypeId::of::<DateTimeType>());
        self._add_type_mapping("decimal", TypeId::of::<DecimalType>());
        self._add_type_mapping("double", TypeId::of::<FloatType>());
        self._add_type_mapping("float", TypeId::of::<FloatType>());
        self._add_type_mapping("json", TypeId::of::<JsonType>());
        self._add_type_mapping("int", TypeId::of::<IntegerType>());
        self._add_type_mapping("integer", TypeId::of::<IntegerType>());
        self._add_type_mapping("longblob", TypeId::of::<BlobType>());
        self._add_type_mapping("longtext", TypeId::of::<TextType>());
        self._add_type_mapping("mediumblob", TypeId::of::<BlobType>());
        self._add_type_mapping("mediumint", TypeId::of::<IntegerType>());
        self._add_type_mapping("mediumtext", TypeId::of::<TextType>());
        self._add_type_mapping("numeric", TypeId::of::<DecimalType>());
        self._add_type_mapping("real", TypeId::of::<FloatType>());
        self._add_type_mapping("set", TypeId::of::<SimpleArrayType>());
        self._add_type_mapping("smallint", TypeId::of::<IntegerType>());
        self._add_type_mapping("string", TypeId::of::<StringType>());
        self._add_type_mapping("text", TypeId::of::<TextType>());
        self._add_type_mapping("time", TypeId::of::<TimeType>());
        self._add_type_mapping("timestamp", TypeId::of::<DateTimeType>());
        self._add_type_mapping("tinyblob", TypeId::of::<BlobType>());
        self._add_type_mapping("tinytext", TypeId::of::<TextType>());
        self._add_type_mapping("varbinary", TypeId::of::<BinaryType>());
        self._add_type_mapping("varchar", TypeId::of::<StringType>());
        self._add_type_mapping("year", TypeId::of::<DateType>());
    }

    fn create_reserved_keywords_list(&self) -> KeywordList {
        match self.variant {
            MySQLVariant::MySQL80 => KeywordList::mysql80_keywords(),
            MySQLVariant::MariaDB => KeywordList::mariadb_keywords(),
            _ => KeywordList::mysql_keywords(),
        }
    }

    #[inline(always)]
    fn supports_identity_columns(&self) -> bool {
        true
    }

    #[inline(always)]
    fn supports_column_length_indexes(&self) -> bool {
        true
    }

    #[inline(always)]
    fn supports_inline_column_comments(&self) -> bool {
        true
    }

    #[inline(always)]
    fn supports_column_collation(&self) -> bool {
        true
    }

    #[inline(always)]
    fn modify_limit_query(
        &self,
        query: &str,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> String {
        mysql::modify_limit_query(query, limit, offset)
    }

    fn _add_type_mapping(&self, db_type: &str, type_id: TypeId) {
        self.type_mappings.insert(db_type.to_string(), type_id);
    }

    fn get_type_mapping(&self, db_type: &str) -> Result<TypeId> {
        let db_type = db_type.to_lowercase();
        self.type_mappings
            .get(&db_type)
            .map(|r| *r.value())
            .ok_or_else(|| Error::unknown_database_type(&db_type, &self.get_name()))
    }
}

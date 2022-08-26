use super::postgresql;
use crate::platform::{DatabasePlatform, DateIntervalUnit, KeywordList};
use crate::schema::{
    Column, ColumnData, ForeignKeyConstraint, Identifier, Index, Sequence, TableDiff, TableOptions,
};
use crate::{platform_debug, Error, EventDispatcher, Result, TransactionIsolationLevel, Value};
use dashmap::DashMap;
use std::any::TypeId;
use std::sync::Arc;

pub trait AbstractPostgreSQLPlatform: DatabasePlatform {}

platform_debug!(PostgreSQLPlatform);
pub(crate) struct PostgreSQLPlatform {
    ev: Arc<EventDispatcher>,
    type_mappings: DashMap<String, TypeId>,
}

impl PostgreSQLPlatform {
    pub fn new(ev: Arc<EventDispatcher>) -> Self {
        Self {
            ev,
            type_mappings: DashMap::default(),
        }
    }
}

impl AbstractPostgreSQLPlatform for PostgreSQLPlatform {}

impl DatabasePlatform for PostgreSQLPlatform {
    fn get_event_manager(&self) -> Arc<EventDispatcher> {
        self.ev.clone()
    }

    fn get_substring_expression(
        &self,
        string: &str,
        start: usize,
        length: Option<usize>,
    ) -> Result<String> {
        postgresql::get_substring_expression(string, start, length)
    }

    fn get_regexp_expression(&self) -> Result<String> {
        postgresql::get_regex_expression()
    }

    fn get_locate_expression(
        &self,
        str: &str,
        substr: &str,
        start_pos: Option<usize>,
    ) -> Result<String> {
        postgresql::get_locate_expression(self, str, substr, start_pos)
    }

    fn get_date_arithmetic_interval_expression(
        &self,
        date: &str,
        operator: &str,
        interval: i64,
        unit: DateIntervalUnit,
    ) -> Result<String> {
        postgresql::get_date_arithmetic_interval_expression(date, operator, interval, unit)
    }

    fn get_date_diff_expression(&self, date1: &str, date2: &str) -> Result<String> {
        postgresql::get_date_diff_expression(date1, date2)
    }

    fn get_current_database_expression(&self) -> String {
        postgresql::get_current_database_expression()
    }

    fn supports_sequences(&self) -> bool {
        true
    }

    fn supports_schemas(&self) -> bool {
        true
    }

    fn supports_identity_columns(&self) -> bool {
        true
    }

    fn supports_partial_indexes(&self) -> bool {
        true
    }

    fn supports_comment_on_statement(&self) -> bool {
        true
    }

    fn has_native_guid_type(&self) -> bool {
        true
    }

    fn get_list_databases_sql(&self) -> Result<String> {
        postgresql::get_list_databases_sql()
    }

    fn get_list_sequences_sql(&self, database: &str) -> Result<String> {
        postgresql::get_list_sequences_sql(self, database)
    }

    fn get_list_tables_sql(&self) -> Result<String> {
        postgresql::get_list_tables_sql()
    }

    fn get_list_views_sql(&self, _: &str) -> Result<String> {
        postgresql::get_list_views_sql()
    }

    fn get_list_table_foreign_keys_sql(&self, table: &str) -> Result<String> {
        postgresql::get_list_table_foreign_keys_sql(self, table)
    }

    fn get_list_table_constraints_sql(&self, table: &str) -> Result<String> {
        postgresql::get_list_table_constraints_sql(self, table)
    }

    fn get_list_table_indexes_sql(&self, table: &str, _: Option<&str>) -> Result<String> {
        postgresql::get_list_table_indexes_sql(self, table)
    }

    fn get_list_table_columns_sql(&self, table: &str, _: Option<&str>) -> Result<String> {
        postgresql::get_list_table_columns_sql(self, table)
    }

    fn get_advanced_foreign_key_options_sql(
        &self,
        foreign_key: &ForeignKeyConstraint,
    ) -> Result<String> {
        postgresql::get_advanced_foreign_key_options_sql(self, foreign_key)
    }

    fn get_alter_table_sql(&self, diff: &mut TableDiff) -> Result<Vec<String>> {
        postgresql::get_alter_table_sql(self, diff)
    }

    fn get_rename_index_sql(
        &self,
        old_index_name: &Identifier,
        index: &Index,
        table_name: &Identifier,
    ) -> Result<Vec<String>> {
        postgresql::get_rename_index_sql(self, old_index_name, index, table_name)
    }

    fn get_comment_on_column_sql(
        &self,
        table_name: &Identifier,
        column: &Column,
        comment: &str,
    ) -> String {
        postgresql::get_comment_on_column_sql(self, table_name, column, comment)
    }

    fn get_create_sequence_sql(&self, sequence: &Sequence) -> Result<String> {
        postgresql::get_create_sequence_sql(self, sequence)
    }

    fn get_alter_sequence_sql(&self, sequence: &Sequence) -> Result<String> {
        postgresql::get_alter_sequence_sql(self, sequence)
    }

    fn get_drop_sequence_sql(&self, sequence: &Sequence) -> Result<String> {
        postgresql::get_drop_sequence_sql(self, sequence)
    }

    fn get_drop_foreign_key_sql(
        &self,
        foreign_key: &ForeignKeyConstraint,
        table_name: &Identifier,
    ) -> Result<String> {
        postgresql::get_drop_foreign_key_sql(self, foreign_key, table_name)
    }

    fn _get_create_table_sql(
        &self,
        name: &Identifier,
        columns: &[ColumnData],
        options: &TableOptions,
    ) -> Result<Vec<String>>
    where
        Self: Sized,
    {
        postgresql::_get_create_table_sql(self, name, columns, options)
    }

    fn convert_boolean(&self, item: Value) -> Result<Value> {
        postgresql::convert_boolean(item)
    }

    fn convert_from_boolean(&self, item: &Value) -> Value {
        postgresql::convert_from_boolean(item)
    }

    fn get_sequence_next_val_sql(&self, sequence: &str) -> Result<String> {
        postgresql::get_sequence_next_val_sql(sequence)
    }

    fn get_set_transaction_isolation_sql(
        &self,
        level: TransactionIsolationLevel,
    ) -> Result<String> {
        postgresql::get_set_transaction_isolation_sql(self, level)
    }

    fn get_boolean_type_declaration_sql(&self, _: &ColumnData) -> Result<String> {
        postgresql::get_boolean_type_declaration_sql()
    }

    fn get_integer_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        postgresql::get_integer_type_declaration_sql(column)
    }

    fn get_bigint_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        postgresql::get_bigint_type_declaration_sql(column)
    }

    fn get_smallint_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        postgresql::get_smallint_type_declaration_sql(column)
    }

    fn get_guid_type_declaration_sql(&self, _: &ColumnData) -> Result<String> {
        postgresql::get_guid_type_declaration_sql()
    }

    fn get_date_time_type_declaration_sql(&self, _: &ColumnData) -> Result<String> {
        postgresql::get_date_time_type_declaration_sql()
    }

    fn get_date_time_tz_type_declaration_sql(&self, _: &ColumnData) -> Result<String> {
        postgresql::get_date_time_tz_type_declaration_sql()
    }

    fn get_date_type_declaration_sql(&self, _: &ColumnData) -> Result<String> {
        postgresql::get_date_type_declaration_sql()
    }

    fn get_time_type_declaration_sql(&self, _: &ColumnData) -> Result<String> {
        postgresql::get_time_type_declaration_sql()
    }

    fn get_varchar_type_declaration_sql_snippet(
        &self,
        length: Option<usize>,
        fixed: bool,
    ) -> Result<String> {
        postgresql::get_varchar_type_declaration_sql_snippet(length, fixed)
    }

    fn get_binary_type_declaration_sql_snippet(&self, _: Option<usize>, _: bool) -> Result<String> {
        postgresql::get_binary_type_declaration_sql_snippet()
    }

    fn get_clob_type_declaration_sql(&self, _: &ColumnData) -> Result<String> {
        postgresql::get_clob_type_declaration_sql()
    }

    fn get_name(&self) -> String {
        "postgresql".to_string()
    }

    fn get_date_time_tz_format_string(&self) -> &str {
        postgresql::get_date_time_tz_format_string()
    }

    fn get_empty_identity_insert_sql(
        &self,
        quoted_table_name: &str,
        quoted_identifier_column_name: &str,
    ) -> String {
        postgresql::get_empty_identity_insert_sql(quoted_table_name, quoted_identifier_column_name)
    }

    fn get_truncate_table_sql(&self, table_name: &Identifier, cascade: bool) -> String {
        postgresql::get_truncate_table_sql(self, table_name, cascade)
    }

    fn get_read_lock_sql(&self) -> Result<String> {
        postgresql::get_read_lock_sql()
    }

    fn _initialize_type_mappings(&self) {
        todo!()
        /*

        $this->doctrineTypeMapping = [
            'bigint'           => 'bigint',
            'bigserial'        => 'bigint',
            'bool'             => 'boolean',
            'boolean'          => 'boolean',
            'bpchar'           => 'string',
            'bytea'            => 'blob',
            'char'             => 'string',
            'date'             => 'date',
            'datetime'         => 'datetime',
            'decimal'          => 'decimal',
            'double'           => 'float',
            'double precision' => 'float',
            'float'            => 'float',
            'float4'           => 'float',
            'float8'           => 'float',
            'inet'             => 'string',
            'int'              => 'integer',
            'int2'             => 'smallint',
            'int4'             => 'integer',
            'int8'             => 'bigint',
            'integer'          => 'integer',
            'interval'         => 'string',
            'json'             => 'json',
            'jsonb'            => 'json',
            'money'            => 'decimal',
            'numeric'          => 'decimal',
            'serial'           => 'integer',
            'serial4'          => 'integer',
            'serial8'          => 'bigint',
            'real'             => 'float',
            'smallint'         => 'smallint',
            'text'             => 'text',
            'time'             => 'time',
            'timestamp'        => 'datetime',
            'timestamptz'      => 'datetimetz',
            'timetz'           => 'time',
            'tsvector'         => 'text',
            'uuid'             => 'guid',
            'varchar'          => 'string',
            'year'             => 'date',
            '_varchar'         => 'string',
        ];
         */
    }

    fn has_native_json_type(&self) -> bool {
        true
    }

    fn get_blob_type_declaration_sql(&self, _: &ColumnData) -> Result<String> {
        postgresql::get_blob_type_declaration_sql()
    }

    fn get_default_value_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        postgresql::get_default_value_declaration_sql(self, column)
    }

    fn supports_column_collation(&self) -> bool {
        true
    }

    fn get_column_collation_declaration_sql(&self, collation: &str) -> String {
        postgresql::get_column_collation_declaration_sql(self, collation)
    }

    fn get_json_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        postgresql::get_json_type_declaration_sql(column)
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

    fn create_reserved_keywords_list(&self) -> KeywordList {
        todo!()
    }
}

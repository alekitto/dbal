use super::postgresql;
use crate::driver::postgres::platform::PostgreSQLSchemaManager;
use crate::platform::{platform_debug, DatabasePlatform, DateIntervalUnit, KeywordList};
use crate::r#type::{
    BigintType, BlobType, BooleanType, DateTimeType, DateTimeTzType, DateType, DecimalType,
    FloatType, GuidType, IntegerType, JsonType, StringType, TextType, TimeType,
};
use crate::schema::{ColumnData, SchemaManager};
use crate::{Connection, Error, EventDispatcher, Result, TransactionIsolationLevel, Value};
use dashmap::DashMap;
use std::any::TypeId;
use std::sync::Arc;

pub trait AbstractPostgreSQLPlatform: DatabasePlatform {}

platform_debug!(PostgreSQLPlatform);
pub struct PostgreSQLPlatform {
    ev: Arc<EventDispatcher>,
    type_mappings: DashMap<String, TypeId>,
}

impl PostgreSQLPlatform {
    pub fn new(ev: Arc<EventDispatcher>) -> Self {
        let pl = Self {
            ev,
            type_mappings: DashMap::default(),
        };

        pl.initialize_all_type_mappings()
            .expect("unable to initialize type mappings");
        pl
    }
}

impl AbstractPostgreSQLPlatform for PostgreSQLPlatform {}

impl DatabasePlatform for PostgreSQLPlatform {
    fn get_event_manager(&self) -> Arc<EventDispatcher> {
        self.ev.clone()
    }

    fn as_dyn(&self) -> &dyn DatabasePlatform {
        self
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

    fn convert_boolean(&self, item: Value) -> Result<Value> {
        postgresql::convert_boolean(item)
    }

    fn convert_from_boolean(&self, item: &Value) -> Value {
        postgresql::convert_from_boolean(item)
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

    fn get_read_lock_sql(&self) -> Result<String> {
        postgresql::get_read_lock_sql()
    }

    fn _initialize_type_mappings(&self) {
        self._add_type_mapping("bigint", TypeId::of::<BigintType>());
        self._add_type_mapping("bigserial", TypeId::of::<BigintType>());
        self._add_type_mapping("bool", TypeId::of::<BooleanType>());
        self._add_type_mapping("boolean", TypeId::of::<BooleanType>());
        self._add_type_mapping("bpchar", TypeId::of::<StringType>());
        self._add_type_mapping("bytea", TypeId::of::<BlobType>());
        self._add_type_mapping("char", TypeId::of::<StringType>());
        self._add_type_mapping("date", TypeId::of::<DateType>());
        self._add_type_mapping("datetime", TypeId::of::<DateTimeType>());
        self._add_type_mapping("decimal", TypeId::of::<DecimalType>());
        self._add_type_mapping("double", TypeId::of::<FloatType>());
        self._add_type_mapping("double precision", TypeId::of::<FloatType>());
        self._add_type_mapping("float", TypeId::of::<FloatType>());
        self._add_type_mapping("float4", TypeId::of::<FloatType>());
        self._add_type_mapping("float8", TypeId::of::<FloatType>());
        self._add_type_mapping("inet", TypeId::of::<StringType>());
        self._add_type_mapping("int", TypeId::of::<IntegerType>());
        self._add_type_mapping("int2", TypeId::of::<IntegerType>());
        self._add_type_mapping("int4", TypeId::of::<IntegerType>());
        self._add_type_mapping("int8", TypeId::of::<IntegerType>());
        self._add_type_mapping("integer", TypeId::of::<IntegerType>());
        self._add_type_mapping("interval", TypeId::of::<StringType>());
        self._add_type_mapping("json", TypeId::of::<JsonType>());
        self._add_type_mapping("jsonb", TypeId::of::<JsonType>());
        self._add_type_mapping("money", TypeId::of::<DecimalType>());
        self._add_type_mapping("numeric", TypeId::of::<DecimalType>());
        self._add_type_mapping("serial", TypeId::of::<IntegerType>());
        self._add_type_mapping("serial4", TypeId::of::<IntegerType>());
        self._add_type_mapping("serial8", TypeId::of::<IntegerType>());
        self._add_type_mapping("real", TypeId::of::<FloatType>());
        self._add_type_mapping("smallint", TypeId::of::<IntegerType>());
        self._add_type_mapping("text", TypeId::of::<TextType>());
        self._add_type_mapping("time", TypeId::of::<TimeType>());
        self._add_type_mapping("timestamp", TypeId::of::<DateTimeType>());
        self._add_type_mapping("timestamptz", TypeId::of::<DateTimeTzType>());
        self._add_type_mapping("timetz", TypeId::of::<TimeType>());
        self._add_type_mapping("tsvector", TypeId::of::<TextType>());
        self._add_type_mapping("uuid", TypeId::of::<GuidType>());
        self._add_type_mapping("varchar", TypeId::of::<StringType>());
        self._add_type_mapping("year", TypeId::of::<DateType>());
        self._add_type_mapping("_varchar", TypeId::of::<StringType>());
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
        KeywordList::postgres_keywords()
    }

    fn create_schema_manager<'a>(&self, connection: &'a Connection) -> Box<dyn SchemaManager + 'a> {
        Box::new(PostgreSQLSchemaManager::new(connection))
    }
}

#[cfg(test)]
mod tests {
    use crate::driver::postgres::PostgreSQLPlatform;
    use crate::platform::DatabasePlatform;
    use crate::r#type::{BINARY, GUID, JSON};
    use crate::schema::Column;
    use crate::tests::common_platform_tests;
    use crate::EventDispatcher;
    use std::sync::Arc;

    pub fn create_postgresql_platform() -> PostgreSQLPlatform {
        PostgreSQLPlatform::new(Arc::new(EventDispatcher::new()))
    }

    #[test]
    pub fn quote_identifier() {
        let platform = create_postgresql_platform();
        let c = '"';

        assert_eq!(platform.quote_identifier("test"), format!("{}test{}", c, c));
        assert_eq!(
            platform.quote_identifier("test.test"),
            format!("{}test{}.{}test{}", c, c, c, c)
        );
        assert_eq!(
            platform.quote_identifier(&c.to_string()),
            format!("{}{}{}{}", c, c, c, c)
        );
    }

    #[test]
    pub fn quote_single_identifier() {
        let platform = create_postgresql_platform();
        let c = '"';

        assert_eq!(
            platform.quote_single_identifier("test"),
            format!("{}test{}", c, c)
        );
        assert_eq!(
            platform.quote_single_identifier("test.test"),
            format!("{}test.test{}", c, c)
        );
        assert_eq!(
            platform.quote_single_identifier(&c.to_string()),
            format!("{}{}{}{}", c, c, c, c)
        );
    }

    common_platform_tests!(create_postgresql_platform());

    #[test]
    pub fn returns_binary_type_declaration_sql() {
        let platform = create_postgresql_platform();
        let mut column = Column::new("foo", BINARY).unwrap();
        assert_eq!(
            platform
                .get_binary_type_declaration_sql(&column.generate_column_data(&platform))
                .unwrap(),
            "BYTEA"
        );

        column.set_length(0);
        assert_eq!(
            platform
                .get_binary_type_declaration_sql(&column.generate_column_data(&platform))
                .unwrap(),
            "BYTEA"
        );

        column.set_length(999999);
        assert_eq!(
            platform
                .get_binary_type_declaration_sql(&column.generate_column_data(&platform))
                .unwrap(),
            "BYTEA"
        );

        column.set_length(None);
        column.set_fixed(true);
        assert_eq!(
            platform
                .get_binary_type_declaration_sql(&column.generate_column_data(&platform))
                .unwrap(),
            "BYTEA"
        );

        column.set_length(0);
        assert_eq!(
            platform
                .get_binary_type_declaration_sql(&column.generate_column_data(&platform))
                .unwrap(),
            "BYTEA"
        );

        column.set_length(999999);
        assert_eq!(
            platform
                .get_binary_type_declaration_sql(&column.generate_column_data(&platform))
                .unwrap(),
            "BYTEA"
        );
    }

    #[test]
    pub fn returns_json_type_declaration_sql() {
        let mut column = Column::new("foo", JSON).unwrap();
        column.set_notnull(true);
        column.set_length(666);

        let platform = create_postgresql_platform();
        assert_eq!(
            platform
                .get_json_type_declaration_sql(&column.generate_column_data(&platform))
                .unwrap(),
            "JSON"
        );
    }

    #[test]
    pub fn returns_guid_type_declaration_sql() {
        let platform = create_postgresql_platform();
        let column = Column::new("foo", GUID).unwrap();

        assert_eq!(
            platform
                .get_guid_type_declaration_sql(&column.generate_column_data(&platform))
                .unwrap(),
            "UUID"
        );
    }
}

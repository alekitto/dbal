use super::sqlite;
use crate::driver::sqlite::SQLiteSchemaManager;
use crate::platform::{platform_debug, DatabasePlatform, DateIntervalUnit, KeywordList, TrimMode};
use crate::r#type::{
    BigintType, BinaryType, BlobType, BooleanType, DateTimeType, DateType, DecimalType, FloatType,
    IntegerType, StringType, TextType, TimeType,
};
use crate::schema::{ColumnData, SchemaManager};
use crate::{Connection, Error, EventDispatcher, Result, TransactionIsolationLevel};
use dashmap::DashMap;
use std::any::TypeId;
use std::sync::Arc;

pub trait AbstractSQLitePlatform: DatabasePlatform {}
impl AbstractSQLitePlatform for SQLitePlatform {}

platform_debug!(SQLitePlatform);
pub struct SQLitePlatform {
    ev: Arc<EventDispatcher>,
    type_mappings: DashMap<String, TypeId>,
}

impl SQLitePlatform {
    pub fn new(ev: Arc<EventDispatcher>) -> Self {
        let pl = Self {
            ev,
            type_mappings: DashMap::new(),
        };

        pl.initialize_all_type_mappings()
            .expect("unable to initialize type mappings");
        pl
    }
}

impl DatabasePlatform for SQLitePlatform {
    fn get_event_manager(&self) -> Arc<EventDispatcher> {
        self.ev.clone()
    }

    fn as_dyn(&self) -> &dyn DatabasePlatform {
        self
    }

    fn get_regexp_expression(&self) -> Result<String> {
        sqlite::get_regexp_expression()
    }

    fn get_trim_expression(
        &self,
        str: &str,
        mode: TrimMode,
        char: Option<String>,
    ) -> Result<String> {
        sqlite::get_trim_expression(str, mode, char)
    }

    fn get_substring_expression(
        &self,
        string: &str,
        start: usize,
        length: Option<usize>,
    ) -> Result<String> {
        sqlite::get_substring_expression(string, start, length)
    }

    fn get_locate_expression(
        &self,
        str: &str,
        substr: &str,
        start_pos: Option<usize>,
    ) -> Result<String> {
        sqlite::get_locate_expression(str, substr, start_pos)
    }

    fn get_date_arithmetic_interval_expression(
        &self,
        date: &str,
        operator: &str,
        interval: i64,
        unit: DateIntervalUnit,
    ) -> Result<String> {
        sqlite::get_date_arithmetic_interval_expression(date, operator, interval, unit)
    }

    fn get_date_diff_expression(&self, date1: &str, date2: &str) -> Result<String> {
        sqlite::get_date_diff_expression(date1, date2)
    }

    /// Multiple databases are not supported on SQLite platform.
    /// Return a fixed string as an indicator of an implicitly selected database.
    fn get_current_database_expression(&self) -> String {
        "'main'".to_string()
    }

    fn get_transaction_isolation_level_sql(&self, level: TransactionIsolationLevel) -> String {
        sqlite::get_transaction_isolation_level_sql(level)
    }

    fn get_set_transaction_isolation_sql(
        &self,
        level: TransactionIsolationLevel,
    ) -> Result<String> {
        sqlite::get_set_transaction_isolation_sql(self, level)
    }

    fn get_boolean_type_declaration_sql(&self, _: &ColumnData) -> Result<String> {
        sqlite::get_boolean_type_declaration_sql()
    }

    fn get_integer_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        sqlite::get_integer_type_declaration_sql(column)
    }

    fn get_bigint_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        sqlite::get_bigint_type_declaration_sql(self, column)
    }

    fn get_smallint_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        sqlite::get_smallint_type_declaration_sql(self, column)
    }

    fn get_date_time_type_declaration_sql(&self, _: &ColumnData) -> Result<String> {
        sqlite::get_date_time_type_declaration_sql()
    }

    fn get_date_type_declaration_sql(&self, _: &ColumnData) -> Result<String> {
        sqlite::get_date_type_declaration_sql()
    }

    fn get_time_type_declaration_sql(&self, _: &ColumnData) -> Result<String> {
        sqlite::get_time_type_declaration_sql()
    }

    fn get_varchar_type_declaration_sql_snippet(
        &self,
        length: Option<usize>,
        fixed: bool,
    ) -> Result<String> {
        sqlite::get_varchar_type_declaration_sql_snippet(length, fixed)
    }

    fn get_binary_type_declaration_sql_snippet(&self, _: Option<usize>, _: bool) -> Result<String> {
        sqlite::get_binary_type_declaration_sql_snippet()
    }

    fn get_clob_type_declaration_sql(&self, _: &ColumnData) -> Result<String> {
        sqlite::get_clob_type_declaration_sql()
    }

    fn supports_create_drop_database(&self) -> bool {
        false
    }

    fn supports_identity_columns(&self) -> bool {
        true
    }

    fn supports_column_collation(&self) -> bool {
        true
    }

    fn get_name(&self) -> String {
        "sqlite".to_string()
    }

    fn get_for_update_sql(&self) -> Result<String> {
        sqlite::get_for_update_sql()
    }

    fn _initialize_type_mappings(&self) {
        self._add_type_mapping("bigint", TypeId::of::<BigintType>());
        self._add_type_mapping("bigserial", TypeId::of::<BigintType>());
        self._add_type_mapping("blob", TypeId::of::<BlobType>());
        self._add_type_mapping("boolean", TypeId::of::<BooleanType>());
        self._add_type_mapping("char", TypeId::of::<StringType>());
        self._add_type_mapping("clob", TypeId::of::<TextType>());
        self._add_type_mapping("date", TypeId::of::<DateType>());
        self._add_type_mapping("datetime", TypeId::of::<DateTimeType>());
        self._add_type_mapping("decimal", TypeId::of::<DecimalType>());
        self._add_type_mapping("double", TypeId::of::<FloatType>());
        self._add_type_mapping("double precision", TypeId::of::<FloatType>());
        self._add_type_mapping("float", TypeId::of::<FloatType>());
        self._add_type_mapping("image", TypeId::of::<BinaryType>());
        self._add_type_mapping("int", TypeId::of::<IntegerType>());
        self._add_type_mapping("integer", TypeId::of::<IntegerType>());
        self._add_type_mapping("longtext", TypeId::of::<TextType>());
        self._add_type_mapping("longvarchar", TypeId::of::<StringType>());
        self._add_type_mapping("mediumint", TypeId::of::<IntegerType>());
        self._add_type_mapping("mediumtext", TypeId::of::<TextType>());
        self._add_type_mapping("ntext", TypeId::of::<StringType>());
        self._add_type_mapping("numeric", TypeId::of::<DecimalType>());
        self._add_type_mapping("nvarchar", TypeId::of::<StringType>());
        self._add_type_mapping("real", TypeId::of::<FloatType>());
        self._add_type_mapping("serial", TypeId::of::<IntegerType>());
        self._add_type_mapping("smallint", TypeId::of::<IntegerType>());
        self._add_type_mapping("text", TypeId::of::<TextType>());
        self._add_type_mapping("time", TypeId::of::<TimeType>());
        self._add_type_mapping("timestamp", TypeId::of::<DateTimeType>());
        self._add_type_mapping("tinyint", TypeId::of::<BooleanType>());
        self._add_type_mapping("tinytext", TypeId::of::<TextType>());
        self._add_type_mapping("varchar", TypeId::of::<StringType>());
        self._add_type_mapping("varchar2", TypeId::of::<StringType>());
    }

    fn modify_limit_query(
        &self,
        query: &str,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> String {
        sqlite::modify_limit_query(query, limit, offset)
    }

    fn get_blob_type_declaration_sql(&self, _: &ColumnData) -> Result<String> {
        sqlite::get_blob_type_declaration_sql()
    }

    fn get_temporary_table_name(&self, table_name: &str) -> Result<String> {
        sqlite::get_temporary_table_name(table_name)
    }

    fn create_reserved_keywords_list(&self) -> KeywordList {
        KeywordList::sqlite_keywords()
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

    fn create_schema_manager<'a>(&self, connection: &'a Connection) -> Box<dyn SchemaManager + 'a> {
        Box::new(SQLiteSchemaManager::new(connection))
    }
}

#[cfg(test)]
mod tests {
    use crate::driver::sqlite::SQLitePlatform;
    use crate::platform::DatabasePlatform;
    use crate::r#type::{BINARY, GUID, JSON};
    use crate::schema::Column;
    use crate::tests::common_platform_tests;
    use crate::EventDispatcher;
    use crate::Result;
    use std::sync::Arc;

    fn create_sqlite_platform() -> SQLitePlatform {
        SQLitePlatform::new(Arc::new(EventDispatcher::new()))
    }

    #[test]
    pub fn quote_identifier() {
        let platform = create_sqlite_platform();
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
        let platform = create_sqlite_platform();
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

    common_platform_tests!(create_sqlite_platform());

    #[test]
    pub fn returns_binary_type_declaration_sql() -> Result<()> {
        use crate::r#type::IntoType;

        let platform = create_sqlite_platform();
        let mut column = Column::new("foo", BINARY.into_type()?);
        assert_eq!(
            platform.get_binary_type_declaration_sql(&column.generate_column_data(&platform))?,
            "BLOB"
        );

        column.set_length(0);
        assert_eq!(
            platform.get_binary_type_declaration_sql(&column.generate_column_data(&platform))?,
            "BLOB"
        );

        column.set_length(999999);
        assert_eq!(
            platform.get_binary_type_declaration_sql(&column.generate_column_data(&platform))?,
            "BLOB"
        );

        column.set_length(None);
        column.set_fixed(true);
        assert_eq!(
            platform.get_binary_type_declaration_sql(&column.generate_column_data(&platform))?,
            "BLOB"
        );

        column.set_length(0);
        assert_eq!(
            platform.get_binary_type_declaration_sql(&column.generate_column_data(&platform))?,
            "BLOB"
        );

        column.set_length(65535);
        assert_eq!(
            platform.get_binary_type_declaration_sql(&column.generate_column_data(&platform))?,
            "BLOB"
        );

        Ok(())
    }

    #[test]
    pub fn returns_json_type_declaration_sql() -> Result<()> {
        use crate::r#type::IntoType;
        let mut column = Column::new("foo", JSON.into_type()?);
        column.set_notnull(true);
        column.set_length(666);

        let platform = create_sqlite_platform();
        assert_eq!(
            platform.get_json_type_declaration_sql(&column.generate_column_data(&platform))?,
            "CLOB"
        );

        Ok(())
    }

    #[test]
    pub fn returns_guid_type_declaration_sql() -> Result<()> {
        use crate::r#type::IntoType;
        let platform = create_sqlite_platform();
        let column = Column::new("foo", GUID.into_type()?);

        assert_eq!(
            platform.get_guid_type_declaration_sql(&column.generate_column_data(&platform))?,
            "CHAR(36)"
        );

        Ok(())
    }
}

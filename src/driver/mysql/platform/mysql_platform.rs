use super::mysql;
use crate::driver::mysql::MySQLSchemaManager;
use crate::driver::mysql::platform::{MySQLVariant, mariadb};
use crate::platform::{DatabasePlatform, DateIntervalUnit, KeywordList, platform_debug};
use crate::schema::{ColumnData, SchemaManager};
use crate::r#type::{
    BigintType, BinaryType, BlobType, BooleanType, DateTimeType, DateType, DecimalType, FloatType,
    IntegerType, JsonType, SimpleArrayType, StringType, TextType, TimeType,
};
use crate::{Connection, Error};
use crate::{EventDispatcher, Result, TransactionIsolationLevel};
use dashmap::DashMap;
use std::any::TypeId;
use std::sync::Arc;

pub const LENGTH_LIMIT_TINYTEXT: usize = 255;
pub const LENGTH_LIMIT_TEXT: usize = 65535;
pub const LENGTH_LIMIT_MEDIUMTEXT: usize = 16777215;
pub const LENGTH_LIMIT_LONGTEXT: usize = 4294967295;

pub const LENGTH_LIMIT_TINYBLOB: usize = 255;
pub const LENGTH_LIMIT_BLOB: usize = 65535;
pub const LENGTH_LIMIT_MEDIUMBLOB: usize = 16777215;
pub const LENGTH_LIMIT_LONGBLOB: usize = 4294967295;

pub trait AbstractMySQLPlatform: DatabasePlatform {}

platform_debug!(MySQLPlatform);
pub struct MySQLPlatform {
    variant: MySQLVariant,
    ev: Arc<EventDispatcher>,
    type_mappings: DashMap<String, TypeId>,
}

impl MySQLPlatform {
    pub fn new(variant: MySQLVariant, ev: Arc<EventDispatcher>) -> Self {
        let pl = Self {
            variant,
            ev,
            type_mappings: DashMap::default(),
        };

        pl.initialize_all_type_mappings()
            .expect("unable to initialize type mappings");
        pl
    }
}

impl AbstractMySQLPlatform for MySQLPlatform {}

impl DatabasePlatform for MySQLPlatform {
    fn get_event_manager(&self) -> Arc<EventDispatcher> {
        self.ev.clone()
    }

    fn as_dyn(&self) -> &dyn DatabasePlatform {
        self
    }

    fn get_boolean_type_declaration_sql(&self, _: &ColumnData) -> Result<String> {
        mysql::get_boolean_type_declaration_sql()
    }

    fn get_json_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        match self.variant {
            MySQLVariant::MariaDB => mariadb::get_json_type_declaration_sql(),
            MySQLVariant::MySQL5_6 => mysql::get_clob_type_declaration_sql(column),
            _ => mysql::get_json_type_declaration_sql(),
        }
    }

    fn get_integer_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        mysql::get_integer_type_declaration_sql(column)
    }

    fn get_bigint_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        mysql::get_bigint_type_declaration_sql(column)
    }

    fn get_smallint_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        mysql::get_smallint_type_declaration_sql(column)
    }

    fn get_varchar_type_declaration_sql_snippet(
        &self,
        length: Option<usize>,
        fixed: bool,
    ) -> Result<String> {
        mysql::get_varchar_type_declaration_sql_snippet(length, fixed)
    }

    fn get_binary_type_declaration_sql_snippet(
        &self,
        length: Option<usize>,
        fixed: bool,
    ) -> Result<String> {
        mysql::get_binary_type_declaration_sql_snippet(length, fixed)
    }

    /// Gets the SQL snippet used to declare a CLOB column type.
    ///   TINYTEXT   : 2 ^  8 - 1 = 255
    ///   TEXT       : 2 ^ 16 - 1 = 65535
    ///   MEDIUMTEXT : 2 ^ 24 - 1 = 16777215
    ///   LONGTEXT   : 2 ^ 32 - 1 = 4294967295
    fn get_clob_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        mysql::get_clob_type_declaration_sql(column)
    }

    fn get_blob_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        mysql::get_blob_type_declaration_sql(column)
    }

    fn get_name(&self) -> String {
        "mysql".to_string()
    }

    fn get_regexp_expression(&self) -> Result<String> {
        mysql::get_regexp_expression()
    }

    fn get_length_expression(&self, column: &str) -> Result<String> {
        mysql::get_length_expression(column)
    }

    fn get_concat_expression(&self, strings: Vec<&str>) -> Result<String> {
        mysql::get_concat_expression(strings)
    }

    fn get_date_diff_expression(&self, date1: &str, date2: &str) -> Result<String> {
        mysql::get_date_diff_expression(date1, date2)
    }

    fn get_date_arithmetic_interval_expression(
        &self,
        date: &str,
        operator: &str,
        interval: i64,
        unit: DateIntervalUnit,
    ) -> Result<String> {
        mysql::get_date_arithmetic_interval_expression(date, operator, interval, unit)
    }

    fn get_current_database_expression(&self) -> String {
        mysql::get_current_database_expression()
    }

    fn get_read_lock_sql(&self) -> Result<String> {
        mysql::get_read_lock_sql()
    }

    fn quote_string_literal(&self, str: &str) -> String {
        mysql::quote_string_literal(self, str)
    }

    fn get_decimal_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        mysql::get_decimal_type_declaration_sql(column)
    }

    fn get_default_value_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        mysql::get_default_value_declaration_sql(self, column)
    }

    fn get_set_transaction_isolation_sql(
        &self,
        level: TransactionIsolationLevel,
    ) -> Result<String> {
        mysql::get_set_transaction_isolation_sql(self, level)
    }

    fn get_date_time_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        mysql::get_date_time_type_declaration_sql(column)
    }

    fn get_date_type_declaration_sql(&self, _: &ColumnData) -> Result<String> {
        mysql::get_date_type_declaration_sql()
    }

    fn get_time_type_declaration_sql(&self, _: &ColumnData) -> Result<String> {
        mysql::get_time_type_declaration_sql()
    }

    fn get_float_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        mysql::get_float_declaration_sql(column)
    }

    fn get_default_transaction_isolation_level(&self) -> TransactionIsolationLevel {
        mysql::get_default_transaction_isolation_level()
    }

    fn quote_single_identifier(&self, str: &str) -> String {
        mysql::quote_single_identifier(str)
    }

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
        self._add_type_mapping("tinyint", TypeId::of::<BooleanType>());
        self._add_type_mapping("tinytext", TypeId::of::<TextType>());
        self._add_type_mapping("varbinary", TypeId::of::<BinaryType>());
        self._add_type_mapping("varchar", TypeId::of::<StringType>());
        self._add_type_mapping("year", TypeId::of::<DateType>());
    }

    fn create_reserved_keywords_list(&self) -> KeywordList {
        match self.variant {
            MySQLVariant::MySQL8_0 => KeywordList::mysql80_keywords(),
            MySQLVariant::MariaDB => KeywordList::mariadb_keywords(),
            _ => KeywordList::mysql_keywords(),
        }
    }

    fn supports_identity_columns(&self) -> bool {
        true
    }

    fn supports_column_length_indexes(&self) -> bool {
        true
    }

    fn supports_inline_column_comments(&self) -> bool {
        true
    }

    fn supports_column_collation(&self) -> bool {
        true
    }

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

    fn create_schema_manager<'a>(&self, connection: &'a Connection) -> Box<dyn SchemaManager + 'a> {
        Box::new(MySQLSchemaManager::new(connection, self.variant))
    }
}

#[cfg(test)]
mod tests {
    use crate::EventDispatcher;
    use crate::Result;
    use crate::driver::mysql::MySQLPlatform;
    use crate::driver::mysql::MySQLVariant;
    use crate::platform::DatabasePlatform;
    use crate::schema::Column;
    use crate::tests::common_platform_tests;
    use crate::r#type::{BINARY, GUID, JSON};
    use std::sync::Arc;

    pub fn create_mysql_platform() -> MySQLPlatform {
        MySQLPlatform::new(MySQLVariant::MySQL5_7, Arc::new(EventDispatcher::new()))
    }

    pub fn create_mysql80_platform() -> MySQLPlatform {
        MySQLPlatform::new(MySQLVariant::MySQL8_0, Arc::new(EventDispatcher::new()))
    }

    pub fn create_mariadb_platform() -> MySQLPlatform {
        MySQLPlatform::new(MySQLVariant::MariaDB, Arc::new(EventDispatcher::new()))
    }

    #[test]
    pub fn quote_identifier() {
        let platform = create_mysql_platform();
        let c = '`';

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
        let platform = create_mysql_platform();
        let c = '`';

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

    common_platform_tests!(create_mysql_platform());

    #[test]
    pub fn returns_binary_type_declaration_sql() -> Result<()> {
        use crate::r#type::IntoType;
        let platform = create_mysql_platform();
        let mut column = Column::new("foo", BINARY.into_type()?);
        assert_eq!(
            platform.get_binary_type_declaration_sql(&column.generate_column_data(&platform))?,
            "VARBINARY(255)"
        );

        column.set_length(0);
        assert_eq!(
            platform.get_binary_type_declaration_sql(&column.generate_column_data(&platform))?,
            "VARBINARY(255)"
        );

        column.set_length(65535);
        assert_eq!(
            platform.get_binary_type_declaration_sql(&column.generate_column_data(&platform))?,
            "VARBINARY(65535)"
        );

        column.set_length(None);
        column.set_fixed(true);
        assert_eq!(
            platform.get_binary_type_declaration_sql(&column.generate_column_data(&platform))?,
            "BINARY(255)"
        );

        column.set_length(0);
        assert_eq!(
            platform.get_binary_type_declaration_sql(&column.generate_column_data(&platform))?,
            "BINARY(255)"
        );

        column.set_length(65535);
        assert_eq!(
            platform.get_binary_type_declaration_sql(&column.generate_column_data(&platform))?,
            "BINARY(65535)"
        );

        Ok(())
    }

    #[test]
    pub fn returns_json_type_declaration_sql() -> Result<()> {
        use crate::r#type::IntoType;
        let mut column = Column::new("foo", JSON.into_type()?);
        column.set_notnull(true);
        column.set_length(666);

        let platform = create_mysql_platform();
        assert_eq!(
            platform.get_json_type_declaration_sql(&column.generate_column_data(&platform))?,
            "JSON"
        );

        let platform = create_mysql80_platform();
        assert_eq!(
            platform.get_json_type_declaration_sql(&column.generate_column_data(&platform))?,
            "JSON"
        );

        let platform = create_mariadb_platform();
        assert_eq!(
            platform.get_json_type_declaration_sql(&column.generate_column_data(&platform))?,
            "LONGTEXT"
        );

        Ok(())
    }

    #[test]
    pub fn returns_guid_type_declaration_sql() -> Result<()> {
        use crate::r#type::IntoType;
        let platform = create_mysql_platform();
        let column = Column::new("foo", GUID.into_type()?);

        assert_eq!(
            platform.get_guid_type_declaration_sql(&column.generate_column_data(&platform))?,
            "CHAR(36)"
        );

        Ok(())
    }
}

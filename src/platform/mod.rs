mod create_flags;
mod date_interval_unit;
pub mod default;
mod keyword;
mod lock_mode;
mod trim_mode;

use crate::r#type::{TypeManager, TypePtr};
use crate::schema::{ColumnData, Identifier};
use crate::{Connection, Error, EventDispatcher, Result, TransactionIsolationLevel, Value};
pub use create_flags::CreateFlags;
use creed::schema::SchemaManager;
pub use date_interval_unit::DateIntervalUnit;
pub use keyword::{KeywordList, Keywords};
pub use lock_mode::LockMode;
use std::any::TypeId;
use std::fmt::{Debug, Display};
use std::sync::Arc;
pub use trim_mode::TrimMode;

pub(crate) macro platform_debug($platform:ident) {
    impl std::fmt::Debug for $platform {
        fn fmt(
            &self,
            f: &mut std::fmt::Formatter<'_>,
        ) -> core::result::Result<(), core::fmt::Error> {
            f.debug_struct(core::any::type_name::<Self>())
                .finish_non_exhaustive()
        }
    }
}

pub trait DatabasePlatform: Debug {
    /// Retrieves the event dispatcher
    fn get_event_manager(&self) -> Arc<EventDispatcher>;

    /// Load Type Mappings.
    /// # Internal - Only to be used by DatabasePlatform hierarchy
    fn _initialize_type_mappings(&self);

    /// As &dyn DatabasePlatform
    fn as_dyn(&self) -> &dyn DatabasePlatform;

    /// Load Type Mappings.
    /// # Internal - Only to be used by DatabasePlatform hierarchy
    fn _add_type_mapping(&self, db_type: &str, type_id: TypeId);

    /// Returns the SQL snippet that declares a boolean column.
    fn get_boolean_type_declaration_sql(&self, column: &ColumnData) -> Result<String>;

    /// Returns the SQL snippet that declares a 4 byte integer column.
    fn get_integer_type_declaration_sql(&self, column: &ColumnData) -> Result<String>;

    /// Returns the SQL snippet that declares a 8 byte integer column.
    fn get_bigint_type_declaration_sql(&self, column: &ColumnData) -> Result<String>;

    /// Returns the SQL snippet that declares a 2 byte integer column.
    fn get_smallint_type_declaration_sql(&self, column: &ColumnData) -> Result<String>;

    /// Returns the SQL snippet used to declare a column that can
    /// store characters in the ASCII character set.
    fn get_ascii_string_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        default::get_ascii_string_type_declaration_sql(self.as_dyn(), column)
    }

    /// Returns the SQL snippet used to declare a VARCHAR column type.
    fn get_string_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        default::get_string_type_declaration_sql(self.as_dyn(), column)
    }

    /// Returns the SQL snippet used to declare a BINARY/VARBINARY column type.
    fn get_binary_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        default::get_binary_type_declaration_sql(self.as_dyn(), column)
    }

    /// Returns the SQL snippet to declare a GUID/UUID column.
    ///
    /// By default this maps directly to a CHAR(36) and only maps to more
    /// special datatypes when the underlying databases support this datatype.
    fn get_guid_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        default::get_guid_type_declaration_sql(self.as_dyn(), column)
    }

    /// Returns the SQL snippet to declare a JSON column.
    ///
    /// By default this maps directly to a CLOB and only maps to more
    /// special datatypes when the underlying databases support this datatype.
    fn get_json_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        default::get_json_type_declaration_sql(self.as_dyn(), column)
    }

    #[allow(unused_variables)]
    fn get_varchar_type_declaration_sql_snippet(
        &self,
        length: Option<usize>,
        fixed: bool,
    ) -> Result<String> {
        Err(Error::platform_feature_unsupported(
            "VARCHARs not supported by the current platform.",
        ))
    }

    /// Returns the SQL snippet used to declare a BINARY/VARBINARY column type.
    ///
    /// # Arguments
    ///
    /// * `length` - The length of the column.
    /// * `fixed` - Whether the column length is fixed.
    #[allow(unused_variables)]
    fn get_binary_type_declaration_sql_snippet(
        &self,
        length: Option<usize>,
        fixed: bool,
    ) -> Result<String> {
        Err(Error::platform_feature_unsupported(
            "BINARY/VARBINARY column types are not supported by this platform.",
        ))
    }

    /// Returns the SQL snippet used to declare a CLOB column type.
    fn get_clob_type_declaration_sql(&self, column: &ColumnData) -> Result<String>;

    /// Returns the SQL Snippet used to declare a BLOB column type.
    fn get_blob_type_declaration_sql(&self, column: &ColumnData) -> Result<String>;

    /// Gets the name of the platform.
    fn get_name(&self) -> String;

    /// Initializes Doctrine Type Mappings with the platform defaults
    /// and with all additional type mappings.
    fn initialize_all_type_mappings(&self) -> Result<()>
    where
        Self: Sized,
    {
        let type_manager = TypeManager::get_instance();
        self._initialize_type_mappings();

        for type_id in type_manager.get_types()? {
            for db_type in type_manager
                .get_type(type_id)?
                .get_mapped_database_types(self)
            {
                self._add_type_mapping(&db_type, type_id)
            }
        }

        Ok(())
    }

    /// Registers a type to be used in conjunction with a column type of this platform.
    fn register_type_mapping(&self, db_type: &str, type_id: TypeId) -> Result<()>
    where
        Self: Sized,
    {
        let _ = TypeManager::get_instance().get_type(type_id)?;
        self._add_type_mapping(db_type, type_id);

        Ok(())
    }

    /// Gets the type that is mapped for the given database column type.
    fn get_type_mapping(&self, db_type: &str) -> Result<TypeId>;

    /// Checks if a database type is currently supported by this platform.
    fn has_type_mapping_for(&self, db_type: &str) -> bool {
        self.get_type_mapping(db_type).is_ok()
    }

    /// Returns the regular expression operator.
    fn get_regexp_expression(&self) -> Result<String> {
        Err(Error::platform_feature_unsupported(
            "REGEXP expressions are not supported by this platform.",
        ))
    }

    /// Returns the SQL snippet to get the length of a text column in characters.
    fn get_length_expression(&self, column: &str) -> Result<String> {
        default::get_length_expression(column)
    }

    /// Returns the SQL snippet to get the remainder of the division operation $expression1 / $expression2.
    fn get_mod_expression(&self, expression1: &str, expression2: &str) -> Result<String> {
        default::get_mod_expression(expression1, expression2)
    }

    /// Returns the SQL snippet to trim a string.
    ///
    /// # Arguments
    ///
    /// * `str` - The expression to apply the trim to.
    /// * `mode` - The position of the trim (leading/trailing/both).
    /// * `char` - The char to trim, has to be quoted already. Defaults to space.
    fn get_trim_expression(
        &self,
        str: &str,
        mode: TrimMode,
        char: Option<String>,
    ) -> Result<String> {
        default::get_trim_expression(str, mode, char)
    }

    /// Returns the SQL snippet to get the position of the first occurrence of substring $substr in string `str`.
    ///
    /// # Arguments
    ///
    /// * 'str' - Literal string.
    /// * 'substr' - Literal string to find.
    /// * 'start_pos' - Position to start at, beginning of string by default.
    #[allow(unused_variables)]
    fn get_locate_expression(
        &self,
        str: &str,
        substr: &str,
        start_pos: Option<usize>,
    ) -> Result<String> {
        Err(Error::platform_feature_unsupported(
            "locate expressions are not supported by this platform",
        ))
    }

    /// Returns a SQL snippet to get a substring inside an SQL statement.
    ///
    /// Note: Not SQL92, but common functionality.
    ///
    /// SQLite only supports the 2 parameter variant of this function.
    ///
    /// # Arguments
    ///
    /// * `string` - An sql string literal or column name/alias.
    /// * `start` - Where to start the substring portion.
    /// * `length` - The substring portion length.
    fn get_substring_expression(
        &self,
        string: &str,
        start: usize,
        length: Option<usize>,
    ) -> Result<String> {
        default::get_substring_expression(string, start, length)
    }

    /// Returns a SQL snippet to concatenate the given expressions.
    fn get_concat_expression(&self, strings: Vec<&str>) -> Result<String> {
        default::get_concat_expression(strings)
    }

    /// Returns the SQL to calculate the difference in days between the two passed dates.
    /// Computes diff = date1 - date2.
    #[allow(unused_variables)]
    fn get_date_diff_expression(&self, date1: &str, date2: &str) -> Result<String> {
        Err(Error::platform_feature_unsupported(
            "date diff expressions are not supported by this platform.",
        ))
    }

    /// Returns the SQL to add the number of given seconds to a date.
    fn get_date_add_seconds_expression(&self, date: &str, seconds: i64) -> Result<String> {
        default::get_date_add_seconds_expression(self.as_dyn(), date, seconds)
    }

    /// Returns the SQL to subtract the number of given seconds from a date.
    fn get_date_sub_seconds_expression(&self, date: &str, seconds: i64) -> Result<String> {
        default::get_date_sub_seconds_expression(self.as_dyn(), date, seconds)
    }

    /// Returns the SQL to add the number of given minutes to a date.
    fn get_date_add_minutes_expression(&self, date: &str, minutes: i64) -> Result<String> {
        default::get_date_add_minutes_expression(self.as_dyn(), date, minutes)
    }

    /// Returns the SQL to subtract the number of given minutes from a date.
    fn get_date_sub_minutes_expression(&self, date: &str, minutes: i64) -> Result<String> {
        default::get_date_sub_minutes_expression(self.as_dyn(), date, minutes)
    }

    /// Returns the SQL to add the number of given hours to a date.
    fn get_date_add_hour_expression(&self, date: &str, hours: i64) -> Result<String> {
        default::get_date_add_hour_expression(self.as_dyn(), date, hours)
    }

    /// Returns the SQL to subtract the number of given hours to a date.
    fn get_date_sub_hour_expression(&self, date: &str, hours: i64) -> Result<String> {
        default::get_date_sub_hour_expression(self.as_dyn(), date, hours)
    }

    /// Returns the SQL to add the number of given days to a date.
    fn get_date_add_days_expression(&self, date: &str, days: i64) -> Result<String> {
        default::get_date_add_days_expression(self.as_dyn(), date, days)
    }

    /// Returns the SQL to subtract the number of given days to a date.
    fn get_date_sub_days_expression(&self, date: &str, days: i64) -> Result<String> {
        default::get_date_sub_days_expression(self.as_dyn(), date, days)
    }

    /// Returns the SQL to add the number of given weeks to a date.
    fn get_date_add_weeks_expression(&self, date: &str, weeks: i64) -> Result<String> {
        default::get_date_add_weeks_expression(self.as_dyn(), date, weeks)
    }

    /// Returns the SQL to subtract the number of given weeks from a date.
    fn get_date_sub_weeks_expression(&self, date: &str, weeks: i64) -> Result<String> {
        default::get_date_sub_weeks_expression(self.as_dyn(), date, weeks)
    }

    /// Returns the SQL to add the number of given months to a date.
    fn get_date_add_month_expression(&self, date: &str, months: i64) -> Result<String> {
        default::get_date_add_month_expression(self.as_dyn(), date, months)
    }

    /// Returns the SQL to subtract the number of given months to a date.
    fn get_date_sub_month_expression(&self, date: &str, months: i64) -> Result<String> {
        default::get_date_sub_month_expression(self.as_dyn(), date, months)
    }

    /// Returns the SQL to add the number of given quarters to a date.
    fn get_date_add_quarters_expression(&self, date: &str, quarters: i64) -> Result<String> {
        default::get_date_add_quarters_expression(self.as_dyn(), date, quarters)
    }

    /// Returns the SQL to subtract the number of given quarters from a date.
    fn get_date_sub_quarters_expression(&self, date: &str, quarters: i64) -> Result<String> {
        default::get_date_sub_quarters_expression(self.as_dyn(), date, quarters)
    }

    /// Returns the SQL to add the number of given years to a date.
    fn get_date_add_years_expression(&self, date: &str, years: i64) -> Result<String> {
        default::get_date_add_years_expression(self.as_dyn(), date, years)
    }

    /// Returns the SQL to subtract the number of given years from a date.
    fn get_date_sub_years_expression(&self, date: &str, years: i64) -> Result<String> {
        default::get_date_sub_years_expression(self.as_dyn(), date, years)
    }

    /// Returns the SQL for a date arithmetic expression.
    ///
    /// # Arguments
    ///
    /// * `date` - The column or literal representing a date to perform the arithmetic operation on.
    /// * `operator` - The arithmetic operator (+ or -).
    /// * `interval` - The interval that shall be calculated into the date.
    /// * `unit` - The unit of the interval that shall be calculated into the date.
    ///            One of the DATE_INTERVAL_UNIT_* constants.
    #[allow(unused_variables)]
    fn get_date_arithmetic_interval_expression(
        &self,
        date: &str,
        operator: &str,
        interval: i64,
        unit: DateIntervalUnit,
    ) -> Result<String> {
        Err(Error::platform_feature_unsupported(
            "date arithmetic interval expressions are not supported by this platform.",
        ))
    }

    /// Returns the SQL bit AND comparison expression.
    fn get_bit_and_comparison_expression(
        &self,
        value1: &dyn Display,
        value2: &dyn Display,
    ) -> Result<String> {
        default::get_bit_and_comparison_expression(value1, value2)
    }

    /// Returns the SQL bit OR comparison expression.
    fn get_bit_or_comparison_expression(
        &self,
        value1: &dyn Display,
        value2: &dyn Display,
    ) -> Result<String> {
        default::get_bit_or_comparison_expression(value1, value2)
    }

    /// Returns the SQL expression which represents the currently selected database.
    fn get_current_database_expression(&self) -> String;

    /// Returns the FOR UPDATE expression.
    fn get_for_update_sql(&self) -> Result<String> {
        default::get_for_update_sql()
    }

    /// Honors that some SQL vendors such as MsSql use table hints for locking instead of the
    /// ANSI SQL FOR UPDATE specification.
    ///
    /// # Arguments
    ///
    /// * `from_clause` - The FROM clause to append the hint for the given lock mode to
    /// * `lock_mode` - One of the LockMode
    fn append_lock_hint(&self, from_clause: &str, lock_mode: LockMode) -> Result<String> {
        default::append_lock_hint(from_clause, lock_mode)
    }

    /// Returns the SQL snippet to append to any SELECT statement which locks rows in shared read lock.
    ///
    /// This defaults to the ANSI SQL "FOR UPDATE", which is an exclusive lock (Write). Some database
    /// vendors allow to lighten this constraint up to be a real read lock.
    fn get_read_lock_sql(&self) -> Result<String> {
        default::get_read_lock_sql(self.as_dyn())
    }

    /// Returns the SQL snippet to append to any SELECT statement which obtains an exclusive lock on the rows.
    ///
    /// The semantics of this lock mode should equal the SELECT .. FOR UPDATE of the ANSI SQL standard.
    fn get_write_lock_sql(&self) -> Result<String> {
        default::get_write_lock_sql(self.as_dyn())
    }

    /// Gets the comment to append to a column comment that helps parsing this type in reverse engineering.
    fn get_creed_type_comment(&self, creed_type: &TypePtr) -> String {
        default::get_creed_type_comment(creed_type)
    }

    /// Quotes a string so that it can be safely used as a table or column name,
    /// even if it is a reserved word of the platform. This also detects identifier
    /// chains separated by dot and quotes them independently.
    ///
    /// NOTE: Just because you CAN use quoted identifiers doesn't mean
    /// you SHOULD use them. In general, they end up causing way more
    /// problems than they solve.
    fn quote_identifier(&self, identifier: &str) -> String {
        default::quote_identifier(self.as_dyn(), identifier)
    }

    /// Quotes a single identifier (no dot chain separation).
    fn quote_single_identifier(&self, str: &str) -> String {
        default::quote_single_identifier(str)
    }

    /// Quotes a literal string.
    /// This method is NOT meant to fix SQL injections!
    /// It is only meant to escape this platform's string literal
    /// quote character inside the given literal string.
    fn quote_string_literal(&self, str: &str) -> String {
        default::quote_string_literal(self.as_dyn(), str)
    }

    /// Gets the character used for string literal quoting.
    fn get_string_literal_quote_character(&self) -> &str {
        default::get_string_literal_quote_character()
    }

    /// Returns the SQL snippet that declares a floating point column of arbitrary precision.
    fn get_decimal_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        default::get_decimal_type_declaration_sql(column)
    }

    /// Obtains DBMS specific SQL code portion needed to set a default value
    /// declaration to be used in statements like CREATE TABLE.
    fn get_default_value_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        default::get_default_value_declaration_sql(self.as_dyn(), column)
    }

    /// Obtains SQL code portion needed to create a custom column,
    /// e.g. when a column has the "column_definition" set.
    /// Only "AUTOINCREMENT" and "PRIMARY KEY" are added if appropriate.
    fn get_custom_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        default::get_custom_type_declaration_sql(column)
    }

    /// Some vendors require temporary table names to be qualified specially.
    fn get_temporary_table_name(&self, table_name: &str) -> Result<String> {
        default::get_temporary_table_name(table_name)
    }

    /// Obtains DBMS specific SQL code portion needed to set the CHARACTER SET
    /// of a column declaration to be used in statements like CREATE TABLE.
    /// # Internal
    #[allow(unused_variables)]
    fn get_column_charset_declaration_sql(&self, charset: &str) -> String {
        default::get_column_charset_declaration_sql()
    }

    /// Some platforms need the boolean values to be converted.
    /// The default conversion in this implementation converts to integers (false => 0, true => 1).
    ///
    /// # Note
    /// If the input is not a boolean the original input might be returned.
    ///
    /// There are two contexts when converting booleans: Literals and Prepared Statements.
    /// This method should handle the literal case
    fn convert_boolean(&self, item: Value) -> Result<Value> {
        Ok(default::convert_boolean(item))
    }

    /// Some platforms have boolean literals that needs to be correctly converted
    ///
    /// The default conversion tries to convert value into bool
    fn convert_from_boolean(&self, item: &Value) -> Value {
        default::convert_from_boolean(item)
    }

    /// This method should handle the prepared statements case. When there is no
    /// distinction, it's OK to use the same method.
    ///
    /// # Note
    /// If the input is not a boolean the original input might be returned.
    fn convert_booleans_to_database_value(&self, item: Value) -> Result<Value> {
        default::convert_booleans_to_database_value(self.as_dyn(), item)
    }

    /// Returns the SQL specific for the platform to get the current date.
    fn get_current_date_sql(&self) -> &str {
        default::get_current_date_sql()
    }

    /// Returns the SQL specific for the platform to get the current time.
    fn get_current_time_sql(&self) -> &str {
        default::get_current_time_sql()
    }

    /// Returns the SQL specific for the platform to get the current timestamp
    fn get_current_timestamp_sql(&self) -> &str {
        default::get_current_timestamp_sql()
    }

    /// Returns the SQL for a given transaction isolation level Connection constant.
    /// # Protected
    fn get_transaction_isolation_level_sql(&self, level: TransactionIsolationLevel) -> String {
        default::get_transaction_isolation_level_sql(level)
    }

    /// Returns the SQL to set the transaction isolation level.
    #[allow(unused_variables)]
    fn get_set_transaction_isolation_sql(
        &self,
        level: TransactionIsolationLevel,
    ) -> Result<String> {
        Err(Error::platform_feature_unsupported(
            "set transaction isolation",
        ))
    }

    /// Obtains DBMS specific SQL to be used to create datetime columns in
    /// statements like CREATE TABLE.
    #[allow(unused_variables)]
    fn get_date_time_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        Err(Error::platform_feature_unsupported("datetime type"))
    }

    /// Obtains DBMS specific SQL to be used to create datetime with timezone offset columns.
    fn get_date_time_tz_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        default::get_date_time_tz_type_declaration_sql(self.as_dyn(), column)
    }

    /// Obtains DBMS specific SQL to be used to create date columns in statements
    /// like CREATE TABLE.
    #[allow(unused_variables)]
    fn get_date_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        Err(Error::platform_feature_unsupported("date type"))
    }

    /// Obtains DBMS specific SQL to be used to create time columns in statements
    /// like CREATE TABLE.
    #[allow(unused_variables)]
    fn get_time_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        Err(Error::platform_feature_unsupported("time type"))
    }

    #[allow(unused_variables)]
    fn get_float_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        default::get_float_declaration_sql()
    }

    /// Gets the default transaction isolation level of the platform.
    fn get_default_transaction_isolation_level(&self) -> TransactionIsolationLevel {
        default::get_default_transaction_isolation_level()
    }

    /// Returns the keyword list instance of this platform.
    fn create_reserved_keywords_list(&self) -> KeywordList;

    /// Whether the platform supports sequences.
    fn supports_sequences(&self) -> bool {
        false
    }

    /// Whether the platform supports identity columns.
    ///
    /// Identity columns are columns that receive an auto-generated value from the
    /// database on insert of a row.
    fn supports_identity_columns(&self) -> bool {
        false
    }

    /// Whether the platform emulates identity columns through sequences.
    ///
    /// Some platforms that do not support identity columns natively
    /// but support sequences can emulate identity columns by using
    /// sequences.
    fn uses_sequence_emulated_identity_columns(&self) -> bool {
        false
    }

    /// Whether the platform supports partial indexes.
    fn supports_partial_indexes(&self) -> bool {
        false
    }

    /// Whether the platform supports indexes with column length definitions.
    fn supports_column_length_indexes(&self) -> bool {
        false
    }

    /// Whether the platform supports savepoints.
    fn supports_savepoints(&self) -> bool {
        true
    }

    /// Whether the platform supports releasing savepoints.
    fn supports_release_savepoints(&self) -> bool {
        self.supports_savepoints()
    }

    /// Whether the platform supports foreign key constraints.
    fn supports_foreign_key_constraints(&self) -> bool {
        true
    }

    /// Whether the platform supports database schemas.
    fn supports_schemas(&self) -> bool {
        false
    }

    /// Whether this platform supports create database.
    ///
    /// Some databases don't allow to create and drop databases at all or only with certain tools.
    fn supports_create_drop_database(&self) -> bool {
        true
    }

    /// Whether this platform support to add inline column comments as postfix.
    fn supports_inline_column_comments(&self) -> bool {
        false
    }

    /// Whether this platform support the proprietary syntax "COMMENT ON asset".
    fn supports_comment_on_statement(&self) -> bool {
        false
    }

    /// Does this platform have native guid type.
    fn has_native_guid_type(&self) -> bool {
        false
    }

    /// Does this platform have native JSON type.
    fn has_native_json_type(&self) -> bool {
        false
    }

    /// Does this platform support column collation?
    fn supports_column_collation(&self) -> bool {
        false
    }

    /// Gets the format string, as accepted by the date() function, that describes
    /// the format of a stored datetime value of this platform.
    fn get_date_time_format_string(&self) -> &str {
        default::get_date_time_format_string()
    }

    /// Gets the format string, as accepted by the date() function, that describes
    /// the format of a stored datetime with timezone value of this platform.
    fn get_date_time_tz_format_string(&self) -> &str {
        default::get_date_time_tz_format_string()
    }

    /// Gets the format string, as accepted by the date() function, that describes
    /// the format of a stored date value of this platform.
    fn get_date_format_string(&self) -> &str {
        default::get_date_format_string()
    }

    /// Gets the format string, as accepted by the date() function, that describes
    /// the format of a stored time value of this platform.
    fn get_time_format_string(&self) -> &str {
        default::get_time_format_string()
    }

    /// Adds an driver-specific LIMIT clause to the query.
    fn modify_limit_query(
        &self,
        query: &str,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> String {
        default::modify_limit_query(query, limit, offset)
    }

    /// Maximum length of any given database identifier, like tables or column names.
    fn get_max_identifier_length(&self) -> usize {
        default::get_max_identifier_length()
    }

    /// Returns the insert SQL for an empty insert statement.
    fn get_empty_identity_insert_sql(
        &self,
        quoted_table_name: &str,
        quoted_identifier_column_name: &str,
    ) -> String {
        default::get_empty_identity_insert_sql(quoted_table_name, quoted_identifier_column_name)
    }

    /// Generates a Truncate Table SQL statement for a given table.
    ///
    /// Cascade is not supported on many platforms but would optionally cascade the truncate by
    /// following the foreign keys.
    #[allow(unused_variables)]
    fn get_truncate_table_sql(&self, table_name: &Identifier, cascade: bool) -> String {
        default::get_truncate_table_sql(self.as_dyn(), table_name)
    }

    /// This is for test reasons, many vendors have special requirements for dummy statements.
    fn get_dummy_select_sql(&self, expression: Option<&str>) -> String {
        format!("SELECT {}", expression.unwrap_or("1"))
    }

    /// Returns the SQL to create a new savepoint.
    fn create_save_point(&self, savepoint: &str) -> String {
        default::create_save_point(savepoint)
    }

    /// Returns the SQL to release a savepoint.
    fn release_save_point(&self, savepoint: &str) -> String {
        default::release_save_point(savepoint)
    }

    /// Returns the SQL to rollback a savepoint.
    fn rollback_save_point(&self, savepoint: &str) -> String {
        default::rollback_save_point(savepoint)
    }

    /// Escapes metacharacters in a string intended to be used with a LIKE operator.
    fn escape_string_for_like(&self, input_string: &str, escape_char: &str) -> Result<String> {
        default::escape_string_for_like(self.as_dyn(), input_string, escape_char)
    }

    fn get_like_wildcard_characters(&self) -> &'static str {
        default::get_like_wildcard_characters()
    }

    fn create_schema_manager<'a>(&self, connection: &'a Connection) -> Box<dyn SchemaManager + 'a>;
}

impl<P: DatabasePlatform + ?Sized> DatabasePlatform for &mut P {
    delegate::delegate! {
        to(**self) {
            fn get_event_manager(&self) -> Arc<EventDispatcher>;
            fn as_dyn(&self) -> &dyn DatabasePlatform;
            fn _initialize_type_mappings(&self);
            fn _add_type_mapping(&self, db_type: &str, type_id: TypeId);
            fn get_boolean_type_declaration_sql(&self, column: &ColumnData) -> Result<String>;
            fn get_integer_type_declaration_sql(&self, column: &ColumnData) -> Result<String>;
            fn get_bigint_type_declaration_sql(&self, column: &ColumnData) -> Result<String>;
            fn get_smallint_type_declaration_sql(&self, column: &ColumnData) -> Result<String>;
            fn get_clob_type_declaration_sql(&self, column: &ColumnData) -> Result<String>;
            fn get_blob_type_declaration_sql(&self, column: &ColumnData) -> Result<String>;
            fn get_name(&self) -> String;
            fn get_type_mapping(&self, db_type: &str) -> Result<TypeId>;
            fn get_current_database_expression(&self) -> String;
            fn create_reserved_keywords_list(&self) -> KeywordList;
            fn create_schema_manager<'a>(&self, connection: &'a Connection) -> Box<dyn SchemaManager + 'a>;
        }
    }
}

impl<P: DatabasePlatform + ?Sized> DatabasePlatform for Box<P> {
    delegate::delegate! {
        to(**self) {
            fn get_event_manager(&self) -> Arc<EventDispatcher>;
            fn as_dyn(&self) -> &dyn DatabasePlatform;
            fn _initialize_type_mappings(&self);
            fn _add_type_mapping(&self, db_type: &str, type_id: TypeId);
            fn get_boolean_type_declaration_sql(&self, column: &ColumnData) -> Result<String>;
            fn get_integer_type_declaration_sql(&self, column: &ColumnData) -> Result<String>;
            fn get_bigint_type_declaration_sql(&self, column: &ColumnData) -> Result<String>;
            fn get_smallint_type_declaration_sql(&self, column: &ColumnData) -> Result<String>;
            fn get_clob_type_declaration_sql(&self, column: &ColumnData) -> Result<String>;
            fn get_blob_type_declaration_sql(&self, column: &ColumnData) -> Result<String>;
            fn get_name(&self) -> String;
            fn get_type_mapping(&self, db_type: &str) -> Result<TypeId>;
            fn get_current_database_expression(&self) -> String;
            fn create_reserved_keywords_list(&self) -> KeywordList;
            fn create_schema_manager<'a>(&self, connection: &'a Connection) -> Box<dyn SchemaManager + 'a>;
        }
    }
}

impl<P: DatabasePlatform + ?Sized> DatabasePlatform for Arc<Box<P>> {
    delegate::delegate! {
        to(**self) {
            fn get_event_manager(&self) -> Arc<EventDispatcher>;
            fn as_dyn(&self) -> &dyn DatabasePlatform;
            fn _initialize_type_mappings(&self);
            fn _add_type_mapping(&self, db_type: &str, type_id: TypeId);
            fn get_boolean_type_declaration_sql(&self, column: &ColumnData) -> Result<String>;
            fn get_integer_type_declaration_sql(&self, column: &ColumnData) -> Result<String>;
            fn get_bigint_type_declaration_sql(&self, column: &ColumnData) -> Result<String>;
            fn get_smallint_type_declaration_sql(&self, column: &ColumnData) -> Result<String>;
            fn get_clob_type_declaration_sql(&self, column: &ColumnData) -> Result<String>;
            fn get_blob_type_declaration_sql(&self, column: &ColumnData) -> Result<String>;
            fn get_name(&self) -> String;
            fn get_type_mapping(&self, db_type: &str) -> Result<TypeId>;
            fn get_current_database_expression(&self) -> String;
            fn create_reserved_keywords_list(&self) -> KeywordList;
            fn create_schema_manager<'a>(&self, connection: &'a Connection) -> Box<dyn SchemaManager + 'a>;
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::tests::create_connection;

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    pub async fn generates_bit_and_comparison_expression_sql() {
        let connection = create_connection().await.unwrap();
        let platform = connection.get_platform().unwrap();

        let sql = platform.get_bit_and_comparison_expression(&2, &4).unwrap();
        assert_eq!(sql, "(2 & 4)");
    }

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    pub async fn generates_bit_or_comparison_expression_sql() {
        let connection = create_connection().await.unwrap();
        let platform = connection.get_platform().unwrap();

        let sql = platform.get_bit_or_comparison_expression(&2, &4).unwrap();
        assert_eq!(sql, "(2 | 4)");
    }
}

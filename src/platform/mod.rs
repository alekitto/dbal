mod create_flags;
mod date_interval_unit;
pub mod default;
mod keyword;
mod lock_mode;
mod trim_mode;

use crate::r#type::{Type, TypeManager};
use crate::schema::{
    Asset, Column, ColumnData, ColumnDiff, ForeignKeyConstraint, ForeignKeyReferentialAction,
    Identifier, Index, Sequence, Table, TableDiff, TableOptions, UniqueConstraint,
};
use crate::{Error, EventDispatcher, Result, TransactionIsolationLevel, Value};
pub use create_flags::CreateFlags;
pub use date_interval_unit::DateIntervalUnit;
pub(crate) use keyword::KeywordList;
pub use lock_mode::LockMode;
use std::any::{Any, TypeId};
use std::fmt::Debug;
use std::sync::Arc;
pub use trim_mode::TrimMode;

#[macro_export]
macro_rules! platform_debug {
    ($platform:ident) => {
        impl std::fmt::Debug for $platform {
            fn fmt(
                &self,
                f: &mut std::fmt::Formatter<'_>,
            ) -> core::result::Result<(), core::fmt::Error> {
                write!(f, "{} {{}}", core::any::type_name::<Self>())
            }
        }
    };
}

pub trait DatabasePlatform: Debug {
    /// Retrieves the event dispatcher
    fn get_event_manager(&self) -> Arc<EventDispatcher>;

    /// Load Type Mappings.
    /// # Internal - Only to be used by DatabasePlatform hierarchy
    fn _initialize_type_mappings(&self);

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
        default::get_ascii_string_type_declaration_sql(self, column)
    }

    /// Returns the SQL snippet used to declare a VARCHAR column type.
    fn get_string_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        default::get_string_type_declaration_sql(self, column)
    }

    /// Returns the SQL snippet used to declare a BINARY/VARBINARY column type.
    fn get_binary_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        default::get_binary_type_declaration_sql(self, column)
    }

    /// Returns the SQL snippet to declare a GUID/UUID column.
    ///
    /// By default this maps directly to a CHAR(36) and only maps to more
    /// special datatypes when the underlying databases support this datatype.
    fn get_guid_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        default::get_guid_type_declaration_sql(self, column)
    }

    /// Returns the SQL snippet to declare a JSON column.
    ///
    /// By default this maps directly to a CLOB and only maps to more
    /// special datatypes when the underlying databases support this datatype.
    fn get_json_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        default::get_json_type_declaration_sql(self, column)
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
                self._add_type_mapping(&db_type, type_id.clone())
            }
        }

        Ok(())
    }

    /// Registers a type to be used in conjunction with a column type of this platform.
    fn register_type_mapping(&self, db_type: &str, type_id: TypeId) -> Result<()>
    where
        Self: Sized,
    {
        let type_manager = TypeManager::get_instance();
        let r#type = type_manager.get_type(type_id)?;
        self._add_type_mapping(db_type, r#type.type_id());

        Ok(())
    }

    /// Gets the type that is mapped for the given database column type.
    fn get_type_mapping(&self, db_type: &str) -> Result<TypeId>;

    /// Checks if a database type is currently supported by this platform.
    fn has_type_mapping_for(&self, db_type: &str) -> bool {
        self.get_type_mapping(db_type).is_ok()
    }

    /// Gets the character used for identifier quoting.
    fn get_identifier_quote_character(&self) -> char {
        default::get_identifier_quote_character()
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
        default::get_date_add_seconds_expression(self, date, seconds)
    }

    /// Returns the SQL to subtract the number of given seconds from a date.
    fn get_date_sub_seconds_expression(&self, date: &str, seconds: i64) -> Result<String> {
        default::get_date_sub_seconds_expression(self, date, seconds)
    }

    /// Returns the SQL to add the number of given minutes to a date.
    fn get_date_add_minutes_expression(&self, date: &str, minutes: i64) -> Result<String> {
        default::get_date_add_minutes_expression(self, date, minutes)
    }

    /// Returns the SQL to subtract the number of given minutes from a date.
    fn get_date_sub_minutes_expression(&self, date: &str, minutes: i64) -> Result<String> {
        default::get_date_sub_minutes_expression(self, date, minutes)
    }

    /// Returns the SQL to add the number of given hours to a date.
    fn get_date_add_hour_expression(&self, date: &str, hours: i64) -> Result<String> {
        default::get_date_add_hour_expression(self, date, hours)
    }

    /// Returns the SQL to subtract the number of given hours to a date.
    fn get_date_sub_hour_expression(&self, date: &str, hours: i64) -> Result<String> {
        default::get_date_sub_hour_expression(self, date, hours)
    }

    /// Returns the SQL to add the number of given days to a date.
    fn get_date_add_days_expression(&self, date: &str, days: i64) -> Result<String> {
        default::get_date_add_days_expression(self, date, days)
    }

    /// Returns the SQL to subtract the number of given days to a date.
    fn get_date_sub_days_expression(&self, date: &str, days: i64) -> Result<String> {
        default::get_date_sub_days_expression(self, date, days)
    }

    /// Returns the SQL to add the number of given weeks to a date.
    fn get_date_add_weeks_expression(&self, date: &str, weeks: i64) -> Result<String> {
        default::get_date_add_weeks_expression(self, date, weeks)
    }

    /// Returns the SQL to subtract the number of given weeks from a date.
    fn get_date_sub_weeks_expression(&self, date: &str, weeks: i64) -> Result<String> {
        default::get_date_sub_weeks_expression(self, date, weeks)
    }

    /// Returns the SQL to add the number of given months to a date.
    fn get_date_add_month_expression(&self, date: &str, months: i64) -> Result<String> {
        default::get_date_add_month_expression(self, date, months)
    }

    /// Returns the SQL to subtract the number of given months to a date.
    fn get_date_sub_month_expression(&self, date: &str, months: i64) -> Result<String> {
        default::get_date_sub_month_expression(self, date, months)
    }

    /// Returns the SQL to add the number of given quarters to a date.
    fn get_date_add_quarters_expression(&self, date: &str, quarters: i64) -> Result<String> {
        default::get_date_add_quarters_expression(self, date, quarters)
    }

    /// Returns the SQL to subtract the number of given quarters from a date.
    fn get_date_sub_quarters_expression(&self, date: &str, quarters: i64) -> Result<String> {
        default::get_date_sub_quarters_expression(self, date, quarters)
    }

    /// Returns the SQL to add the number of given years to a date.
    fn get_date_add_years_expression(&self, date: &str, years: i64) -> Result<String> {
        default::get_date_add_years_expression(self, date, years)
    }

    /// Returns the SQL to subtract the number of given years from a date.
    fn get_date_sub_years_expression(&self, date: &str, years: i64) -> Result<String> {
        default::get_date_sub_years_expression(self, date, years)
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
    fn get_bit_and_comparison_expression(&self, value1: &str, value2: &str) -> Result<String> {
        default::get_bit_and_comparison_expression(value1, value2)
    }

    /// Returns the SQL bit OR comparison expression.
    fn get_bit_or_comparison_expression(&self, value1: &str, value2: &str) -> Result<String> {
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
        default::get_read_lock_sql(self)
    }

    /// Returns the SQL snippet to append to any SELECT statement which obtains an exclusive lock on the rows.
    ///
    /// The semantics of this lock mode should equal the SELECT .. FOR UPDATE of the ANSI SQL standard.
    fn get_write_lock_sql(&self) -> Result<String> {
        default::get_write_lock_sql(self)
    }

    /// Returns the SQL snippet to drop an existing table.
    fn get_drop_table_sql(&self, table_name: &Identifier) -> Result<String>
    where
        Self: Sized + Sync,
    {
        default::get_drop_table_sql(self, table_name)
    }

    /// Returns the SQL to safely drop a temporary table WITHOUT implicitly committing an open transaction.
    fn get_drop_temporary_table_sql(&self, table: &Identifier) -> Result<String>
    where
        Self: Sized + Sync,
    {
        default::get_drop_temporary_table_sql(self, table)
    }

    /// Returns the SQL to drop an index from a table.
    #[allow(unused_variables)]
    fn get_drop_index_sql(&self, index: &Identifier, table: &Identifier) -> Result<String> {
        default::get_drop_index_sql(self, index)
    }

    /// Returns the SQL to drop a constraint.
    ///
    /// # Internal
    /// The method should be only used from within the Platform trait.
    fn get_drop_constraint_sql(
        &self,
        constraint: Identifier,
        table_name: &Identifier,
    ) -> Result<String> {
        default::get_drop_constraint_sql(self, constraint, table_name)
    }

    /// Returns the SQL to drop a foreign key.
    fn get_drop_foreign_key_sql(
        &self,
        foreign_key: &ForeignKeyConstraint,
        table_name: &Identifier,
    ) -> Result<String> {
        default::get_drop_foreign_key_sql(self, foreign_key, table_name)
    }

    /// Returns the SQL to drop a unique constraint.
    fn get_drop_unique_constraint_sql(
        &self,
        name: Identifier,
        table_name: &Identifier,
    ) -> Result<String> {
        default::get_drop_unique_constraint_sql(self, name, table_name)
    }

    /// Returns the SQL statement(s) to create a table with the specified name, columns and constraints
    /// on this platform.
    fn get_create_table_sql(
        &self,
        table: Table,
        create_flags: Option<CreateFlags>,
    ) -> Result<Vec<String>>
    where
        Self: Sized + Sync,
    {
        default::get_create_table_sql(self, table, create_flags)
    }

    fn get_create_tables_sql(&self, tables: &[Table]) -> Result<Vec<String>>
    where
        Self: Sized + Sync,
    {
        default::get_create_tables_sql(self, tables)
    }

    fn get_drop_tables_sql(&self, tables: &[Table]) -> Result<Vec<String>>
    where
        Self: Sized + Sync,
    {
        default::get_drop_tables_sql(self, tables)
    }

    fn get_comment_on_table_sql(&self, table_name: &Identifier, comment: &str) -> Result<String> {
        default::get_comment_on_table_sql(self, table_name, comment)
    }

    fn get_comment_on_column_sql(
        &self,
        table_name: &Identifier,
        column: &Column,
        comment: &str,
    ) -> String {
        default::get_comment_on_column_sql(self, table_name, column, comment)
    }

    /// Returns the SQL to create inline comment on a column.
    fn get_inline_column_comment_sql(&self, comment: &str) -> Result<String> {
        default::get_inline_column_comment_sql(self, comment)
    }

    /// Returns the SQL used to create a table.
    ///
    /// # Internal
    /// The method should be only used from within the Platform trait.
    fn _get_create_table_sql(
        &self,
        name: &Identifier,
        columns: &[ColumnData],
        options: &TableOptions,
    ) -> Result<Vec<String>>
    where
        Self: Sized,
    {
        default::_get_create_table_sql(self, name, columns, options)
    }

    fn get_create_temporary_table_snippet_sql(&self) -> Result<String> {
        default::get_create_temporary_table_snippet_sql()
    }

    /// Returns the SQL to create a sequence on this platform.
    #[allow(unused_variables)]
    fn get_create_sequence_sql(&self, sequence: &Sequence) -> Result<String> {
        Err(Error::platform_feature_unsupported(
            "Sequences are not supported by this platform",
        ))
    }

    /// Returns the SQL to change a sequence on this platform.
    #[allow(unused_variables)]
    fn get_alter_sequence_sql(&self, sequence: &Sequence) -> Result<String> {
        Err(Error::platform_feature_unsupported(
            "Sequences are not supported by this platform",
        ))
    }

    /// Returns the SQL snippet to drop an existing sequence.
    fn get_drop_sequence_sql(&self, sequence: &Sequence) -> Result<String> {
        if !self.supports_sequences() {
            Err(Error::platform_feature_unsupported(
                "Sequences are not supported by this platform",
            ))
        } else {
            Ok(format!("DROP SEQUENCE {}", sequence.get_quoted_name(self)))
        }
    }

    /// Returns the SQL to create an index on a table on this platform.
    fn get_create_index_sql(&self, index: &Index, table: &Identifier) -> Result<String> {
        default::get_create_index_sql(self, index, table)
    }

    /// Adds condition for partial index.
    fn get_partial_index_sql(&self, index: &Index) -> String {
        default::get_partial_index_sql(self, index)
    }

    /// Adds additional flags for index generation.
    fn get_create_index_sql_flags(&self, index: &Index) -> String {
        default::get_create_index_sql_flags(index)
    }

    /// Returns the SQL to create an unnamed primary key constraint.
    fn get_create_primary_key_sql(&self, index: &Index, table: &Identifier) -> Result<String> {
        default::get_create_primary_key_sql(self, index, table)
    }

    /// Returns the SQL to create a named schema.
    fn get_create_schema_sql(&self, schema_name: &str) -> Result<String> {
        default::get_create_schema_sql(self, schema_name)
    }

    /// Returns the SQL to create a unique constraint on a table on this platform.
    fn get_create_unique_constraint_sql(
        &self,
        constraint: &UniqueConstraint,
        table_name: &Identifier,
    ) -> Result<String> {
        default::get_create_unique_constraint_sql(self, constraint, table_name)
    }

    /// Returns the SQL snippet to drop a schema.
    fn get_drop_schema_sql(&self, schema_name: &str) -> Result<String> {
        default::get_drop_schema_sql(self, schema_name)
    }

    /// Gets the comment to append to a column comment that helps parsing this type in reverse engineering.
    fn get_creed_type_comment(&self, creed_type: &dyn Type) -> String {
        default::get_creed_type_comment(creed_type)
    }

    /// Gets the comment of a passed column modified by potential doctrine type comment hints.
    fn get_column_comment(&self, column: &Column) -> Result<String>
    where
        Self: Sized,
    {
        default::get_column_comment(self, column)
    }

    /// Quotes a string so that it can be safely used as a table or column name,
    /// even if it is a reserved word of the platform. This also detects identifier
    /// chains separated by dot and quotes them independently.
    ///
    /// NOTE: Just because you CAN use quoted identifiers doesn't mean
    /// you SHOULD use them. In general, they end up causing way more
    /// problems than they solve.
    fn quote_identifier(&self, identifier: &str) -> String {
        default::quote_identifier(self, identifier)
    }

    /// Quotes a single identifier (no dot chain separation).
    fn quote_single_identifier(&self, str: &str) -> String {
        default::quote_single_identifier(self, str)
    }

    /// Quotes a literal string.
    /// This method is NOT meant to fix SQL injections!
    /// It is only meant to escape this platform's string literal
    /// quote character inside the given literal string.
    fn quote_string_literal(&self, str: &str) -> String {
        default::quote_string_literal(self, str)
    }

    /// Gets the character used for string literal quoting.
    fn get_string_literal_quote_character(&self) -> &str {
        default::get_string_literal_quote_character()
    }

    /// Returns the SQL to create a new foreign key.
    fn get_create_foreign_key_sql(
        &self,
        foreign_key: &ForeignKeyConstraint,
        table: &Identifier,
    ) -> Result<String> {
        default::get_create_foreign_key_sql(self, foreign_key, table)
    }

    /// Gets the SQL statements for altering an existing table.
    /// This method returns an array of SQL statements, since some platforms need several statements.
    #[allow(unused_variables)]
    fn get_alter_table_sql(&self, diff: &mut TableDiff) -> Result<Vec<String>> {
        Err(Error::platform_feature_unsupported("alter table"))
    }

    /// # Protected
    fn on_schema_alter_table_add_column(
        &self,
        column: &Column,
        diff: &TableDiff,
        column_sql: Vec<String>,
    ) -> Result<(bool, Vec<String>)>
    where
        Self: Sized + Sync,
    {
        default::on_schema_alter_table_add_column(self, column, diff, column_sql)
    }

    /// # Protected
    fn on_schema_alter_table_remove_column(
        &self,
        column: &Column,
        diff: &TableDiff,
        column_sql: Vec<String>,
    ) -> Result<(bool, Vec<String>)>
    where
        Self: Sized + Sync,
    {
        default::on_schema_alter_table_remove_column(self, column, diff, column_sql)
    }

    /// # Protected
    fn on_schema_alter_table_change_column(
        &self,
        column_diff: &ColumnDiff,
        diff: &TableDiff,
        column_sql: Vec<String>,
    ) -> Result<(bool, Vec<String>)>
    where
        Self: Sized + Sync,
    {
        default::on_schema_alter_table_change_column(self, column_diff, diff, column_sql)
    }

    /// # Protected
    fn on_schema_alter_table_rename_column(
        &self,
        old_column_name: &str,
        column: &Column,
        diff: &TableDiff,
        column_sql: Vec<String>,
    ) -> Result<(bool, Vec<String>)>
    where
        Self: Sized + Sync,
    {
        default::on_schema_alter_table_rename_column(
            self,
            old_column_name,
            column,
            diff,
            column_sql,
        )
    }

    /// # Protected
    fn on_schema_alter_table(
        &self,
        diff: &TableDiff,
        sql: Vec<String>,
    ) -> Result<(bool, Vec<String>)>
    where
        Self: Sized + Sync,
    {
        default::on_schema_alter_table(self, diff, sql)
    }

    /// # Protected
    fn get_pre_alter_table_index_foreign_key_sql(
        &self,
        diff: &mut TableDiff,
    ) -> Result<Vec<String>> {
        default::get_pre_alter_table_index_foreign_key_sql(self, diff)
    }

    /// # Protected
    fn get_post_alter_table_index_foreign_key_sql(&self, diff: &TableDiff) -> Result<Vec<String>> {
        default::get_post_alter_table_index_foreign_key_sql(self, diff)
    }

    /// Returns the SQL for renaming an index on a table.
    ///
    /// # Arguments
    ///
    /// * `old_index_name` - The name of the index to rename from.
    /// * `index` - The definition of the index to rename to.
    /// * `tableName` - The table to rename the given index on.
    ///
    /// # Protected
    fn get_rename_index_sql(
        &self,
        old_index_name: &Identifier,
        index: &Index,
        table_name: &Identifier,
    ) -> Result<Vec<String>> {
        default::get_rename_index_sql(self, old_index_name, index, table_name)
    }

    /// Gets declaration of a number of columns in bulk.
    fn get_column_declaration_list_sql(&self, columns: &[ColumnData]) -> Result<String>
    where
        Self: Sized,
    {
        default::get_column_declaration_list_sql(self, columns)
    }

    /// Obtains DBMS specific SQL code portion needed to declare a generic type
    /// column to be used in statements like CREATE TABLE.
    fn get_column_declaration_sql(&self, name: &str, column: &ColumnData) -> Result<String>
    where
        Self: Sized,
    {
        default::get_column_declaration_sql(self, name, column)
    }

    /// Returns the SQL snippet that declares a floating point column of arbitrary precision.
    fn get_decimal_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        default::get_decimal_type_declaration_sql(column)
    }

    /// Obtains DBMS specific SQL code portion needed to set a default value
    /// declaration to be used in statements like CREATE TABLE.
    fn get_default_value_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        default::get_default_value_declaration_sql(self, column)
    }

    /// Obtains DBMS specific SQL code portion needed to set a CHECK constraint
    /// declaration to be used in statements like CREATE TABLE.
    fn get_check_declaration_sql(&self, definition: &[ColumnData]) -> Result<String> {
        default::get_check_declaration_sql(self, definition)
    }

    fn get_check_field_declaration_sql(&self, definition: &ColumnData) -> Result<String> {
        default::get_check_field_declaration_sql(self, definition)
    }

    /// Obtains DBMS specific SQL code portion needed to set a unique
    /// constraint declaration to be used in statements like CREATE TABLE.
    fn get_unique_constraint_declaration_sql(
        &self,
        name: &str,
        constraint: &UniqueConstraint,
    ) -> Result<String> {
        default::get_unique_constraint_declaration_sql(self, name, constraint)
    }

    /// Obtains DBMS specific SQL code portion needed to set an index
    /// declaration to be used in statements like CREATE TABLE.
    fn get_index_declaration_sql(&self, name: &str, index: &Index) -> Result<String> {
        default::get_index_declaration_sql(self, name, index)
    }

    /// Obtains SQL code portion needed to create a custom column,
    /// e.g. when a column has the "columnDefinition" keyword.
    /// Only "AUTOINCREMENT" and "PRIMARY KEY" are added if appropriate.
    fn get_custom_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        default::get_custom_type_declaration_sql(column)
    }

    /// Obtains DBMS specific SQL code portion needed to set an index
    /// declaration to be used in statements like CREATE TABLE.
    fn get_index_field_declaration_list_sql(&self, index: &Index) -> Result<String> {
        default::get_index_field_declaration_list_sql(self, index)
    }

    /// Obtains DBMS specific SQL code portion needed to set an index
    /// declaration to be used in statements like CREATE TABLE.
    fn get_columns_field_declaration_list_sql(&self, columns: &[String]) -> Result<String> {
        default::get_columns_field_declaration_list_sql(columns)
    }

    /// Some vendors require temporary table names to be qualified specially.
    fn get_temporary_table_name(&self, table_name: &str) -> Result<String> {
        default::get_temporary_table_name(table_name)
    }

    /// Obtain DBMS specific SQL code portion needed to set the FOREIGN KEY constraint
    /// of a column declaration to be used in statements like CREATE TABLE.
    fn get_foreign_key_declaration_sql(
        &self,
        foreign_key: &ForeignKeyConstraint,
    ) -> Result<String> {
        default::get_foreign_key_declaration_sql(self, foreign_key)
    }

    /// Returns the FOREIGN KEY query section dealing with non-standard options
    /// as MATCH, INITIALLY DEFERRED, ON UPDATE, ...
    fn get_advanced_foreign_key_options_sql(
        &self,
        foreign_key: &ForeignKeyConstraint,
    ) -> Result<String> {
        default::get_advanced_foreign_key_options_sql(self, foreign_key)
    }

    /// Returns the given referential action in uppercase if valid, otherwise throws an exception.
    fn get_foreign_key_referential_action_sql(
        &self,
        action: &ForeignKeyReferentialAction,
    ) -> Result<String> {
        default::get_foreign_key_referential_action_sql(action)
    }

    /// Obtains DBMS specific SQL code portion needed to set the FOREIGN KEY constraint
    /// of a column declaration to be used in statements like CREATE TABLE.
    fn get_foreign_key_base_declaration_sql(
        &self,
        foreign_key: &ForeignKeyConstraint,
    ) -> Result<String> {
        default::get_foreign_key_base_declaration_sql(self, foreign_key)
    }

    /// Obtains DBMS specific SQL code portion needed to set the CHARACTER SET
    /// of a column declaration to be used in statements like CREATE TABLE.
    /// # Internal
    #[allow(unused_variables)]
    fn get_column_charset_declaration_sql(&self, charset: &str) -> String {
        default::get_column_charset_declaration_sql()
    }

    /// Obtains DBMS specific SQL code portion needed to set the COLLATION
    /// of a column declaration to be used in statements like CREATE TABLE.
    fn get_column_collation_declaration_sql(&self, collation: &str) -> String {
        default::get_column_collation_declaration_sql(self, collation)
    }

    /// Some platforms need the boolean values to be converted.
    /// The default conversion in this implementation converts to integers (false => 0, true => 1).
    ///
    /// # Note
    /// If the input is not a boolean the original input might be returned.
    ///
    /// There are two contexts when converting booleans: Literals and Prepared Statements.
    /// This method should handle the literal case
    fn convert_boolean(&self, item: Value) -> Value {
        default::convert_boolean(item)
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
    fn convert_booleans_to_database_value(&self, item: Value) -> Value {
        default::convert_booleans_to_database_value(self, item)
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

    fn get_list_databases_sql(&self) -> Result<String> {
        Err(Error::platform_feature_unsupported("list databases"))
    }

    #[allow(unused_variables)]
    fn get_list_sequences_sql(&self, database: &str) -> Result<String> {
        Err(Error::platform_feature_unsupported("list sequences"))
    }

    #[allow(unused_variables)]
    fn get_list_table_constraints_sql(&self, table: &str) -> Result<String> {
        Err(Error::platform_feature_unsupported(
            "list table constraints",
        ))
    }

    #[allow(unused_variables)]
    fn get_list_table_columns_sql(&self, table: &str, database: Option<&str>) -> Result<String> {
        Err(Error::platform_feature_unsupported("list table columns"))
    }

    fn get_list_tables_sql(&self) -> Result<String> {
        Err(Error::platform_feature_unsupported("list tables"))
    }

    /// Returns the SQL to list all views of a database or user.
    #[allow(unused_variables)]
    fn get_list_views_sql(&self, database: &str) -> Result<String> {
        Err(Error::platform_feature_unsupported("list views"))
    }

    /// Returns the list of indexes for the current database.
    /// The current database parameter is optional but will always be passed
    /// when using the SchemaManager API and is the database the given table is in.
    ///
    /// Attention: Some platforms only support currentDatabase when they
    /// re connected with that database. Cross-database information schema
    /// requests may be impossible.
    #[allow(unused_variables)]
    fn get_list_table_indexes_sql(&self, table: &str, database: Option<&str>) -> Result<String> {
        Err(Error::platform_feature_unsupported("list table indexes"))
    }

    #[allow(unused_variables)]
    fn get_list_table_foreign_keys_sql(&self, table: &str) -> Result<String> {
        Err(Error::platform_feature_unsupported(
            "list table foreign keys",
        ))
    }

    fn get_create_view_sql(&self, name: &str, sql: &str) -> Result<String> {
        default::get_create_view_sql(name, sql)
    }

    fn get_drop_view_sql(&self, name: &str) -> Result<String> {
        default::get_drop_view_sql(name)
    }

    #[allow(unused_variables)]
    fn get_sequence_next_val_sql(&self, sequence: &str) -> Result<String> {
        Err(Error::platform_feature_unsupported("sequence next val"))
    }

    /// Returns the SQL to create a new database.
    fn get_create_database_sql(&self, name: &str) -> Result<String> {
        default::get_create_database_sql(self, name)
    }

    /// Returns the SQL snippet to drop an existing database.
    fn get_drop_database_sql(&self, name: &str) -> Result<String> {
        default::get_drop_database_sql(self, name)
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
        default::get_date_time_tz_type_declaration_sql(self, column)
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
        default::get_truncate_table_sql(self, table_name)
    }

    /// This is for test reasons, many vendors have special requirements for dummy statements.
    fn get_dummy_select_sql(&self) -> String {
        "SELECT 1".to_string()
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
        default::escape_string_for_like(self, input_string, escape_char)
    }

    fn get_like_wildcard_characters(&self) -> &'static str {
        default::get_like_wildcard_characters()
    }

    /// Compares the definitions of the given columns in the context of this platform.
    fn columns_equal(&self, column1: &Column, column2: &Column) -> Result<bool>
    where
        Self: Sized,
    {
        default::columns_equal(self, column1, column2)
    }
}

#[cfg(test)]
mod tests {
    use crate::event::SchemaDropTableEvent;
    use crate::platform::keyword::{KeywordList, Keywords};
    use crate::platform::DatabasePlatform;
    use crate::schema::ColumnData;
    use crate::EventDispatcher;
    use crate::Result;
    use std::any::TypeId;
    use std::fmt::{Debug, Formatter};
    use std::sync::Arc;

    struct MockPlatform {
        ev: Arc<EventDispatcher>,
    }

    pub(super) struct MockKeywords {}
    impl Keywords for MockKeywords {
        fn get_name(&self) -> &'static str {
            "Mock"
        }

        fn get_keywords(&self) -> &[&'static str] {
            &["TABLE"]
        }
    }

    static MOCK_KEYWORDS: MockKeywords = MockKeywords {};

    impl Debug for MockPlatform {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "MockPlatform {{}}")
        }
    }

    impl DatabasePlatform for MockPlatform {
        fn get_event_manager(&self) -> Arc<EventDispatcher> {
            self.ev.clone()
        }

        fn _initialize_type_mappings(&self) {
            todo!()
        }

        fn _add_type_mapping(&self, db_type: &str, type_id: TypeId) {
            todo!()
        }

        fn get_boolean_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
            todo!()
        }

        fn get_integer_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
            todo!()
        }

        fn get_bigint_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
            todo!()
        }

        fn get_smallint_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
            todo!()
        }

        fn get_clob_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
            todo!()
        }

        fn get_blob_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
            todo!()
        }

        fn get_name(&self) -> String {
            todo!()
        }

        fn get_type_mapping(&self, db_type: &str) -> Result<TypeId> {
            todo!()
        }

        fn get_current_database_expression(&self) -> String {
            todo!()
        }

        fn create_reserved_keywords_list(&self) -> KeywordList {
            KeywordList::new(Box::new(&MOCK_KEYWORDS))
        }
    }

    #[tokio::test]
    async fn can_overwrite_drop_table_sql_via_event_listener() {
        let ev = Arc::new(EventDispatcher::new());
        ev.add_listener(|e: &mut SchemaDropTableEvent| {
            e.prevent_default();
            e.sql = Some(format!("-- DROP SCHEMA {}", e.get_table()));
        });

        let platform = MockPlatform { ev };
        let d = platform.get_drop_table_sql(&"table".into()).unwrap();

        assert_eq!("-- DROP SCHEMA table", d);
    }
}

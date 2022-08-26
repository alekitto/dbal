use crate::driver::mysql::platform::mysql_platform::{
    AbstractMySQLPlatform, LENGTH_LIMIT_BLOB, LENGTH_LIMIT_MEDIUMBLOB, LENGTH_LIMIT_MEDIUMTEXT,
    LENGTH_LIMIT_TEXT, LENGTH_LIMIT_TINYBLOB, LENGTH_LIMIT_TINYTEXT,
};
use crate::platform::{default, DateIntervalUnit};
use crate::r#type::{BlobType, TextType};
use crate::schema::{
    Asset, ColumnData, ForeignKeyConstraint, Identifier, Index, TableDiff, TableOptions,
};
use crate::{Error, Result, SchemaDropTableEvent, TransactionIsolationLevel, Value};
use core::marker::Sized;
use core::option::Option::Some;
use itertools::Itertools;
use std::any::TypeId;
use std::cmp::Ordering;

pub fn build_table_options<T: AbstractMySQLPlatform + ?Sized>(
    this: &T,
    options: &TableOptions,
) -> String {
    let options = options.clone();
    if let Some(table_options) = &options.table_options {
        return table_options.clone();
    }

    let mut opts = vec![];

    let charset = options.charset.unwrap_or_else(|| "utf8".to_string());
    opts.push(format!("DEFAULT CHARACTER SET {}", charset));

    let collation = options
        .collation
        .unwrap_or_else(|| format!("{}_unicode_ci", charset));
    opts.push(this.get_column_collation_declaration_sql(&collation));

    let engine = options.engine.unwrap_or_else(|| "InnoDB".to_string());
    opts.push(format!("ENGINE = {}", engine));

    if let Some(auto_increment) = options.auto_increment {
        opts.push(format!("AUTO_INCREMENT = {}", auto_increment));
    }

    if let Some(comment) = options.comment {
        opts.push(format!("COMMENT = {}", comment));
    }

    if let Some(row_format) = options.row_format {
        opts.push(format!("ROW_FORMAT = {}", row_format));
    }

    opts.iter().join(", ")
}

pub fn build_partition_options(options: &TableOptions) -> String {
    if let Some(po) = &options.partition_options {
        format!(" {}", po)
    } else {
        "".to_string()
    }
}

pub fn modify_limit_query(query: &str, limit: Option<usize>, offset: Option<usize>) -> String {
    let mut query = query.to_string();
    let offset = offset.unwrap_or(0);
    if let Some(limit) = limit {
        query += &format!(" LIMIT {}", limit);

        if offset > 0 {
            query += &format!(" OFFSET {}", offset);
        }
    } else if offset > 0 {
        // 2^64-1 is the maximum of unsigned BIGINT, the biggest limit possible
        query += &format!(" LIMIT 18446744073709551615 OFFSET {}", offset);
    }

    query
}

pub fn get_regexp_expression() -> Result<String> {
    Ok("RLIKE".to_string())
}

pub fn get_concat_expression(strings: Vec<&str>) -> Result<String> {
    Ok(format!("CONCAT({})", strings.join(", ")))
}

pub fn get_date_arithmetic_interval_expression(
    date: &str,
    operator: &str,
    interval: i64,
    unit: DateIntervalUnit,
) -> Result<String> {
    let function = if operator == "+" {
        "DATE_ADD"
    } else {
        "DATE_SUB"
    };

    Ok(format!(
        "{}({}, INTERVAL {} {})",
        function, date, interval, unit
    ))
}

pub fn get_date_diff_expression(date1: &str, date2: &str) -> Result<String> {
    Ok(format!("DATEDIFF({}, {})", date1, date2))
}

pub fn get_current_database_expression() -> String {
    "DATABASE()".to_string()
}

pub fn get_length_expression(column: &str) -> Result<String> {
    Ok(format!("CHAR_LENGTH({})", column))
}

pub fn get_list_databases_sql() -> Result<String> {
    Ok("SHOW DATABASES".to_string())
}

pub fn get_list_views_sql<T: AbstractMySQLPlatform + ?Sized>(
    this: &T,
    database: &str,
) -> Result<String> {
    Ok(format!(
        "SELECT * FROM information_schema.VIEWS WHERE TABLE_SCHEMA = {}",
        this.quote_string_literal(database)
    ))
}

pub fn get_varchar_type_declaration_sql_snippet(
    length: Option<usize>,
    fixed: bool,
) -> Result<String> {
    let c_type = if fixed { "CHAR" } else { "VARCHAR" };
    let length = length.unwrap_or(255);

    Ok(format!("{}({})", c_type, length))
}

pub fn get_binary_type_declaration_sql_snippet(
    length: Option<usize>,
    fixed: bool,
) -> Result<String> {
    let c_type = if fixed { "BINARY" } else { "VARBINARY" };
    let length = length.unwrap_or(255);

    Ok(format!("{}({})", c_type, length))
}

/// Gets the SQL snippet used to declare a CLOB column type.
///     TINYTEXT   : 2 ^  8 - 1 = 255
///     TEXT       : 2 ^ 16 - 1 = 65535
///     MEDIUMTEXT : 2 ^ 24 - 1 = 16777215
///     LONGTEXT   : 2 ^ 32 - 1 = 4294967295
pub fn get_clob_type_declaration_sql(column: &ColumnData) -> Result<String> {
    Ok(if let Some(len) = column.length {
        if len <= LENGTH_LIMIT_TINYTEXT {
            "TINYTEXT"
        } else if len <= LENGTH_LIMIT_TEXT {
            "TEXT"
        } else if len <= LENGTH_LIMIT_MEDIUMTEXT {
            "MEDIUMTEXT"
        } else {
            "LONGTEXT"
        }
    } else {
        "LONGTEXT"
    }
    .to_string())
}

pub fn get_date_time_type_declaration_sql(column: &ColumnData) -> Result<String> {
    Ok(if column.version.unwrap_or(false) {
        "TIMESTAMP"
    } else {
        "DATETIME"
    }
    .to_string())
}

pub fn get_date_type_declaration_sql() -> Result<String> {
    Ok("DATE".to_string())
}

pub fn get_time_type_declaration_sql() -> Result<String> {
    Ok("TIME".to_string())
}

pub fn get_boolean_type_declaration_sql() -> Result<String> {
    Ok("TINYINT(1)".to_string())
}

pub fn _get_create_table_sql<T: AbstractMySQLPlatform>(
    this: &T,
    name: &Identifier,
    columns: &[ColumnData],
    options: &TableOptions,
) -> Result<Vec<String>> {
    let mut query_fields = this.get_column_declaration_list_sql(columns)?;
    for (constraint_name, definition) in &options.unique_constraints {
        query_fields += ", ";
        query_fields += &this.get_unique_constraint_declaration_sql(constraint_name, definition)?;
    }

    for (index_name, definition) in &options.indexes {
        query_fields += ", ";
        query_fields += &this.get_index_declaration_sql(index_name, definition)?;
    }

    if let Some((primary, _)) = &options.primary {
        let key_columns = primary.iter().unique().join(", ");
        query_fields += &format!(", PRIMARY KEY({})", key_columns);
    }

    let mut query = "CREATE ".to_string();
    if options.temporary {
        query += "TEMPORARY ";
    }

    query += &format!("TABLE {} ({}) ", name.get_quoted_name(this), query_fields);
    query += &this.build_table_options(options);
    query += &this.build_partition_options(options);

    let mut sql = vec![query];

    // Propagate foreign key constraints only for InnoDB.
    for foreign_key in &options.foreign_keys {
        sql.push(this.get_create_foreign_key_sql(foreign_key, name)?);
    }

    Ok(sql)
}

pub fn get_default_value_declaration_sql<T: AbstractMySQLPlatform + ?Sized>(
    this: &T,
    column: &ColumnData,
) -> Result<String> {
    let default =
        if column.r#type == TypeId::of::<TextType>() || column.r#type == TypeId::of::<BlobType>() {
            Value::NULL
        } else {
            column.default.clone()
        };

    let mut column = column.clone();
    column.default = default;

    default::get_default_value_declaration_sql(this, &column)
}

pub fn get_alter_table_sql<T: AbstractMySQLPlatform + Sync>(
    this: &T,
    diff: &mut TableDiff,
) -> Result<Vec<String>> {
    let mut column_sql = vec![];
    let mut query_parts = vec![];
    let new_name = diff.get_new_name();

    if let Some(new_name) = new_name {
        query_parts.push(format!("RENAME TO {}", new_name.get_quoted_name(this)));
    }

    for column in &diff.added_columns {
        let (res, new_column_sql) =
            this.on_schema_alter_table_add_column(column, diff, column_sql)?;
        column_sql = new_column_sql;

        if res {
            continue;
        }

        let mut column_data = column.generate_column_data(this);
        let comment = this.get_column_comment(column)?;
        column_data.comment = if comment.is_empty() {
            None
        } else {
            Some(comment)
        };

        query_parts.push(format!(
            "ADD {}",
            this.get_column_declaration_sql(&column.get_quoted_name(this), &column_data)?
        ))
    }

    for column in &diff.removed_columns {
        let (res, new_column_sql) =
            this.on_schema_alter_table_remove_column(column, diff, column_sql)?;
        column_sql = new_column_sql;

        if res {
            continue;
        }

        query_parts.push(format!("DROP {}", column.get_quoted_name(this)));
    }

    for column_diff in &diff.changed_columns {
        let (res, new_column_sql) =
            this.on_schema_alter_table_change_column(column_diff, diff, column_sql)?;
        column_sql = new_column_sql;

        if res {
            continue;
        }

        let column = &column_diff.column;
        let mut column_data = column.generate_column_data(this);

        let comment = this.get_column_comment(column)?;
        column_data.comment = if comment.is_empty() {
            None
        } else {
            Some(comment)
        };

        query_parts.push(format!(
            "CHANGE {} {}",
            column_diff.get_old_column_name().get_quoted_name(this),
            this.get_column_declaration_sql(&column.get_quoted_name(this), &column_data)?
        ));
    }

    for (old_column_name, column) in &diff.renamed_columns {
        let (res, new_column_sql) =
            this.on_schema_alter_table_rename_column(old_column_name, column, diff, column_sql)?;
        column_sql = new_column_sql;

        if res {
            continue;
        }

        let old_column_name = Identifier::new(old_column_name, false);
        let mut column_data = column.generate_column_data(this);

        let comment = this.get_column_comment(column)?;
        column_data.comment = if comment.is_empty() {
            None
        } else {
            Some(comment)
        };

        query_parts.push(format!(
            "CHANGE {} {}",
            old_column_name.get_quoted_name(this),
            this.get_column_declaration_sql(&column.get_quoted_name(this), &column_data)?
        ));
    }

    if let Some((pos, primary_index)) = diff
        .added_indexes
        .iter()
        .find_position(|index| (*index).is_primary())
    {
        let columns = primary_index.get_columns();
        let mut key_columns = columns.iter().unique();

        query_parts.push(format!("ADD PRIMARY KEY ({})", key_columns.join(", ")));
        diff.added_indexes.remove(pos);
    } else if let Some((pos, primary_index)) = diff
        .changed_indexes
        .iter()
        .find_position(|index| (*index).is_primary())
    {
        for column_name in primary_index.get_columns() {
            if let Some(added_column) = diff.get_added_column(&column_name) {
                if added_column.is_autoincrement() {
                    {
                        let columns = primary_index.get_columns();
                        let mut key_columns = columns.iter().unique();

                        query_parts.push("DROP PRIMARY KEY".to_string());
                        query_parts.push(format!("ADD PRIMARY KEY ({})", key_columns.join(", ")));
                    }

                    diff.changed_indexes.remove(pos);
                    break;
                }
            }
        }
    }

    let mut sql = vec![];
    let (res, mut table_sql) = this.on_schema_alter_table(diff, vec![])?;
    if !res {
        let mut pre_alter_table = this.get_pre_alter_table_index_foreign_key_sql(diff)?;
        let mut post_alter_table = this.get_post_alter_table_index_foreign_key_sql(diff)?;

        sql.append(&mut pre_alter_table);
        if !query_parts.is_empty() {
            sql.push(format!(
                "ALTER TABLE {} {}",
                diff.get_name().get_quoted_name(this),
                query_parts.join(", ")
            ));
        }
        sql.append(&mut post_alter_table);
    }

    sql.append(&mut table_sql);
    sql.append(&mut column_sql);

    Ok(sql)
}

fn get_pre_alter_table_alter_primary_key_sql<T: AbstractMySQLPlatform>(
    this: &T,
    diff: &TableDiff,
    index: &Index,
) -> Result<Vec<String>> {
    let mut sql = vec![];
    if !index.is_primary() || diff.from_table.is_none() {
        return Ok(sql);
    }

    let table_name = diff.get_name().get_quoted_name(this);
    let from_table = diff.from_table.unwrap();

    for column_name in index.get_columns() {
        if let Some(column) = from_table.get_column(&Identifier::new(&column_name, false)) {
            if !column.is_autoincrement() {
                continue;
            }

            let mut column_data = column.generate_column_data(this);
            column_data.autoincrement = Some(false);

            sql.push(format!(
                "ALTER TABLE {} MODIFY {}",
                table_name,
                this.get_column_declaration_sql(&column.get_quoted_name(this), &column_data)?
            ));
        }
    }

    Ok(sql)
}

pub fn get_pre_alter_table_index_foreign_key_sql<T: AbstractMySQLPlatform + Sync>(
    this: &T,
    diff: &mut TableDiff,
) -> Result<Vec<String>> {
    let mut sql = vec![];
    let table = diff.get_name().get_quoted_name(this);

    for index in &diff.changed_indexes {
        let mut pre = get_pre_alter_table_alter_primary_key_sql(this, diff, index)?;
        sql.append(&mut pre);
    }

    let mut indexes_to_be_removed = vec![];
    for removed_index in &diff.removed_indexes {
        let mut pre = get_pre_alter_table_alter_primary_key_sql(this, diff, removed_index)?;
        sql.append(&mut pre);

        for (add_idx, added_index) in diff.added_indexes.iter().enumerate() {
            if removed_index.get_columns().cmp(&added_index.get_columns()) != Ordering::Equal {
                continue;
            }

            let index_clause = if added_index.is_primary() {
                "PRIMARY KEY".to_string()
            } else if added_index.is_unique() {
                format!("UNIQUE INDEX {}", added_index.get_name())
            } else {
                format!("INDEX {}", added_index.get_name())
            };

            sql.push(format!(
                "ALTER TABLE {} DROP INDEX {}, ADD {} ({})",
                table,
                removed_index.get_name(),
                index_clause,
                this.get_index_field_declaration_list_sql(added_index)?
            ));

            indexes_to_be_removed.push(removed_index.get_name());
            diff.added_indexes.remove(add_idx);

            break;
        }
    }

    diff.removed_indexes = diff
        .removed_indexes
        .iter()
        .cloned()
        .filter(|idx| !indexes_to_be_removed.contains(&idx.get_name()))
        .collect();

    let engine = diff
        .from_table
        .and_then(|t| t.get_engine())
        .unwrap_or_else(|| "INNODB".to_string())
        .trim()
        .to_uppercase();

    if engine != "INNODB" {
        diff.added_foreign_keys = vec![];
        diff.changed_foreign_keys = vec![];
        diff.removed_foreign_keys = vec![];
    }

    sql.append(&mut this.get_pre_alter_table_index_foreign_key_sql(diff)?);
    sql.append(&mut default::get_pre_alter_table_index_foreign_key_sql(
        this, diff,
    )?);

    Ok(sql)
}

pub fn get_create_index_sql_flags(index: &Index) -> String {
    let mut index_type = "".to_string();
    if index.is_unique() {
        index_type += "UNIQUE ";
    } else if index.has_flag("fulltext") {
        index_type += "FULLTEXT "
    } else if index.has_flag("spatial") {
        index_type += "SPATIAL ";
    }

    index_type
}

fn get_unsigned_declaration(column: &ColumnData) -> &'static str {
    if column.unsigned.unwrap_or(false) {
        " UNSIGNED"
    } else {
        ""
    }
}

fn get_common_integer_type_declaration_sql(column: &ColumnData) -> String {
    let mut autoinc = "".to_string();
    if column.autoincrement.unwrap_or(false) {
        autoinc += " AUTO_INCREMENT";
    }

    format!("{}{}", get_unsigned_declaration(column), autoinc)
}

pub fn get_integer_type_declaration_sql(column: &ColumnData) -> Result<String> {
    Ok(format!(
        "INT{}",
        get_common_integer_type_declaration_sql(column)
    ))
}

pub fn get_bigint_type_declaration_sql(column: &ColumnData) -> Result<String> {
    Ok(format!(
        "BIGINT{}",
        get_common_integer_type_declaration_sql(column)
    ))
}

pub fn get_smallint_type_declaration_sql(column: &ColumnData) -> Result<String> {
    Ok(format!(
        "SMALLINT{}",
        get_common_integer_type_declaration_sql(column)
    ))
}

pub fn get_float_declaration_sql(column: &ColumnData) -> Result<String> {
    Ok(format!(
        "DOUBLE PRECISION{}",
        get_unsigned_declaration(column)
    ))
}

pub fn get_decimal_type_declaration_sql(column: &ColumnData) -> Result<String> {
    Ok(format!(
        "{}{}",
        default::get_decimal_type_declaration_sql(column)?,
        get_unsigned_declaration(column)
    ))
}

pub fn get_column_charset_declaration_sql(charset: &str) -> String {
    format!("CHARACTER SET {}", charset)
}

pub fn get_column_collation_declaration_sql<T: AbstractMySQLPlatform + ?Sized>(
    this: &T,
    collation: &str,
) -> String {
    format!("COLLATE {}", this.quote_single_identifier(collation))
}

/**
 * {@inheritDoc}
 *
 * @internal The method should be only used from within the {@see AbstractPlatform} class hierarchy.
 */
pub fn get_advanced_foreign_key_options_sql<T: AbstractMySQLPlatform + ?Sized>(
    this: &T,
    foreign_key: &ForeignKeyConstraint,
) -> Result<String> {
    let mut query = "".to_string();
    if let Some(m) = foreign_key.get_option("match") {
        if let Value::String(match_string) = m {
            query += &format!(" MATCH {}", match_string)
        } else {
            return Err(Error::type_mismatch());
        }
    }

    query += &default::get_advanced_foreign_key_options_sql(this, foreign_key)?;
    Ok(query)
}

pub fn get_drop_index_sql<T: AbstractMySQLPlatform + ?Sized>(
    this: &T,
    index: &Identifier,
    table: &Identifier,
) -> Result<String> {
    Ok(format!(
        "DROP INDEX {} ON {}",
        index.get_quoted_name(this),
        table.get_quoted_name(this)
    ))
}

pub fn get_drop_unique_constraint_sql<T: AbstractMySQLPlatform + ?Sized>(
    this: &T,
    name: Identifier,
    table_name: &Identifier,
) -> Result<String> {
    this.get_drop_index_sql(&name, table_name)
}

pub fn get_set_transaction_isolation_sql<T: AbstractMySQLPlatform + ?Sized>(
    this: &T,
    level: TransactionIsolationLevel,
) -> Result<String> {
    Ok(format!(
        "SET SESSION TRANSACTION ISOLATION LEVEL {}",
        this.get_transaction_isolation_level_sql(level)
    ))
}

pub fn get_read_lock_sql() -> Result<String> {
    Ok("LOCK IN SHARE MODE".to_string())
}

pub fn get_drop_temporary_table_sql<T: AbstractMySQLPlatform + Sync>(
    this: &T,
    table: &Identifier,
) -> Result<String> {
    let table_arg = table.get_quoted_name(this);

    let mut ev = SchemaDropTableEvent::new(table_arg.clone(), this);
    this.get_event_manager().dispatch_sync(&mut ev);

    if ev.is_default_prevented() {
        if let Some(ref sql) = ev.sql {
            return Ok(sql.clone());
        }
    }

    Ok(format!("DROP TEMPORARY TABLE {}", table_arg))
}

pub fn get_blob_type_declaration_sql(column: &ColumnData) -> Result<String> {
    Ok(if let Some(length) = column.length {
        if length <= LENGTH_LIMIT_TINYBLOB {
            "TINYBLOB"
        } else if length <= LENGTH_LIMIT_BLOB {
            "BLOB"
        } else if length <= LENGTH_LIMIT_MEDIUMBLOB {
            "MEDIUMBLOB"
        } else {
            "LONGBLOB"
        }
    } else {
        "LONGBLOB"
    }
    .to_string())
}

pub fn quote_string_literal<T: AbstractMySQLPlatform + Sync>(this: &T, str: &str) -> String {
    let str = str.replace('\\', "\\\\");
    default::quote_string_literal(this, &str)
}

pub fn get_default_transaction_isolation_level() -> TransactionIsolationLevel {
    TransactionIsolationLevel::RepeatableRead
}

pub fn get_rename_index_sql<T: AbstractMySQLPlatform + Sync>(
    this: &T,
    old_index_name: &Identifier,
    index: &Index,
    table_name: &Identifier,
) -> Result<Vec<String>> {
    Ok(vec![format!(
        "ALTER TABLE {} RENAME INDEX {} TO {}",
        table_name.get_quoted_name(this),
        old_index_name.get_quoted_name(this),
        index.get_quoted_name(this)
    )])
}

pub fn get_json_type_declaration_sql() -> Result<String> {
    Ok("JSON".to_string())
}

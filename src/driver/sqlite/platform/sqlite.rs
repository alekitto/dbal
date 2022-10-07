use super::sqlite_platform::AbstractSQLitePlatform;
use crate::driver::sqlite::platform::AbstractSQLiteSchemaManager;
use crate::error::ErrorKind;
use crate::platform::{default, CreateFlags, DatabasePlatform, DateIntervalUnit, TrimMode};
use crate::r#type::{IntoType, BIGINT, DATE, DATETIME, INTEGER, STRING, TIME};
use crate::schema::{
    Asset, Column, ColumnData, ForeignKeyConstraint, Identifier, Index, SchemaManager, Table,
    TableDiff, TableOptions,
};
use crate::{Error, Result, TransactionIsolationLevel};
use itertools::Itertools;
use std::cmp::Ordering;
use std::collections::hash_map::Entry::Occupied;
use std::collections::HashMap;

pub fn get_regexp_expression() -> Result<String> {
    Ok("REGEXP".to_string())
}

pub fn get_trim_expression(str: &str, mode: TrimMode, char: Option<String>) -> Result<String> {
    let trim_char = if let Some(char) = char {
        format!(", {}", char)
    } else {
        "".to_string()
    };
    let trim_fn = match mode {
        TrimMode::Leading => "LTRIM",
        TrimMode::Trailing => "RTRIM",
        _ => "TRIM",
    };

    Ok(format!("{}({}{})", trim_fn, str, trim_char))
}

pub fn get_substring_expression(
    string: &str,
    start: usize,
    length: Option<usize>,
) -> Result<String> {
    if let Some(length) = length {
        Ok(format!("SUBSTR({}, {}, {})", string, start, length))
    } else {
        Ok(format!("SUBSTR({}, {}, LENGTH({}))", string, start, string))
    }
}

pub fn get_locate_expression(str: &str, substr: &str, start_pos: Option<usize>) -> Result<String> {
    if let Some(start_pos) = start_pos {
        Ok(format!("LOCATE({}, {}, {})", str, substr, start_pos))
    } else {
        Ok(format!("LOCATE({}, {})", str, substr))
    }
}

pub fn get_date_arithmetic_interval_expression(
    date: &str,
    operator: &str,
    interval: i64,
    unit: DateIntervalUnit,
) -> Result<String> {
    match unit {
        DateIntervalUnit::Second | DateIntervalUnit::Minute | DateIntervalUnit::Hour => Ok(
            format!("DATETIME({}, '{}{} {}')))", date, operator, interval, unit),
        ),
        DateIntervalUnit::Week => Ok(format!(
            "DATE({}, '{}{} {}')))",
            date,
            operator,
            interval * 7,
            DateIntervalUnit::Day
        )),
        DateIntervalUnit::Quarter => Ok(format!(
            "DATE({}, '{}{} {}')))",
            date,
            operator,
            interval * 3,
            DateIntervalUnit::Month
        )),
        _ => Ok(format!(
            "DATE({}, '{}{} {}')))",
            date, operator, interval, unit
        )),
    }
}

pub fn get_date_diff_expression(date1: &str, date2: &str) -> Result<String> {
    Ok(format!(
        "JULIANDAY({}, 'start of day') - JULIANDAY({}, 'start of day')",
        date1, date2
    ))
}

pub fn get_transaction_isolation_level_sql(level: TransactionIsolationLevel) -> String {
    match level {
        TransactionIsolationLevel::ReadUncommitted => "1".to_string(),
        TransactionIsolationLevel::ReadCommitted
        | TransactionIsolationLevel::RepeatableRead
        | TransactionIsolationLevel::Serializable => "0".to_string(),
    }
}

pub fn get_set_transaction_isolation_sql<T: AbstractSQLitePlatform + ?Sized>(
    this: &T,
    level: TransactionIsolationLevel,
) -> Result<String> {
    Ok(format!(
        "PRAGMA read_uncommitted = {}",
        this.get_transaction_isolation_level_sql(level)
    ))
}

pub fn get_boolean_type_declaration_sql() -> Result<String> {
    Ok("BOOLEAN".to_string())
}

fn get_common_integer_type_declaration_sql(column: &ColumnData) -> &'static str {
    // sqlite autoincrement is only possible for the primary key
    if column.autoincrement.unwrap_or(false) {
        " PRIMARY KEY AUTOINCREMENT"
    } else if column.unsigned.unwrap_or(false) {
        " UNSIGNED"
    } else {
        ""
    }
}

pub fn get_integer_type_declaration_sql(column: &ColumnData) -> Result<String> {
    Ok(format!(
        "INTEGER{}",
        get_common_integer_type_declaration_sql(column)
    ))
}

pub fn get_bigint_type_declaration_sql<T: AbstractSQLitePlatform + ?Sized>(
    this: &T,
    column: &ColumnData,
) -> Result<String> {
    // SQLite autoincrement is implicit for INTEGER PKs, but not for BIGINT columns
    if column.autoincrement.unwrap_or(false) {
        this.get_integer_type_declaration_sql(column)
    } else {
        Ok(format!(
            "BIGINT{}",
            get_common_integer_type_declaration_sql(column)
        ))
    }
}

pub fn get_smallint_type_declaration_sql<T: AbstractSQLitePlatform + ?Sized>(
    this: &T,
    column: &ColumnData,
) -> Result<String> {
    // SQLite autoincrement is implicit for INTEGER PKs, but not for BIGINT columns
    if column.autoincrement.unwrap_or(false) {
        this.get_integer_type_declaration_sql(column)
    } else {
        Ok(format!(
            "SMALLINT{}",
            get_common_integer_type_declaration_sql(column)
        ))
    }
}

pub fn get_date_time_type_declaration_sql() -> Result<String> {
    Ok("DATETIME".to_string())
}

pub fn get_date_type_declaration_sql() -> Result<String> {
    Ok("DATE".to_string())
}

pub fn get_time_type_declaration_sql() -> Result<String> {
    Ok("TIME".to_string())
}

/// Generate a PRIMARY KEY definition if no autoincrement value is used
fn get_non_autoincrement_primary_key_definition(
    columns: &[ColumnData],
    options: &TableOptions,
) -> String {
    if let Some((vec, _)) = &options.primary {
        if vec.is_empty() {
            "".to_string()
        } else {
            let mut key_columns = vec.iter().unique();
            let joined_columns = { key_columns.join(", ") };
            for key_column in key_columns {
                if let Some(column) = columns
                    .iter()
                    .find(|c| c.name.cmp(key_column) == Ordering::Equal)
                {
                    if column.autoincrement.unwrap_or(false) {
                        return "".to_string();
                    }
                }
            }

            format!(", PRIMARY KEY({})", joined_columns)
        }
    } else {
        "".to_string()
    }
}

fn get_inline_table_comment_sql<T: AbstractSQLiteSchemaManager + ?Sized>(
    this: &T,
    comment: &str,
) -> Result<String> {
    this.get_inline_column_comment_sql(comment)
}

pub fn _get_create_table_sql<T: AbstractSQLiteSchemaManager + ?Sized>(
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

    query_fields += &get_non_autoincrement_primary_key_definition(columns, options);

    for foreign_key in &options.foreign_keys {
        query_fields += ", ";
        query_fields += &this.get_foreign_key_declaration_sql(foreign_key)?;
    }

    let table_comment = if let Some(comment) = &options.comment {
        let comment = comment.trim_matches(&[' ', '\''] as &[_]);
        get_inline_table_comment_sql(this, comment)?
    } else {
        "".to_string()
    };

    let mut query = vec![format!(
        "CREATE TABLE {} {}({})",
        name.get_quoted_name(&this.get_platform()?),
        table_comment,
        query_fields
    )];

    if !options.alter {
        for index_def in options.indexes.values() {
            query.push(this.get_create_index_sql(index_def, name)?);
        }
    }

    Ok(query)
}

pub fn get_varchar_type_declaration_sql_snippet(
    length: Option<usize>,
    fixed: bool,
) -> Result<String> {
    if fixed {
        Ok(format!("CHAR({})", length.unwrap_or(255)))
    } else {
        let length = length.unwrap_or(0);
        if length > 0 {
            Ok(format!("VARCHAR({})", length))
        } else {
            Ok("TEXT".to_string())
        }
    }
}

pub fn get_binary_type_declaration_sql_snippet() -> Result<String> {
    Ok("BLOB".to_string())
}

pub fn get_clob_type_declaration_sql() -> Result<String> {
    Ok("CLOB".to_string())
}

pub fn get_list_table_constraints_sql<T: AbstractSQLiteSchemaManager + ?Sized>(
    this: &T,
    table: &str,
) -> Result<String> {
    Ok(format!(
        "SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name = {} AND sql NOT NULL ORDER BY name",
        this.quote_string_literal(table)
    ))
}

pub fn get_list_table_columns_sql<T: AbstractSQLiteSchemaManager + ?Sized>(
    this: &T,
    table: &str,
) -> Result<String> {
    Ok(format!(
        "PRAGMA table_info({})",
        this.quote_string_literal(table)
    ))
}

pub fn get_list_tables_sql() -> Result<String> {
    Ok("SELECT name FROM sqlite_master \
        WHERE type = 'table' \
        AND name != 'sqlite_sequence' \
        AND name != 'geometry_columns' \
        AND name != 'spatial_ref_sys' \
        UNION ALL SELECT name FROM sqlite_temp_master' \
        WHERE type = 'table' ORDER BY name"
        .to_string())
}

pub fn get_list_views_sql() -> Result<String> {
    Ok("SELECT name, sql FROM sqlite_master WHERE type='view' AND sql NOT NULL".to_string())
}

pub fn get_advanced_foreign_key_options_sql(
    this: &dyn SchemaManager,
    foreign_key: &ForeignKeyConstraint,
) -> Result<String> {
    let mut query = default::get_advanced_foreign_key_options_sql(this, foreign_key)?;

    let is_deferrable = bool::from(foreign_key.get_option("deferrable").unwrap_or_default());
    if !is_deferrable {
        query += " NOT";
    }

    query += " DEFERRABLE INITIALLY";

    let deferred = bool::from(foreign_key.get_option("deferred").unwrap_or_default());
    query += if deferred { " DEFERRED" } else { " IMMEDIATE" };

    Ok(query)
}

pub fn get_truncate_table_sql(this: &dyn DatabasePlatform, table_name: &Identifier) -> String {
    format!("DELETE FROM {}", table_name.get_quoted_name(this))
}

pub fn get_for_update_sql() -> Result<String> {
    Ok("".to_string())
}

pub fn get_inline_column_comment_sql(comment: &str) -> Result<String> {
    Ok(format!("--{}\n", comment.replace('\n', "\n--")))
}

pub fn get_pre_alter_table_index_foreign_key_sql() -> Result<Vec<String>> {
    Ok(vec![])
}

fn get_column_names_in_altered_table(
    diff: &TableDiff,
    from_table: &Table,
) -> HashMap<String, String> {
    let mut columns = HashMap::new();

    for column in from_table.get_columns() {
        let name = column.get_name();
        columns.insert(name.to_lowercase(), name);
    }

    for removed_column in &diff.removed_columns {
        let column_name = removed_column.get_name().to_lowercase();
        columns.remove(&column_name);
    }

    for (old_column_name, column) in &diff.renamed_columns {
        let column_name = column.get_name();
        columns.insert(old_column_name.to_lowercase(), column_name.clone());
        columns.insert(column_name.to_lowercase(), column_name);
    }

    for column_diff in &diff.changed_columns {
        let column_name = column_diff.column.get_name();
        columns.insert(
            column_diff.get_old_column_name().get_name().to_lowercase(),
            column_name.clone(),
        );
        columns.insert(column_name.to_lowercase(), column_name);
    }

    for column in &diff.added_columns {
        let column_name = column.get_name();
        columns.insert(column_name.to_lowercase(), column_name);
    }

    columns
}

fn get_indexes_in_altered_table(diff: &TableDiff, from_table: &Table) -> Vec<Index> {
    let indexes = from_table.get_indices().clone();
    let column_names = get_column_names_in_altered_table(diff, from_table);

    #[allow(clippy::needless_collect)]
    let renamed_indexes = diff
        .renamed_indexes
        .iter()
        .map(|i| i.0.to_lowercase())
        .collect::<Vec<_>>();

    let mut new_indexes = vec![];
    'a: for index in &indexes {
        let index_name = index.get_name();
        if renamed_indexes.contains(&index_name.to_lowercase()) {
            continue;
        }

        let mut changed = false;
        let mut index_columns = vec![];
        for column_name in &index.get_columns() {
            let normalized_column_name = column_name.to_lowercase();
            if column_names.get(&normalized_column_name).is_some() {
                index_columns.push(normalized_column_name.clone());
                if *column_name == normalized_column_name {
                    continue;
                }

                changed = true;
            } else {
                continue 'a;
            }
        }

        if changed {
            new_indexes.push(Index::new(
                index.get_name(),
                &index_columns,
                index.is_unique(),
                index.is_primary(),
                index.get_flags(),
                index.get_options().clone(),
            ));
        } else {
            new_indexes.push(index.clone());
        }
    }

    for index in &diff.removed_indexes {
        let index_name = index.get_name().to_lowercase();
        if index_name.is_empty() || !indexes.iter().any(|ix| ix.get_name() == index_name) {
            new_indexes.push(index.clone());
        }
    }

    for index in &diff.changed_indexes {
        new_indexes.push(index.clone());
    }

    for index in &diff.added_indexes {
        new_indexes.push(index.clone());
    }

    for (_, index) in &diff.renamed_indexes {
        new_indexes.push(index.clone());
    }

    new_indexes
}

// /** @return ForeignKeyConstraint[] */
fn get_foreign_keys_in_altered_table(
    diff: &TableDiff,
    from_table: &Table,
) -> Vec<ForeignKeyConstraint> {
    let foreign_keys = from_table.get_foreign_keys();
    let column_names = get_column_names_in_altered_table(diff, from_table);

    let mut new_foreign_keys = vec![];
    'a: for constraint in foreign_keys {
        let mut changed = false;
        let mut local_columns = vec![];

        for local_column in constraint.get_local_columns() {
            let normalized_column_name = local_column.get_name().to_lowercase();
            if let Some(column_name) = column_names.get(&normalized_column_name) {
                local_columns.push(column_name.clone());
                if column_name == &local_column.get_name() {
                    continue;
                }

                changed = true
            } else {
                continue 'a;
            }
        }

        if changed {
            new_foreign_keys.push(ForeignKeyConstraint::new(
                local_columns,
                constraint
                    .get_foreign_columns()
                    .iter()
                    .map(|col| col.get_name())
                    .collect(),
                constraint.get_foreign_table().get_name(),
                constraint.get_options(),
                constraint.on_update,
                constraint.on_delete,
            ));
        } else {
            new_foreign_keys.push(constraint.clone());
        }
    }

    let removed_key_names: Vec<String> = diff
        .removed_foreign_keys
        .iter()
        .map(|fk| fk.get_name().to_lowercase())
        .collect();
    new_foreign_keys = new_foreign_keys
        .iter()
        .cloned()
        .filter(|fk| !removed_key_names.contains(&fk.get_name().to_lowercase()))
        .collect();

    for constraint in &diff.changed_foreign_keys {
        new_foreign_keys.push(constraint.clone());
    }
    for constraint in &diff.added_foreign_keys {
        new_foreign_keys.push(constraint.clone());
    }

    new_foreign_keys
}

fn get_primary_index_in_altered_table(diff: &TableDiff, from_table: &Table) -> Option<Index> {
    let mut primary_index = None;
    for index in get_indexes_in_altered_table(diff, from_table) {
        if index.is_primary() {
            primary_index = Some(index);
        }
    }

    primary_index
}

pub fn get_post_alter_table_index_foreign_key_sql<T: AbstractSQLiteSchemaManager + ?Sized>(
    this: &T,
    diff: &TableDiff,
) -> Result<Vec<String>> {
    let from_table = diff.from_table.ok_or_else(|| Error::new(ErrorKind::UnknownError, "Sqlite platform requires for alter table the table diff with reference to original table schema"))?;
    let mut sql = vec![];
    let table_name = if let Some(name) = diff.get_new_name() {
        name
    } else {
        diff.get_name()
    };

    for index in get_indexes_in_altered_table(diff, from_table) {
        if !index.is_primary() {
            sql.push(this.get_create_index_sql(&index, &table_name)?)
        }
    }

    Ok(sql)
}

pub fn modify_limit_query(query: &str, limit: Option<usize>, offset: Option<usize>) -> String {
    let current_offset = offset.unwrap_or(0);
    if limit.is_none() && current_offset > 0 {
        format!("{} LIMIT -1 OFFSET {}", query, current_offset)
    } else {
        default::modify_limit_query(query, limit, offset)
    }
}

pub fn get_blob_type_declaration_sql() -> Result<String> {
    Ok("BLOB".to_string())
}

pub fn get_temporary_table_name(table_name: &str) -> Result<String> {
    Ok(table_name.to_string())
}

pub fn get_create_tables_sql<T: AbstractSQLiteSchemaManager + Sync>(
    this: &T,
    tables: &[Table],
) -> Result<Vec<String>> {
    let mut sql = vec![];
    for table in tables {
        let mut table_sql = this.get_create_table_sql(table, None)?;
        sql.append(&mut table_sql);
    }

    Ok(sql)
}

pub fn get_drop_tables_sql<T: AbstractSQLiteSchemaManager + Sync>(
    this: &T,
    tables: &[Table],
) -> Result<Vec<String>> {
    let mut sql = vec![];
    for table in tables {
        let table_sql = this.get_drop_table_sql(table.get_table_name())?;
        sql.push(table_sql);
    }

    Ok(sql)
}

pub fn get_create_table_sql(
    this: &dyn SchemaManager,
    table: &Table,
    create_flags: Option<CreateFlags>,
) -> Result<Vec<String>> {
    let create_flags =
        create_flags.unwrap_or(CreateFlags::CREATE_INDEXES | CreateFlags::CREATE_FOREIGN_KEYS);
    default::get_create_table_sql(this, table, Some(create_flags))
}

pub fn get_list_table_foreign_keys_sql<T: AbstractSQLiteSchemaManager + ?Sized>(
    this: &T,
    table: &str,
) -> Result<String> {
    Ok(format!(
        "PRAGMA foreign_key_list({})",
        this.quote_string_literal(table)
    ))
}

fn get_simple_alter_table_sql<T: AbstractSQLiteSchemaManager + Sync + ?Sized>(
    this: &T,
    diff: &mut TableDiff,
) -> Result<Option<Vec<String>>> {
    let mut changed_columns = vec![];
    let integer_type = INTEGER.into_type()?;
    let bigint_type = BIGINT.into_type()?;

    // Suppress changes on integer type autoincrement columns.
    for column_diff in diff.changed_columns.drain(..) {
        if column_diff.column.is_autoincrement() && column_diff.column.get_type() == integer_type {
            if let Some(from_column) = &column_diff.from_column {
                let from_column_type = from_column.get_type();
                if from_column_type == integer_type || from_column_type == bigint_type {
                    continue;
                }
            }
        }

        changed_columns.push(column_diff);
    }

    diff.changed_columns = changed_columns;

    if !diff.renamed_columns.is_empty()
        || !diff.added_foreign_keys.is_empty()
        || !diff.added_indexes.is_empty()
        || !diff.changed_columns.is_empty()
        || !diff.changed_foreign_keys.is_empty()
        || !diff.changed_indexes.is_empty()
        || !diff.removed_columns.is_empty()
        || !diff.removed_foreign_keys.is_empty()
        || !diff.removed_indexes.is_empty()
        || !diff.renamed_indexes.is_empty()
    {
        Ok(None)
    } else {
        let platform = this.get_platform()?;

        let table = Table::new(diff.get_name());
        let table_name = table.get_quoted_name(&platform);

        let mut sql = vec![];
        let mut column_sql = vec![];

        for column in &diff.added_columns {
            let (res, new_column_sql) =
                this.on_schema_alter_table_add_column(column, diff, column_sql)?;
            column_sql = new_column_sql;
            if res {
                continue;
            }

            let mut definition = column.generate_column_data(&platform);
            let r#type = column.get_type();

            if definition.column_definition.is_some()
                || definition.autoincrement.unwrap_or(false)
                || definition.unique
                || (r#type == DATETIME.into_type()?
                    && definition.default == platform.get_current_timestamp_sql().into())
                || (r#type == DATE.into_type()?
                    && definition.default == platform.get_current_date_sql().into())
                || (r#type == TIME.into_type()?
                    && definition.default == platform.get_current_time_sql().into())
            {
                return Ok(None);
            }

            if r#type == STRING.into_type()? {
                definition.length = Some(definition.length.unwrap_or(255));
            }

            sql.push(format!(
                "ALTER TABLE {} ADD COLUMN {}",
                table_name,
                this.get_column_declaration_sql(&column.get_quoted_name(&platform), &definition)?
            ));
        }

        let (res, mut table_sql) = this.on_schema_alter_table(diff, vec![])?;
        if !res {
            if let Some(new_name) = &diff.new_name {
                let new_name = Identifier::new(new_name, false);
                sql.push(format!(
                    "ALTER TABLE {} RENAME TO {}",
                    table_name,
                    new_name.get_quoted_name(&platform)
                ));
            }
        }

        sql.append(&mut table_sql);
        sql.append(&mut column_sql);

        Ok(Some(sql))
    }
}

/// Replace the column with the given name with the new column.
fn replace_column(
    table_name: &str,
    mut columns: HashMap<String, Column>,
    column_name: &str,
    column: &Column,
) -> Result<HashMap<String, Column>> {
    let column_name = column_name.to_lowercase();
    if let Occupied(mut e) = columns.entry(column_name.clone()) {
        e.insert(column.clone());
        Ok(columns)
    } else {
        Err(Error::column_does_not_exist(&column_name, table_name))
    }
}

pub fn get_alter_table_sql<T: AbstractSQLiteSchemaManager + Sync + ?Sized>(
    this: &T,
    diff: &mut TableDiff,
) -> Result<Vec<String>> {
    let platform = this.get_platform()?;
    if let Some(sql) = get_simple_alter_table_sql(this, diff)? {
        Ok(sql)
    } else if let Some(from_table) = diff.from_table {
        let mut column_sql = vec![];
        let mut columns = HashMap::new();
        let mut old_column_names = HashMap::new();
        let mut new_column_names = HashMap::new();

        for column in from_table.get_columns() {
            let column_name = column.get_name().to_lowercase();
            columns.insert(column_name.clone(), column.clone());

            let quoted_name = column.get_quoted_name(&platform);
            old_column_names.insert(column_name.clone(), quoted_name.clone());
            new_column_names.insert(column_name, quoted_name);
        }

        for column in &diff.removed_columns {
            let (res, new_column_sql) =
                this.on_schema_alter_table_remove_column(column, diff, column_sql)?;
            column_sql = new_column_sql;
            if res {
                continue;
            }

            let column_name = column.get_name().to_lowercase();
            if columns.remove(&column_name).is_none() {
                continue;
            }

            old_column_names.remove(&column_name);
            new_column_names.remove(&column_name);
        }

        for (old_column_name, column) in &diff.renamed_columns {
            let (res, new_column_sql) = this.on_schema_alter_table_rename_column(
                old_column_name,
                column,
                diff,
                column_sql,
            )?;
            column_sql = new_column_sql;
            if res {
                continue;
            }

            let old_column_name = old_column_name.to_lowercase();
            columns = replace_column(&diff.name, columns, &old_column_name, column)?;

            if new_column_names.get(&old_column_name).is_some() {
                new_column_names.insert(old_column_name, column.get_quoted_name(&platform));
            }
        }

        for column_diff in &diff.changed_columns {
            let (res, new_column_sql) =
                this.on_schema_alter_table_change_column(column_diff, diff, column_sql)?;
            column_sql = new_column_sql;
            if res {
                continue;
            }

            let old_column_name = column_diff.get_old_column_name().get_name().to_lowercase();
            columns = replace_column(&diff.name, columns, &old_column_name, &column_diff.column)?;

            if new_column_names.get(&old_column_name).is_some() {
                new_column_names.insert(
                    old_column_name,
                    column_diff.column.get_quoted_name(&platform),
                );
            }
        }

        for column in &diff.added_columns {
            let (res, new_column_sql) =
                this.on_schema_alter_table_add_column(column, diff, column_sql)?;
            column_sql = new_column_sql;
            if res {
                continue;
            }

            columns.insert(column.get_name().to_lowercase(), column.clone());
        }

        let mut sql = vec![];
        let (res, mut table_sql) = this.on_schema_alter_table(diff, vec![])?;
        if !res {
            let data_table = Identifier::new(format!("__temp__{}", from_table.get_name()), false);
            let mut new_table = from_table.template();
            new_table.set_alter(true);
            new_table.add_columns(columns.into_values());

            if let Some(primary) = get_primary_index_in_altered_table(diff, from_table) {
                new_table.add_index(primary);
            }

            new_table
                .add_foreign_keys(get_foreign_keys_in_altered_table(diff, from_table).into_iter());

            let old_columns = old_column_names.into_values().join(", ");
            let new_columns = new_column_names.into_values().join(", ");
            let data_table_quoted = data_table.get_quoted_name(&platform);

            let mut sql = this.get_pre_alter_table_index_foreign_key_sql(diff)?;
            sql.push(format!(
                "CREATE TEMPORARY TABLE {} AS SELECT {} FROM {}",
                data_table_quoted,
                old_columns,
                from_table.get_table_name().get_quoted_name(&platform)
            ));
            sql.push(this.get_drop_table_sql(from_table.get_table_name())?);

            let mut create_sql = this.get_create_table_sql(&new_table, None)?;
            sql.append(&mut create_sql);

            sql.push(format!(
                "INSERT INTO {} ({}) SELECT {} FROM {}",
                new_table.get_quoted_name(&platform),
                new_columns,
                old_columns,
                data_table_quoted
            ));
            sql.push(this.get_drop_table_sql(&data_table)?);

            if let Some(new_name) = diff.get_new_name() {
                sql.push(format!(
                    "ALTER TABLE {} RENAME TO {}",
                    new_table.get_quoted_name(&platform),
                    new_name.get_quoted_name(&platform)
                ));
            }

            sql.append(&mut this.get_post_alter_table_index_foreign_key_sql(diff)?);
        }

        sql.append(&mut table_sql);
        sql.append(&mut column_sql);

        Ok(sql)
    } else {
        Err(Error::new(ErrorKind::UnknownError, "Sqlite platform requires for alter table the table diff with reference to original table schema"))
    }
}

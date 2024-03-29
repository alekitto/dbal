use super::sqlite_platform::AbstractSQLitePlatform;
use crate::driver::sqlite::platform::AbstractSQLiteSchemaManager;
use crate::driver::statement_result::StatementResult;
use crate::error::ErrorKind;
use crate::platform::{default, CreateFlags, DatabasePlatform, DateIntervalUnit, TrimMode};
use crate::r#type::{IntoType, BIGINT, DATE, DATETIME, INTEGER, STRING, TIME};
use crate::schema::{
    Asset, Column, ColumnData, ForeignKeyConstraint, Identifier, Index, SchemaManager, Table,
    TableDiff, TableOptions,
};
use crate::schema::{ColumnList, IntoIdentifier};
use crate::{params, Error, Parameters, Result, Row, TransactionIsolationLevel, Value};
use creed::schema::IndexList;
use itertools::Itertools;
use regex::{escape, Regex};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::str::FromStr;

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
    if column.autoincrement {
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
    if column.autoincrement {
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
    if column.autoincrement {
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
            let key_columns = vec.iter().unique();
            let joined_columns = { key_columns.clone().join(", ") };
            for key_column in key_columns.into_iter() {
                if let Some(column) = columns
                    .iter()
                    .find(|c| c.name.cmp(key_column) == Ordering::Equal)
                {
                    if column.autoincrement {
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
        let length = length.unwrap_or(255);
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
        UNION ALL SELECT name FROM sqlite_temp_master \
        WHERE type = 'table' ORDER BY name"
        .to_string())
}

pub fn get_list_views_sql() -> Result<String> {
    Ok("SELECT name AS viewname, NULL AS schemaname, sql AS definition FROM sqlite_master WHERE type='view' AND sql NOT NULL".to_string())
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

pub fn get_truncate_table_sql(
    this: &dyn SchemaManager,
    table_name: &dyn IntoIdentifier,
) -> Result<String> {
    let platform = this.get_platform()?;
    Ok(format!(
        "DELETE FROM {}",
        table_name
            .into_identifier()
            .get_quoted_name(platform.as_dyn())
    ))
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

    for column in from_table.columns() {
        let name = column.get_name();
        columns.insert(name.to_lowercase(), name.into_owned());
    }

    for removed_column in &diff.removed_columns {
        let column_name = removed_column.get_name().to_lowercase();
        columns.remove(&column_name);
    }

    for (old_column_name, column) in &diff.renamed_columns {
        let column_name = column.get_name().into_owned();
        columns.insert(old_column_name.to_lowercase(), column_name.clone());
        columns.insert(column_name.to_lowercase(), column_name);
    }

    for column_diff in &diff.changed_columns {
        let column_name = column_diff.column.get_name().into_owned();
        columns.insert(
            column_diff.get_old_column_name().get_name().to_lowercase(),
            column_name.clone(),
        );
        columns.insert(column_name.to_lowercase(), column_name);
    }

    for column in &diff.added_columns {
        let column_name = column.get_name().into_owned();
        columns.insert(column_name.to_lowercase(), column_name);
    }

    columns
}

fn get_indexes_in_altered_table(diff: &TableDiff, from_table: &Table) -> Vec<Index> {
    let mut empty_index_idx = 0;
    let indexes = from_table.indices().clone();
    let column_names = get_column_names_in_altered_table(diff, from_table);

    #[allow(clippy::needless_collect)]
    let renamed_indexes = diff
        .renamed_indexes
        .iter()
        .map(|i| i.0.to_lowercase())
        .collect::<Vec<_>>();

    let mut new_indexes = HashMap::new();
    let mut add_index = |new_indexes: &mut HashMap<String, Index>, index: Index| {
        let name = if !index.get_name().is_empty() {
            index.get_name().into_owned()
        } else {
            empty_index_idx += 1;
            format!("{}", empty_index_idx)
        };

        new_indexes.insert(name, index);
    };

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
            add_index(
                &mut new_indexes,
                Index::new(
                    index.get_name(),
                    &index_columns,
                    index.is_unique(),
                    index.is_primary(),
                    index.get_flags(),
                    index.get_options().clone(),
                ),
            );
        } else {
            add_index(&mut new_indexes, index.clone());
        }
    }

    for index in &diff.removed_indexes {
        let index_name = index.get_name().to_lowercase();
        if !index_name.is_empty() {
            if !indexes.iter().any(|ix| ix.get_name() == index_name) {
                add_index(&mut new_indexes, index.clone());
            } else {
                new_indexes.remove(index.get_name().as_ref());
            }
        } else {
            add_index(&mut new_indexes, index.clone());
        }
    }

    for index in &diff.changed_indexes {
        add_index(&mut new_indexes, index.clone());
    }

    for index in &diff.added_indexes {
        add_index(&mut new_indexes, index.clone());
    }

    for (_, index) in &diff.renamed_indexes {
        add_index(&mut new_indexes, index.clone());
    }

    new_indexes
        .into_values()
        .sorted_by_key(|i| i.get_name().to_string())
        .collect()
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
                if column_name != &local_column.get_name() {
                    continue;
                }

                changed = true
            } else {
                continue 'a;
            }
        }

        if changed {
            let mut fk = ForeignKeyConstraint::new(
                &local_columns,
                &constraint
                    .get_foreign_columns()
                    .iter()
                    .map(|col| col.get_name())
                    .collect::<Vec<_>>(),
                constraint.get_foreign_table().get_name(),
                constraint.get_options(),
                constraint.on_update,
                constraint.on_delete,
            );
            fk.set_name(constraint.get_name().as_ref());

            new_foreign_keys.push(fk);
        } else {
            new_foreign_keys.push(constraint.clone());
        }
    }

    let removed_key_names: Vec<String> = diff
        .removed_foreign_keys
        .iter()
        .map(|fk| fk.get_name().to_lowercase())
        .collect();
    new_foreign_keys.retain(|fk| !removed_key_names.contains(&fk.get_name().to_lowercase()));

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
                || definition.autoincrement
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
    mut columns: ColumnList,
    column_name: &str,
    column: &Column,
) -> Result<ColumnList> {
    let column_name = column_name.to_lowercase();
    if let Some((pos, _)) = columns.get_position(column_name.as_str()) {
        columns.replace(pos, column.clone());
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
        let mut columns = ColumnList::default();
        let mut old_column_names = vec![];

        for column in from_table.columns() {
            columns.push(column.clone());
            old_column_names.push(column.get_quoted_name(&platform));
        }

        for column in &diff.removed_columns {
            let (res, new_column_sql) =
                this.on_schema_alter_table_remove_column(column, diff, column_sql)?;
            column_sql = new_column_sql;
            if res {
                continue;
            }

            let column_name = column.get_name().to_lowercase();
            if columns.remove(column_name.as_str()).is_some() {
                if let Some((p, _)) = old_column_names
                    .iter()
                    .find_position(|c| *c == &column_name)
                {
                    old_column_names.remove(p);
                }
            }
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

            columns = replace_column(&diff.name, columns, old_column_name, column)?;
        }

        for column_diff in &diff.changed_columns {
            let (res, new_column_sql) =
                this.on_schema_alter_table_change_column(column_diff, diff, column_sql)?;
            column_sql = new_column_sql;
            if res {
                continue;
            }

            let old_column_identifier = column_diff.get_old_column_name();
            let old_column_name = old_column_identifier.get_name().to_lowercase();
            columns = replace_column(&diff.name, columns, &old_column_name, &column_diff.column)?;
        }

        for column in &diff.added_columns {
            let (res, new_column_sql) =
                this.on_schema_alter_table_add_column(column, diff, column_sql)?;
            column_sql = new_column_sql;
            if res {
                continue;
            }

            columns.push(column.clone());
        }

        let mut sql = vec![];
        let (res, mut table_sql) = this.on_schema_alter_table(diff, vec![])?;
        if !res {
            let old_columns = old_column_names.join(", ");
            let added_columns = diff
                .added_columns
                .iter()
                .map(|c| c.get_name())
                .collect::<Vec<_>>();
            let new_columns = columns
                .filter(|c| !added_columns.contains(&c.get_name()))
                .map(|c| c.get_quoted_name(&platform))
                .join(", ");

            let data_table = Identifier::new(format!("__temp__{}", from_table.get_name()), false);
            let data_table_quoted = data_table.get_quoted_name(&platform);

            let mut new_table = from_table.template();
            new_table.set_alter(true);
            new_table.add_columns(columns.into_iter());

            if let Some(primary) = get_primary_index_in_altered_table(diff, from_table) {
                new_table.add_index(primary);
            }

            new_table.add_foreign_keys_raw(
                get_foreign_keys_in_altered_table(diff, from_table).into_iter(),
            );

            sql = this.get_pre_alter_table_index_foreign_key_sql(diff)?;
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

pub fn get_portable_table_column_definition(
    this: &dyn SchemaManager,
    table_column: &Row,
) -> Result<Column> {
    let platform = this.get_platform()?;
    let ty = String::try_from(table_column.get("type").unwrap())?;
    let parts = Regex::new("[()]")?.split(&ty).collect::<Vec<_>>();

    let db_type = parts.first().unwrap().to_lowercase();
    let mut length = parts.get(1).map(ToString::to_string);

    let unsigned = db_type.contains(" unsigned");
    let db_type = db_type.replace(" unsigned", "");

    let mut fixed = false;
    let ty = platform.get_type_mapping(&db_type)?;
    let default = table_column.get("dflt_value").unwrap().to_string();

    let default = if default == "NULL" {
        None
    } else {
        let rx = Regex::new("^'(.*)'$")?;
        Some(if let Some(matches) = rx.captures(&default) {
            matches.get(1).unwrap().as_str().replace("''", "'")
        } else {
            default
        })
    };

    let notnull = table_column.get("notnull").unwrap() == &Value::Int(1);
    let name = String::try_from(
        table_column
            .get("name")
            .cloned()
            .unwrap_or(Value::String("".to_string())),
    )?;

    let mut precision = None;
    let mut scale = None;

    match db_type.as_str() {
        "char" => {
            fixed = true;
        }
        "float" | "double" | "real" | "decimal" | "numeric" => {
            if let Some(len) = length {
                let len = if len.contains(',') {
                    len
                } else {
                    format!("{},0", len)
                };

                let l = len
                    .split(',')
                    .map(|x| x.trim().to_string())
                    .collect::<Vec<_>>();
                precision = usize::from_str(l.first().unwrap()).ok();
                scale = usize::from_str(l.get(1).unwrap()).ok();
            }

            length = None;
        }

        _ => {
            // Do nothing
        }
    }

    let r#type = ty.into_type()?;
    let default = if let Some(default) = default {
        r#type.convert_to_value(&Value::String(default), &platform)?
    } else {
        Value::NULL
    };

    let mut column = Column::new(name, r#type.into_type()?);
    column.set_length(length.and_then(|x| usize::from_str(&x).ok()));
    column.set_jsonb(unsigned);
    column.set_fixed(fixed);
    column.set_notnull(notnull);
    column.set_default(default);
    column.set_precision(precision);
    column.set_scale(scale);
    column.set_autoincrement(false);

    Ok(column)
}

pub async fn get_portable_table_indexes_list(
    this: &dyn SchemaManager,
    table_indexes: Vec<Row>,
    table_name: String,
) -> Result<IndexList> {
    let mut buffer = vec![];
    let mut primary = this
        .get_connection()
        .fetch_all(
            "SELECT * FROM PRAGMA_TABLE_INFO (?)",
            params![0 => Value::String(table_name.clone())],
        )
        .await?;

    primary.sort_by(|a, b| {
        let a_pk = a.get("pk").unwrap();
        let b_pk = b.get("pk").unwrap();
        if a_pk == b_pk {
            let a = a.get("cid").unwrap();
            let b = b.get("cid").unwrap();

            i32::try_from(a).unwrap().cmp(&i32::try_from(b).unwrap())
        } else {
            i32::try_from(a_pk)
                .unwrap()
                .cmp(&i32::try_from(b_pk).unwrap())
        }
    });

    for row in primary {
        let pk = row.get("pk").unwrap();
        if pk == &Value::Int(0) || pk == &Value::String("0".into()) {
            continue;
        }

        buffer.push(Row::new(
            vec![
                "key_name".into(),
                "primary".into(),
                "non_unique".into(),
                "column_name".into(),
                "where".into(),
                "flags".into(),
            ],
            vec![
                Value::String("primary".into()),
                Value::Boolean(true),
                Value::Boolean(false),
                row.get("name")?.clone(),
                Value::NULL,
                Value::NULL,
            ],
        ));
    }

    let conn = this.get_connection();
    for row in table_indexes {
        let key_name = String::try_from(row.get("name")?).unwrap();
        if key_name.starts_with("sqlite_") {
            continue;
        }

        let index_info = conn
            .fetch_all(
                "SELECT * FROM PRAGMA_INDEX_INFO (?)",
                params![0 => Value::String(key_name.clone())],
            )
            .await?;
        for col_row in index_info {
            let row = Row::new(
                vec![
                    "key_name".into(),
                    "primary".into(),
                    "non_unique".into(),
                    "column_name".into(),
                    "where".into(),
                    "flags".into(),
                ],
                vec![
                    Value::String(key_name.clone()),
                    Value::Boolean(false),
                    Value::Boolean(i32::try_from(row.get("unique")?.clone())? == 0),
                    col_row.get("name")?.clone(),
                    Value::NULL,
                    Value::NULL,
                ],
            );

            buffer.push(row);
        }
    }

    default::get_portable_table_indexes_list(this, buffer, table_name)
}

pub async fn select_foreign_key_columns(
    this: &dyn SchemaManager,
    table_name: Option<String>,
) -> Result<StatementResult> {
    let mut sql = r#"
SELECT t.name AS table_name,
p.*
    FROM sqlite_master t
JOIN pragma_foreign_key_list(t.name) p
ON p."seq" != "-1"
"#
    .to_string();

    let mut conditions = vec![
        "t.type = 'table'",
        "t.name NOT IN ('geometry_columns', 'spatial_ref_sys', 'sqlite_sequence')",
    ];

    let mut params = vec![];

    if let Some(table_name) = table_name {
        conditions.push("t.name = ?");
        params.push(table_name.replace('.', "__"));
    }

    sql += format!(
        " WHERE {} ORDER BY t.name, p.id DESC, p.seq",
        conditions.join(" AND ")
    )
    .as_str();

    this.get_connection()
        .query(sql, Parameters::from(params))
        .await
}

fn parse_table_comment_from_sql(
    platform: &dyn DatabasePlatform,
    table: &str,
    sql: &str,
) -> Result<String> {
    let pattern = Regex::new(&format!(
        "\\s*CREATE\\s+TABLE(?:\\W{}\\W|\\W{}\\W)((?:\\s*--[^\\n]*\\n?)+)",
        platform.quote_single_identifier(table),
        escape(table)
    ))?;

    let Some(captures) = pattern.captures(sql) else {
        return Ok("".to_string());
    };
    let comment = captures
        .get(1)
        .unwrap()
        .as_str()
        .trim_end_matches('\n')
        .trim_start_matches(&Regex::new("^\\s*--")?)
        .to_string();

    Ok(comment)
}

pub async fn fetch_table_options_by_table(
    this: &dyn SchemaManager,
    table_name: Option<String>,
) -> Result<HashMap<String, Row>> {
    let tables = if let Some(table_name) = table_name {
        vec![table_name]
    } else {
        this.list_table_names().await?
    };

    let mut table_options = HashMap::new();
    for table in tables {
        let Some(create_sql_row) = this
            .get_connection()
            .query(
                r#"
    SELECT sql
    FROM (
        SELECT *
            FROM sqlite_master
        UNION ALL
        SELECT *
            FROM sqlite_temp_master
    )
    WHERE type = 'table'
AND name = ?
"#,
                params![0 => Value::from(&table)],
            )
            .await?
            .fetch_one()
            .await?
        else {
            continue;
        };

        let create_sql = create_sql_row.get("sql").unwrap().to_string();
        let Ok(comment) =
            parse_table_comment_from_sql(this.get_platform()?.as_dyn(), &table, &create_sql)
        else {
            continue;
        };

        table_options.insert(
            table,
            Row::new(vec!["comment".to_string()], vec![Value::String(comment)]),
        );
    }

    Ok(table_options)
}

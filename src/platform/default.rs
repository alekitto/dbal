use super::{CreateFlags, DatabasePlatform, DateIntervalUnit, LockMode, TrimMode};
use crate::driver::statement_result::StatementResult;
use crate::event::{
    SchemaAlterTableAddColumnEvent, SchemaAlterTableRemoveColumnEvent, SchemaCreateTableEvent,
    SchemaDropTableEvent,
};
use crate::r#type::{
    IntoType, TypeManager, TypePtr, BIGINT, BOOLEAN, DATE, DATETIME, DATETIMETZ, INTEGER, TIME,
};
use crate::schema::{
    get_database, string_from_value, Asset, CheckConstraint, Column, ColumnData, ColumnDiff,
    ForeignKeyConstraint, ForeignKeyReferentialAction, Identifier, Index, IndexOptions,
    IntoIdentifier, SchemaManager, Sequence, Table, TableDiff, TableOptions, UniqueConstraint,
    View,
};
use crate::util::{filter_asset_names, function_name};
use crate::{
    params, AsyncResult, Error, Result, Row, SchemaAlterTableChangeColumnEvent,
    SchemaAlterTableEvent, SchemaAlterTableRenameColumnEvent, SchemaColumnDefinitionEvent,
    SchemaCreateTableColumnEvent, SchemaIndexDefinitionEvent, TransactionIsolationLevel, Value,
};
use itertools::Itertools;
use regex::Regex;
use std::borrow::Cow;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::fmt::Display;

pub fn get_ascii_string_type_declaration_sql(
    this: &dyn DatabasePlatform,
    column: &ColumnData,
) -> Result<String> {
    this.get_string_type_declaration_sql(column)
}

pub fn get_string_type_declaration_sql(
    this: &dyn DatabasePlatform,
    column: &ColumnData,
) -> Result<String> {
    let fixed = column.fixed;
    this.get_varchar_type_declaration_sql_snippet(column.length, fixed)
}

pub fn get_binary_type_declaration_sql(
    this: &dyn DatabasePlatform,
    column: &ColumnData,
) -> Result<String> {
    let fixed = column.fixed;
    this.get_binary_type_declaration_sql_snippet(column.length, fixed)
}

pub fn get_guid_type_declaration_sql(
    this: &dyn DatabasePlatform,
    column: &ColumnData,
) -> Result<String> {
    let mut column = column.clone();
    let _ = column.length.insert(36);
    column.fixed = true;

    this.get_string_type_declaration_sql(&column)
}

pub fn get_json_type_declaration_sql(
    this: &dyn DatabasePlatform,
    column: &ColumnData,
) -> Result<String> {
    this.get_clob_type_declaration_sql(column)
}

pub fn get_identifier_quote_character() -> char {
    '"'
}

pub fn get_length_expression(column: &str) -> Result<String> {
    Ok(format!("LENGTH({})", column))
}

pub fn get_mod_expression(expression1: &str, expression2: &str) -> Result<String> {
    Ok(format!("MOD({}, {})", expression1, expression2))
}

pub fn get_trim_expression(str: &str, mode: TrimMode, char: Option<String>) -> Result<String> {
    let mut expression = "".to_string();

    match mode {
        TrimMode::Leading => expression = "LEADING ".to_string(),
        TrimMode::Trailing => expression = "TRAILING ".to_string(),
        TrimMode::Both => expression = "BOTH ".to_string(),
        _ => {}
    }

    if let Some(ref char) = char {
        expression += &format!("{} ", char.clone());
    }

    if mode != TrimMode::Unspecified || char.is_some() {
        expression += "FROM ";
    }

    Ok(format!("TRIM({}{})", expression, str))
}

pub fn get_substring_expression(
    string: &str,
    start: usize,
    length: Option<usize>,
) -> Result<String> {
    Ok(if let Some(len) = length {
        format!("SUBSTRING({} FROM {} FOR {})", string, start, len)
    } else {
        format!("SUBSTRING({} FROM {})", string, start)
    })
}

pub fn get_concat_expression(strings: Vec<&str>) -> Result<String> {
    Ok(strings.join(" || "))
}

pub fn get_date_add_seconds_expression(
    this: &dyn DatabasePlatform,
    date: &str,
    seconds: i64,
) -> Result<String> {
    this.get_date_arithmetic_interval_expression(date, "+", seconds, DateIntervalUnit::Second)
}

pub fn get_date_sub_seconds_expression(
    this: &dyn DatabasePlatform,
    date: &str,
    seconds: i64,
) -> Result<String> {
    this.get_date_arithmetic_interval_expression(date, "-", seconds, DateIntervalUnit::Second)
}

pub fn get_date_add_minutes_expression(
    this: &dyn DatabasePlatform,
    date: &str,
    minutes: i64,
) -> Result<String> {
    this.get_date_arithmetic_interval_expression(date, "+", minutes, DateIntervalUnit::Minute)
}

pub fn get_date_sub_minutes_expression(
    this: &dyn DatabasePlatform,
    date: &str,
    minutes: i64,
) -> Result<String> {
    this.get_date_arithmetic_interval_expression(date, "-", minutes, DateIntervalUnit::Minute)
}

pub fn get_date_add_hour_expression(
    this: &dyn DatabasePlatform,
    date: &str,
    hours: i64,
) -> Result<String> {
    this.get_date_arithmetic_interval_expression(date, "+", hours, DateIntervalUnit::Hour)
}

pub fn get_date_sub_hour_expression(
    this: &dyn DatabasePlatform,
    date: &str,
    hours: i64,
) -> Result<String> {
    this.get_date_arithmetic_interval_expression(date, "-", hours, DateIntervalUnit::Hour)
}

pub fn get_date_add_days_expression(
    this: &dyn DatabasePlatform,
    date: &str,
    days: i64,
) -> Result<String> {
    this.get_date_arithmetic_interval_expression(date, "+", days, DateIntervalUnit::Day)
}

pub fn get_date_sub_days_expression(
    this: &dyn DatabasePlatform,
    date: &str,
    days: i64,
) -> Result<String> {
    this.get_date_arithmetic_interval_expression(date, "-", days, DateIntervalUnit::Day)
}

pub fn get_date_add_weeks_expression(
    this: &dyn DatabasePlatform,
    date: &str,
    weeks: i64,
) -> Result<String> {
    this.get_date_arithmetic_interval_expression(date, "+", weeks, DateIntervalUnit::Week)
}

pub fn get_date_sub_weeks_expression(
    this: &dyn DatabasePlatform,
    date: &str,
    weeks: i64,
) -> Result<String> {
    this.get_date_arithmetic_interval_expression(date, "-", weeks, DateIntervalUnit::Week)
}

pub fn get_date_add_month_expression(
    this: &dyn DatabasePlatform,
    date: &str,
    months: i64,
) -> Result<String> {
    this.get_date_arithmetic_interval_expression(date, "+", months, DateIntervalUnit::Month)
}

pub fn get_date_sub_month_expression(
    this: &dyn DatabasePlatform,
    date: &str,
    months: i64,
) -> Result<String> {
    this.get_date_arithmetic_interval_expression(date, "-", months, DateIntervalUnit::Month)
}

pub fn get_date_add_quarters_expression(
    this: &dyn DatabasePlatform,
    date: &str,
    quarters: i64,
) -> Result<String> {
    this.get_date_arithmetic_interval_expression(date, "+", quarters, DateIntervalUnit::Quarter)
}

pub fn get_date_sub_quarters_expression(
    this: &dyn DatabasePlatform,
    date: &str,
    quarters: i64,
) -> Result<String> {
    this.get_date_arithmetic_interval_expression(date, "-", quarters, DateIntervalUnit::Quarter)
}

pub fn get_date_add_years_expression(
    this: &dyn DatabasePlatform,
    date: &str,
    years: i64,
) -> Result<String> {
    this.get_date_arithmetic_interval_expression(date, "+", years, DateIntervalUnit::Year)
}

pub fn get_date_sub_years_expression(
    this: &dyn DatabasePlatform,
    date: &str,
    years: i64,
) -> Result<String> {
    this.get_date_arithmetic_interval_expression(date, "-", years, DateIntervalUnit::Year)
}

pub fn get_bit_and_comparison_expression(
    value1: &dyn Display,
    value2: &dyn Display,
) -> Result<String> {
    Ok(format!("({} & {})", value1, value2))
}

pub fn get_bit_or_comparison_expression(
    value1: &dyn Display,
    value2: &dyn Display,
) -> Result<String> {
    Ok(format!("({} | {})", value1, value2))
}

pub fn get_for_update_sql() -> Result<String> {
    Ok("FOR UPDATE".to_string())
}

pub fn append_lock_hint(from_clause: &str, lock_mode: LockMode) -> Result<String> {
    match lock_mode {
        LockMode::None
        | LockMode::Optimistic
        | LockMode::PessimisticRead
        | LockMode::PessimisticWrite => Ok(from_clause.to_string()),
    }
}

pub fn get_read_lock_sql(this: &dyn DatabasePlatform) -> Result<String> {
    this.get_for_update_sql()
}

pub fn get_write_lock_sql(this: &dyn DatabasePlatform) -> Result<String> {
    this.get_for_update_sql()
}

pub fn get_drop_table_sql(
    this: &dyn SchemaManager,
    table_name: &dyn IntoIdentifier,
) -> Result<String> {
    let platform = this.get_platform()?;
    let table_arg = table_name
        .into_identifier()
        .get_quoted_name(platform.as_dyn());
    let ev = platform
        .get_event_manager()
        .dispatch_sync(SchemaDropTableEvent::new(
            table_arg.clone(),
            platform.clone(),
        ))?;

    if ev.is_default_prevented() {
        if let Some(ref sql) = ev.sql {
            return Ok(sql.clone());
        }
    }

    Ok(format!("DROP TABLE {}", table_arg))
}

pub fn get_drop_temporary_table_sql(
    this: &dyn SchemaManager,
    table: &Identifier,
) -> Result<String> {
    this.get_drop_table_sql(table)
}

pub fn get_drop_index_sql(platform: &dyn DatabasePlatform, index: &Identifier) -> Result<String> {
    let index_name = index.get_quoted_name(platform);
    Ok(format!("DROP INDEX {}", index_name))
}

pub fn get_drop_constraint_sql(
    platform: &dyn DatabasePlatform,
    constraint: &Identifier,
    table_name: &Identifier,
) -> Result<String> {
    let constraint = constraint.get_quoted_name(platform);
    let table_name = table_name.get_quoted_name(platform);
    Ok(format!(
        "ALTER TABLE {} DROP CONSTRAINT {}",
        table_name, constraint
    ))
}

pub fn get_drop_foreign_key_sql(
    platform: &dyn DatabasePlatform,
    foreign_key: &dyn IntoIdentifier,
    table_name: &dyn IntoIdentifier,
) -> Result<String> {
    let foreign_key = foreign_key.into_identifier().get_quoted_name(platform);
    let table_name = table_name.into_identifier().get_quoted_name(platform);
    Ok(format!(
        "ALTER TABLE {} DROP FOREIGN KEY {}",
        table_name, foreign_key
    ))
}

pub fn get_drop_unique_constraint_sql(
    this: &dyn SchemaManager,
    name: &Identifier,
    table_name: &Identifier,
) -> Result<String> {
    this.get_drop_constraint_sql(name, table_name)
}

pub fn get_create_table_sql(
    this: &dyn SchemaManager,
    table: &Table,
    create_flags: Option<CreateFlags>,
) -> Result<Vec<String>> {
    let platform = this.get_platform()?;
    let create_flags = create_flags.unwrap_or(CreateFlags::CREATE_INDEXES);
    if table.is_empty() {
        return Err(Error::no_columns_specified_for_table(
            table.get_table_name(),
        ));
    }

    let mut options = TableOptions::default();
    if create_flags.contains(CreateFlags::CREATE_INDEXES) {
        for index in table.get_indices() {
            if !index.is_primary() {
                let _ = options
                    .indexes
                    .insert(index.get_quoted_name(platform.as_dyn()), index.clone());
                continue;
            }

            options.primary = Some((index.get_quoted_columns(platform.as_dyn()), index.clone()));
        }

        for unique_constraint in table.get_unique_constraints() {
            options.unique_constraints.insert(
                unique_constraint.get_quoted_name(platform.as_dyn()),
                unique_constraint.clone(),
            );
        }
    }

    if create_flags.contains(CreateFlags::CREATE_FOREIGN_KEYS) {
        for fk_constraint in table.get_foreign_keys() {
            options.foreign_keys.push(fk_constraint.clone());
        }
    }

    let mut column_sql = vec![];
    let mut columns = vec![];

    for column in table.get_columns() {
        let e = platform
            .get_event_manager()
            .dispatch_sync(SchemaCreateTableColumnEvent::new(
                table,
                column,
                platform.clone(),
            ))?;
        let mut sql = e.get_sql();
        column_sql.append(&mut sql);

        if e.is_default_prevented() {
            continue;
        }

        let mut column_data = column.generate_column_data(platform.as_dyn());
        let comment = this.get_column_comment(column)?;
        column_data.comment = if comment.is_empty() {
            None
        } else {
            Some(comment)
        };

        if let Some(p) = &options.primary {
            if p.0.iter().any(|n| n.eq(&column_data.name)) {
                column_data.primary = true;
            }
        }

        columns.push(column_data);
    }

    let e = platform
        .get_event_manager()
        .dispatch_sync(SchemaCreateTableEvent::new(table, platform.clone()))?;
    if e.is_default_prevented() {
        let mut sql = e.get_sql();
        sql.append(&mut column_sql);

        return Ok(sql);
    }

    let mut sql =
        this._get_create_table_sql(table.get_table_name(), columns.as_slice(), &options)?;

    if platform.supports_comment_on_statement() {
        if let Some(comment) = table.get_comment() {
            sql.push(this.get_comment_on_table_sql(table.get_table_name(), comment)?);
        }
    }

    sql.append(&mut column_sql);

    if platform.supports_comment_on_statement() {
        for column in table.get_columns() {
            let comment = this.get_column_comment(column)?;
            if comment.is_empty() {
                continue;
            }

            sql.push(this.get_comment_on_column_sql(table.get_table_name(), column, &comment)?);
        }
    }

    Ok(sql)
}

pub fn get_create_tables_sql(this: &dyn SchemaManager, tables: &[Table]) -> Result<Vec<String>> {
    let mut sql = vec![];
    for table in tables {
        let mut other_sql = this.get_create_table_sql(table, Some(CreateFlags::CREATE_INDEXES))?;
        sql.append(&mut other_sql);
    }

    for table in tables {
        for fk in table.get_foreign_keys() {
            sql.push(this.get_create_foreign_key_sql(fk, table.get_table_name())?)
        }
    }

    Ok(sql)
}

pub fn get_drop_tables_sql(this: &dyn SchemaManager, tables: &[Table]) -> Result<Vec<String>> {
    let mut sql = vec![];
    for table in tables {
        for fk in table.get_foreign_keys() {
            sql.push(this.get_drop_foreign_key_sql(fk, table.get_table_name())?)
        }
    }

    for table in tables {
        let other_sql = this.get_drop_table_sql(table.get_table_name())?;
        sql.push(other_sql);
    }

    Ok(sql)
}

pub fn get_comment_on_table_sql(
    platform: &dyn DatabasePlatform,
    table_name: &Identifier,
    comment: &str,
) -> Result<String> {
    Ok(format!(
        "COMMENT ON TABLE {} IS {}",
        table_name.get_quoted_name(platform),
        platform.quote_string_literal(comment)
    ))
}

pub fn get_comment_on_column_sql(
    platform: &dyn DatabasePlatform,
    table_name: &dyn IntoIdentifier,
    column: &dyn IntoIdentifier,
    comment: &str,
) -> Result<String> {
    Ok(format!(
        "COMMENT ON COLUMN {}.{} IS {}",
        table_name.into_identifier().get_quoted_name(platform),
        column.into_identifier().get_quoted_name(platform),
        platform.quote_string_literal(comment)
    ))
}

pub fn get_inline_column_comment_sql(this: &dyn DatabasePlatform, comment: &str) -> Result<String> {
    if !this.supports_inline_column_comments() {
        Err(Error::platform_feature_unsupported(
            "inline column comment unsupported for this platform",
        ))
    } else {
        Ok(format!("COMMENT {}", this.quote_string_literal(comment)))
    }
}

pub fn _get_create_table_sql(
    this: &dyn SchemaManager,
    name: &Identifier,
    columns: &[ColumnData],
    options: &TableOptions,
) -> Result<Vec<String>> {
    let mut column_list_sql = this.get_column_declaration_list_sql(columns)?;
    let platform = this.get_platform()?;

    if !options.unique_constraints.is_empty() {
        for (index, definition) in &options.unique_constraints {
            column_list_sql += ", ";
            column_list_sql += &*this.get_unique_constraint_declaration_sql(index, definition)?;
        }
    }

    if let Some(primary) = &options.primary {
        let v: Vec<_> = primary.0.clone().into_iter().unique().collect();
        column_list_sql += &format!(", PRIMARY KEY({})", v.join(", "))
    }

    if !options.indexes.is_empty() {
        for (index, definition) in &options.indexes {
            column_list_sql += ", ";
            column_list_sql += &*this.get_index_declaration_sql(index, definition)?;
        }
    }

    let check = this.get_check_declaration_sql(columns)?;
    let query = format!(
        "CREATE TABLE {} ({}{}{})",
        name.get_quoted_name(platform.as_dyn()),
        column_list_sql,
        if check.is_empty() { "" } else { ", " },
        check
    );

    let mut sql = vec![query];
    for fk in &options.foreign_keys {
        sql.push(this.get_create_foreign_key_sql(fk, name)?);
    }

    Ok(sql)
}

pub fn get_create_temporary_table_snippet_sql() -> Result<String> {
    Ok("CREATE TEMPORARY TABLE".to_string())
}

pub fn get_drop_sequence_sql(
    platform: &dyn DatabasePlatform,
    sequence: &dyn IntoIdentifier,
) -> Result<String> {
    if !platform.supports_sequences() {
        Err(Error::platform_feature_unsupported(
            "Sequences are not supported by this platform",
        ))
    } else {
        Ok(format!(
            "DROP SEQUENCE {}",
            sequence.into_identifier().get_quoted_name(platform)
        ))
    }
}

pub fn get_create_index_sql(
    this: &dyn SchemaManager,
    index: &Index,
    table: &dyn IntoIdentifier,
) -> Result<String> {
    let platform = this.get_platform()?;
    let columns = index.get_columns();

    if columns.is_empty() {
        return Err(Error::index_definition_invalid("columns"));
    }

    if index.is_primary() {
        this.get_create_primary_key_sql(index, table)
    } else {
        let table = table.into_identifier().get_quoted_name(platform.as_dyn());
        let name = index.get_quoted_name(platform.as_dyn());

        Ok(format!(
            "CREATE {}INDEX {} ON {} ({}){}",
            this.get_create_index_sql_flags(index),
            name,
            table,
            this.get_index_field_declaration_list_sql(index)?,
            this.get_partial_index_sql(index)?
        ))
    }
}

pub fn get_partial_index_sql(platform: &dyn DatabasePlatform, index: &Index) -> Result<String> {
    Ok(
        if platform.supports_partial_indexes() && index.r#where.is_some() {
            format!(" WHERE {}", index.r#where.as_ref().unwrap())
        } else {
            "".to_string()
        },
    )
}

pub fn get_create_index_sql_flags(index: &Index) -> String {
    if index.is_unique() {
        "UNIQUE ".to_string()
    } else {
        "".to_string()
    }
}

pub fn get_create_primary_key_sql(
    this: &dyn SchemaManager,
    index: &Index,
    table: &dyn IntoIdentifier,
) -> Result<String> {
    let platform = this.get_platform()?;
    let table = table.into_identifier().get_quoted_name(platform.as_dyn());
    Ok(format!(
        "ALTER TABLE {} ADD PRIMARY KEY ({})",
        table,
        this.get_index_field_declaration_list_sql(index)?
    ))
}

pub fn get_create_schema_sql(
    platform: &dyn DatabasePlatform,
    schema_name: &dyn IntoIdentifier,
) -> Result<String> {
    if platform.supports_schemas() {
        Ok(format!(
            "CREATE SCHEMA {}",
            schema_name.into_identifier().get_quoted_name(platform)
        ))
    } else {
        Err(Error::platform_feature_unsupported("schemas"))
    }
}

pub fn get_create_unique_constraint_sql(
    platform: &dyn DatabasePlatform,
    constraint: &UniqueConstraint,
    table_name: &dyn IntoIdentifier,
) -> Result<String> {
    let table = table_name.into_identifier().get_quoted_name(platform);
    let query = format!(
        "ALTER TABLE {} ADD CONSTRAINT {} UNIQUE ({})",
        table,
        constraint.get_quoted_name(platform),
        constraint.get_quoted_columns(platform).join(", ")
    );

    Ok(query)
}

pub fn get_drop_schema_sql(this: &dyn SchemaManager, schema_name: &str) -> Result<String> {
    if !this.get_platform()?.supports_schemas() {
        Err(Error::platform_feature_unsupported("schemas"))
    } else {
        Ok(format!("DROP SCHEMA {}", schema_name))
    }
}

pub fn get_creed_type_comment(creed_type: &TypePtr) -> String {
    format!("(CRType:{})", creed_type.get_name())
}

pub fn get_column_comment(platform: &dyn DatabasePlatform, column: &Column) -> Result<String> {
    let mut comment = column.get_comment().as_ref().cloned().unwrap_or_default();
    let column_type = TypeManager::get_instance().get_type(column.get_type())?;
    if column_type.requires_sql_comment_hint(platform.as_dyn()) {
        comment += &platform.get_creed_type_comment(&column_type);
    }

    Ok(comment)
}

pub fn quote_identifier(this: &dyn DatabasePlatform, identifier: &str) -> String {
    identifier
        .split('.')
        .map(|w| this.quote_single_identifier(w))
        .collect::<Vec<String>>()
        .join(".")
}

pub fn quote_single_identifier(str: &str) -> String {
    let c = '"';
    format!("{}{}{}", c, str.replace(c, &c.to_string().repeat(2)), c)
}

pub fn quote_string_literal(this: &dyn DatabasePlatform, str: &str) -> String {
    let c = this.get_string_literal_quote_character();
    format!("{}{}{}", c, str.replace(c, &c.repeat(2)), c)
}

pub fn get_string_literal_quote_character() -> &'static str {
    "'"
}

pub fn get_create_foreign_key_sql(
    this: &dyn SchemaManager,
    foreign_key: &ForeignKeyConstraint,
    table: &dyn IntoIdentifier,
) -> Result<String> {
    let table = table
        .into_identifier()
        .get_quoted_name(&this.get_platform()?);
    Ok(format!(
        "ALTER TABLE {} ADD {}",
        table,
        this.get_foreign_key_declaration_sql(foreign_key)?
    ))
}

pub fn on_schema_alter_table_add_column(
    this: &dyn SchemaManager,
    column: &Column,
    diff: &TableDiff,
    mut column_sql: Vec<String>,
) -> Result<(bool, Vec<String>)> {
    let platform = this.get_platform()?;
    let event = platform
        .get_event_manager()
        .dispatch_sync(SchemaAlterTableAddColumnEvent::new(
            column,
            diff,
            platform.clone(),
        ))?;
    let mut sql = event.get_sql();
    column_sql.append(&mut sql);

    Ok((event.is_default_prevented(), column_sql))
}

pub fn on_schema_alter_table_remove_column(
    this: &dyn SchemaManager,
    column: &Column,
    diff: &TableDiff,
    mut column_sql: Vec<String>,
) -> Result<(bool, Vec<String>)> {
    let platform = this.get_platform()?;
    let event =
        platform
            .get_event_manager()
            .dispatch_sync(SchemaAlterTableRemoveColumnEvent::new(
                column,
                diff,
                platform.clone(),
            ))?;
    let mut sql = event.get_sql();
    column_sql.append(&mut sql);

    Ok((event.is_default_prevented(), column_sql))
}

pub fn on_schema_alter_table_change_column(
    this: &dyn SchemaManager,
    column_diff: &ColumnDiff,
    diff: &TableDiff,
    mut column_sql: Vec<String>,
) -> Result<(bool, Vec<String>)> {
    let platform = this.get_platform()?;
    let event =
        platform
            .get_event_manager()
            .dispatch_sync(SchemaAlterTableChangeColumnEvent::new(
                column_diff,
                diff,
                platform.clone(),
            ))?;
    let mut sql = event.get_sql();
    column_sql.append(&mut sql);

    Ok((event.is_default_prevented(), column_sql))
}

pub fn on_schema_alter_table_rename_column(
    this: &dyn SchemaManager,
    old_column_name: &str,
    column: &Column,
    diff: &TableDiff,
    mut column_sql: Vec<String>,
) -> Result<(bool, Vec<String>)> {
    let platform = this.get_platform()?;
    let event =
        platform
            .get_event_manager()
            .dispatch_sync(SchemaAlterTableRenameColumnEvent::new(
                old_column_name,
                column,
                diff,
                platform.clone(),
            ))?;
    let mut sql = event.get_sql();
    column_sql.append(&mut sql);

    Ok((event.is_default_prevented(), column_sql))
}

pub fn on_schema_alter_table(
    this: &dyn SchemaManager,
    diff: &TableDiff,
    mut sql: Vec<String>,
) -> Result<(bool, Vec<String>)> {
    let platform = this.get_platform()?;
    let event = platform
        .get_event_manager()
        .dispatch_sync(SchemaAlterTableEvent::new(diff, platform.clone()))?;
    let mut alt_sql = event.get_sql();
    sql.append(&mut alt_sql);

    Ok((event.is_default_prevented(), sql))
}

pub fn get_pre_alter_table_index_foreign_key_sql(
    this: &dyn SchemaManager,
    diff: &TableDiff,
) -> Result<Vec<String>> {
    let table_name = diff.get_name();
    let platform = this.get_platform()?;

    let mut sql = vec![];
    if platform.supports_foreign_key_constraints() {
        for foreign_key in &diff.removed_foreign_keys {
            sql.push(this.get_drop_foreign_key_sql(foreign_key, &table_name)?);
        }

        for foreign_key in &diff.changed_foreign_keys {
            sql.push(this.get_drop_foreign_key_sql(foreign_key, &table_name)?);
        }
    }

    for index in &diff.removed_indexes {
        sql.push(this.get_drop_index_sql(&Identifier::new(index.get_name(), false), &table_name)?);
    }

    for index in &diff.changed_indexes {
        sql.push(this.get_drop_index_sql(&Identifier::new(index.get_name(), false), &table_name)?);
    }

    Ok(sql)
}

pub fn get_post_alter_table_index_foreign_key_sql(
    this: &dyn SchemaManager,
    diff: &TableDiff,
) -> Result<Vec<String>> {
    let mut sql = vec![];
    let table_name = if let Some(n) = diff.get_new_name() {
        n
    } else {
        diff.get_name()
    };

    let platform = this.get_platform()?;
    if platform.supports_foreign_key_constraints() {
        for foreign_key in &diff.added_foreign_keys {
            sql.push(this.get_create_foreign_key_sql(foreign_key, &table_name)?)
        }

        for foreign_key in &diff.changed_foreign_keys {
            sql.push(this.get_create_foreign_key_sql(foreign_key, &table_name)?)
        }
    }

    for index in &diff.added_indexes {
        sql.push(this.get_create_index_sql(index, &table_name)?);
    }

    for index in &diff.changed_indexes {
        sql.push(this.get_create_index_sql(index, &table_name)?);
    }

    for (old_index_name, index) in &diff.renamed_indexes {
        let old_index_name = Identifier::new(old_index_name, false);
        for q in this.get_rename_index_sql(&old_index_name, index, &table_name)? {
            sql.push(q);
        }
    }

    Ok(sql)
}

pub fn get_rename_index_sql(
    this: &dyn SchemaManager,
    old_index_name: &Identifier,
    index: &Index,
    table_name: &Identifier,
) -> Result<Vec<String>> {
    Ok(vec![
        this.get_drop_index_sql(old_index_name, table_name)?,
        this.get_create_index_sql(index, table_name)?,
    ])
}

pub fn get_column_declaration_list_sql(
    this: &dyn SchemaManager,
    columns: &[ColumnData],
) -> Result<String> {
    let mut declarations = vec![];
    for column in columns {
        declarations.push(this.get_column_declaration_sql(&column.name, column)?)
    }

    Ok(declarations.join(", "))
}

pub fn get_column_declaration_sql(
    this: &dyn SchemaManager,
    name: &str,
    column: &ColumnData,
) -> Result<String> {
    let platform = this.get_platform()?;
    let declaration = if column.column_definition.is_some() {
        platform.get_custom_type_declaration_sql(column)?
    } else {
        let default = platform.get_default_value_declaration_sql(column)?;
        let charset = column
            .charset
            .as_ref()
            .map(|v| format!(" {}", this.get_column_charset_declaration_sql(v)))
            .unwrap_or_default();
        let collation = if let Some(collation) = column.collation.as_ref() {
            if !collation.is_empty() {
                format!(" {}", this.get_column_collation_declaration_sql(collation)?)
            } else {
                Default::default()
            }
        } else {
            Default::default()
        };

        let not_null = if column.notnull { " NOT NULL" } else { "" };
        let unique = if column.unique { " UNIQUE" } else { "" };
        let check = if column.check.is_some() {
            format!(" {}", this.get_check_field_declaration_sql(column)?)
        } else {
            "".to_string()
        };

        let type_decl = TypeManager::get_instance()
            .get_type(column.r#type.clone())?
            .get_sql_declaration(column, platform.as_dyn())?;
        let mut comment_decl = "".to_string();
        if platform.supports_inline_column_comments() {
            if let Some(comment) = column.comment.as_ref() {
                if !comment.is_empty() {
                    comment_decl = format!(" {}", this.get_inline_column_comment_sql(comment)?);
                }
            }
        }

        format!(
            "{}{}{}{}{}{}{}{}",
            type_decl, charset, default, not_null, unique, check, collation, comment_decl
        )
    };

    Ok(format!("{} {}", name, declaration).trim().to_string())
}

pub fn get_decimal_type_declaration_sql(column: &ColumnData) -> Result<String> {
    let precision = column.precision.unwrap_or(10);
    let scale = column.scale.unwrap_or(0);

    Ok(format!("NUMERIC({}, {})", precision, scale))
}

pub fn get_default_value_declaration_sql(
    this: &dyn DatabasePlatform,
    column: &ColumnData,
) -> Result<String> {
    let default = column.default.clone();
    if matches!(default, Value::NULL) {
        return Ok((if column.notnull { "" } else { " DEFAULT NULL" }).to_string());
    }

    let t = column.r#type.clone();
    if t == INTEGER.into_type()? || t == BIGINT.into_type()? {
        return Ok(format!(" DEFAULT {}", default));
    }

    if (t == DATETIME.into_type()? || t == DATETIMETZ.into_type()?)
        && default == Value::String(this.get_current_timestamp_sql().to_string())
    {
        return Ok(format!(" DEFAULT {}", this.get_current_timestamp_sql()));
    }

    if t == TIME.into_type()? && default == Value::String(this.get_current_time_sql().to_string()) {
        return Ok(format!(" DEFAULT {}", this.get_current_time_sql()));
    }

    if t == DATE.into_type()? && default == Value::String(this.get_current_date_sql().to_string()) {
        return Ok(format!(" DEFAULT {}", this.get_current_date_sql()));
    }

    if t == BOOLEAN.into_type()? {
        return Ok(format!(" DEFAULT {}", this.convert_boolean(default)?));
    }

    Ok(format!(
        " DEFAULT {}",
        this.quote_string_literal(&default.to_string())
    ))
}

pub fn get_check_declaration_sql(
    this: &dyn SchemaManager,
    definition: &[ColumnData],
) -> Result<String> {
    let mut constraints = vec![];
    for def in definition {
        let sql = this.get_check_field_declaration_sql(def)?;

        if !sql.is_empty() {
            constraints.push(sql)
        }
    }

    Ok(constraints.join(", "))
}

pub fn get_check_field_declaration_sql(
    this: &dyn SchemaManager,
    definition: &ColumnData,
) -> Result<String> {
    let column_name = &definition.name;
    let sql = match &definition.check {
        None => "".to_string(),
        Some(CheckConstraint::Literal(literal)) => format!("CHECK ({})", literal),
        Some(CheckConstraint::EqString(str)) => {
            format!("CHECK({} = {}", column_name, this.quote_string_literal(str))
        }
        Some(CheckConstraint::NotEqString(str)) => format!(
            "CHECK({} != {}",
            column_name,
            this.quote_string_literal(str)
        ),
        Some(CheckConstraint::MinInt(value)) => {
            format!("CHECK ({} >= {})", column_name, value)
        }
        Some(CheckConstraint::MaxInt(value)) => {
            format!("CHECK ({} <= {})", column_name, value)
        }
        Some(CheckConstraint::MinFloat(value)) => {
            format!("CHECK ({} >= {})", column_name, value)
        }
        Some(CheckConstraint::MaxFloat(value)) => {
            format!("CHECK ({} <= {})", column_name, value)
        }
    };

    Ok(sql)
}

pub fn get_unique_constraint_declaration_sql(
    this: &dyn SchemaManager,
    name: &str,
    constraint: &UniqueConstraint,
) -> Result<String> {
    let platform = this.get_platform()?;
    let columns = constraint.get_quoted_columns(platform.as_dyn());
    let name = Identifier::new(name, false);

    if columns.is_empty() {
        Err(Error::index_definition_invalid("columns"))
    } else {
        let mut constraint_flags = constraint.get_flags().clone();
        constraint_flags.push("UNIQUE".to_string());

        let constraint_name = name.get_quoted_name(platform.as_dyn());
        let column_list_names = this.get_columns_field_declaration_list_sql(columns.as_slice())?;

        Ok(format!(
            "CONSTRAINT {} {} ({})",
            constraint_name,
            constraint_flags.join(" "),
            column_list_names
        ))
    }
}

pub fn get_index_declaration_sql(
    this: &dyn SchemaManager,
    name: &str,
    index: &Index,
) -> Result<String> {
    let platform = this.get_platform()?;
    let columns = index.get_columns();
    let name = Identifier::new(name, false);

    if columns.is_empty() {
        Err(Error::index_definition_invalid("columns"))
    } else {
        Ok(format!(
            "{}INDEX {} ({}){}",
            this.get_create_index_sql_flags(index),
            name.get_quoted_name(platform.as_dyn()),
            this.get_index_field_declaration_list_sql(index)?,
            this.get_partial_index_sql(index)?
        ))
    }
}

pub fn get_custom_type_declaration_sql(column: &ColumnData) -> Result<String> {
    Ok(column
        .column_definition
        .as_ref()
        .cloned()
        .unwrap_or_default())
}

pub fn get_index_field_declaration_list_sql(
    platform: &dyn DatabasePlatform,
    index: &Index,
) -> Result<String> {
    Ok(index.get_quoted_columns(platform).join(", "))
}

pub fn get_columns_field_declaration_list_sql(columns: &[String]) -> Result<String> {
    Ok(columns.join(", "))
}

pub fn get_temporary_table_name(table_name: &str) -> Result<String> {
    Ok(table_name.to_string())
}

pub fn get_foreign_key_declaration_sql(
    this: &dyn SchemaManager,
    foreign_key: &ForeignKeyConstraint,
) -> Result<String> {
    Ok(format!(
        "{}{}",
        this.get_foreign_key_base_declaration_sql(foreign_key)?,
        this.get_advanced_foreign_key_options_sql(foreign_key)?,
    ))
}

pub fn get_advanced_foreign_key_options_sql(
    this: &dyn SchemaManager,
    foreign_key: &ForeignKeyConstraint,
) -> Result<String> {
    let mut query = "".to_string();

    if let Some(opt) = &foreign_key.on_update {
        query += &*format!(
            " ON UPDATE {}",
            this.get_foreign_key_referential_action_sql(opt)?
        )
    }

    if let Some(opt) = &foreign_key.on_delete {
        query += &*format!(
            " ON UPDATE {}",
            this.get_foreign_key_referential_action_sql(opt)?
        )
    }

    Ok(query)
}

pub fn get_foreign_key_referential_action_sql(
    action: &ForeignKeyReferentialAction,
) -> Result<String> {
    let act = match action {
        ForeignKeyReferentialAction::Cascade => "CASCADE",
        ForeignKeyReferentialAction::SetNull => "SET NULL",
        ForeignKeyReferentialAction::NoAction => "NO ACTION",
        ForeignKeyReferentialAction::Restrict => "RESTRICT",
        ForeignKeyReferentialAction::SetDefault => "SET DEFAULT",
    };

    Ok(act.to_string())
}

pub fn get_foreign_key_base_declaration_sql(
    platform: &dyn DatabasePlatform,
    foreign_key: &ForeignKeyConstraint,
) -> Result<String> {
    let sql = if foreign_key.get_name().is_empty() {
        "".to_string()
    } else {
        format!("CONSTRAINT {} ", foreign_key.get_quoted_name(platform))
    };

    if foreign_key.get_local_columns().is_empty() {
        return Err(Error::foreign_key_definition_invalid("local"));
    }

    if foreign_key.get_foreign_columns().is_empty() {
        return Err(Error::foreign_key_definition_invalid("foreign"));
    }

    if foreign_key.get_foreign_table().is_empty() {
        return Err(Error::foreign_key_definition_invalid("foreign_table"));
    }

    Ok(format!(
        "{}FOREIGN KEY ({}) REFERENCES {} ({})",
        sql,
        foreign_key.get_quoted_local_columns(platform).join(", "),
        foreign_key.get_quoted_foreign_table_name(platform),
        foreign_key.get_quoted_foreign_columns(platform).join(", "),
    ))
}

pub fn get_column_charset_declaration_sql() -> String {
    "".to_string()
}

pub fn get_column_collation_declaration_sql(
    platform: &dyn DatabasePlatform,
    collation: &str,
) -> Result<String> {
    Ok(if platform.supports_column_collation() {
        format!("COLLATE {}", collation)
    } else {
        "".to_string()
    })
}

pub fn convert_boolean(item: Value) -> Value {
    match item {
        Value::Boolean(b) => {
            if b {
                Value::Int(1)
            } else {
                Value::Int(0)
            }
        }
        _ => item,
    }
}

pub fn convert_from_boolean(item: &Value) -> Value {
    Value::Boolean(bool::from(item))
}

pub fn convert_booleans_to_database_value(
    this: &dyn DatabasePlatform,
    item: Value,
) -> Result<Value> {
    this.convert_boolean(item)
}

pub fn get_current_date_sql() -> &'static str {
    "CURRENT_DATE"
}

pub fn get_current_time_sql() -> &'static str {
    "CURRENT_TIME"
}

pub fn get_current_timestamp_sql() -> &'static str {
    "CURRENT_TIMESTAMP"
}

pub fn get_transaction_isolation_level_sql(level: TransactionIsolationLevel) -> String {
    match level {
        TransactionIsolationLevel::ReadUncommitted => "READ UNCOMMITTED",
        TransactionIsolationLevel::ReadCommitted => "READ COMMITTED",
        TransactionIsolationLevel::RepeatableRead => "REPEATABLE READ",
        TransactionIsolationLevel::Serializable => "SERIALIZABLE",
    }
    .to_string()
}

pub fn get_create_view_sql(platform: &dyn DatabasePlatform, view: &View) -> Result<String> {
    Ok(format!(
        "CREATE VIEW {} AS {}",
        view.get_quoted_name(platform),
        view.get_sql()
    ))
}

pub fn get_drop_view_sql(platform: &dyn DatabasePlatform, name: &Identifier) -> Result<String> {
    Ok(format!("DROP VIEW {}", name.get_quoted_name(platform)))
}

pub fn get_create_database_sql(
    platform: &dyn DatabasePlatform,
    name: &Identifier,
) -> Result<String> {
    if !platform.supports_create_drop_database() {
        Err(Error::platform_feature_unsupported("create drop database"))
    } else {
        Ok(format!(
            "CREATE DATABASE {}",
            name.get_quoted_name(platform)
        ))
    }
}

pub fn get_drop_database_sql(this: &dyn SchemaManager, name: &str) -> Result<String> {
    if !this.get_platform()?.supports_create_drop_database() {
        Err(Error::platform_feature_unsupported("create drop database"))
    } else {
        Ok(format!("DROP DATABASE {}", name))
    }
}

pub fn get_date_time_tz_type_declaration_sql(
    this: &dyn DatabasePlatform,
    column: &ColumnData,
) -> Result<String> {
    this.get_date_time_type_declaration_sql(column)
}

pub fn get_float_declaration_sql() -> Result<String> {
    Ok("DOUBLE PRECISION".to_string())
}

pub fn get_default_transaction_isolation_level() -> TransactionIsolationLevel {
    TransactionIsolationLevel::ReadCommitted
}

pub fn get_date_time_format_string() -> &'static str {
    "%Y-%m-%d %H:%M:%S"
}

pub fn get_date_time_tz_format_string() -> &'static str {
    "%Y-%m-%d %H:%M:%S"
}

pub fn get_date_format_string() -> &'static str {
    "%Y-%m-%d"
}

pub fn get_time_format_string() -> &'static str {
    "%H:%M:%S"
}

pub fn modify_limit_query(query: &str, limit: Option<usize>, offset: Option<usize>) -> String {
    let offset = offset.unwrap_or(0);
    let mut query = query.to_string();
    if let Some(limit) = limit {
        query += &format!(" LIMIT {}", limit);
    }

    if offset > 0 {
        query += &format!(" OFFSET {}", offset);
    }

    query
}

pub fn get_max_identifier_length() -> usize {
    63
}

pub fn get_empty_identity_insert_sql(
    quoted_table_name: &str,
    quoted_identifier_column_name: &str,
) -> String {
    format!(
        "INSERT INTO {} ({}) VALUES (null)",
        quoted_table_name, quoted_identifier_column_name
    )
}

pub fn get_truncate_table_sql(
    this: &dyn SchemaManager,
    table_name: &dyn IntoIdentifier,
) -> Result<String> {
    let platform = this.get_platform()?;
    Ok(format!(
        "TRUNCATE {}",
        table_name
            .into_identifier()
            .get_quoted_name(platform.as_dyn())
    ))
}

pub fn create_save_point(savepoint: &str) -> String {
    format!("SAVEPOINT {}", savepoint)
}

pub fn release_save_point(savepoint: &str) -> String {
    format!("RELEASE SAVEPOINT {}", savepoint)
}

pub fn rollback_save_point(savepoint: &str) -> String {
    format!("ROLLBACK TO SAVEPOINT {}", savepoint)
}

pub fn escape_string_for_like(
    this: &dyn DatabasePlatform,
    input_string: &str,
    escape_char: &str,
) -> Result<String> {
    let rx = Regex::new(&format!(
        "([{}{}])",
        regex::escape(this.get_like_wildcard_characters()),
        regex::escape(escape_char)
    ))?;

    Ok(rx.replace_all(input_string, "\\$1").to_string())
}

pub fn get_like_wildcard_characters() -> &'static str {
    "%_"
}

pub fn columns_equal(this: &dyn SchemaManager, column1: &Column, column2: &Column) -> Result<bool> {
    let platform = this.get_platform()?;
    let c1 =
        this.get_column_declaration_sql("", &column1.generate_column_data(platform.as_dyn()))?;
    let c2 =
        this.get_column_declaration_sql("", &column2.generate_column_data(platform.as_dyn()))?;

    Ok(if c1 != c2 {
        false
    } else if platform.supports_inline_column_comments() {
        true
    } else if column1.get_comment().clone().unwrap_or_default()
        != column2.get_comment().clone().unwrap_or_default()
    {
        false
    } else {
        column1.get_type() == column2.get_type()
    })
}

pub fn list_databases(this: &dyn SchemaManager) -> AsyncResult<Vec<Identifier>> {
    Box::pin(async {
        let sql = this.get_list_databases_sql()?;
        let databases = this.get_connection().fetch_all(sql, params!()).await?;

        this.get_portable_databases_list(databases)
    })
}

pub fn list_sequences(this: &dyn SchemaManager) -> AsyncResult<Vec<Sequence>> {
    Box::pin(async move {
        let conn = this.get_connection();
        let database = get_database(conn, function_name!()).await?;
        let sql = this.get_list_sequences_sql(&database)?;
        let sequences = this.get_connection().fetch_all(sql, params!()).await?;

        this.get_portable_sequences_list(sequences)
            .map(|r| filter_asset_names(conn, r))
    })
}

/// Lists the columns for a given table.
pub async fn list_table_columns(
    this: &dyn SchemaManager,
    table: String,
    database: Option<String>,
) -> Result<Vec<Column>> {
    let database = if let Some(database) = database {
        database
    } else {
        get_database(this.get_connection(), function_name!()).await?
    };

    let sql = this.get_list_table_columns_sql(&table, &database)?;
    let table_columns = this.get_connection().fetch_all(sql, params!()).await?;

    this.get_portable_table_column_list(&table, &database, table_columns)
        .await
}

/// Lists the indexes for a given table returning an array of Index instances.
/// Keys of the portable indexes list are all lower-cased.
pub async fn list_table_indexes(this: &dyn SchemaManager, table: String) -> Result<Vec<Index>> {
    let database = get_database(this.get_connection(), function_name!()).await?;
    let sql = this.get_list_table_indexes_sql(&table, &database)?;

    let table_indexes = this.get_connection().fetch_all(sql, params!()).await?;

    this.get_portable_table_indexes_list(table_indexes, &table)
        .await
}

/// Whether all the given tables exist.
pub async fn tables_exist(this: &dyn SchemaManager, names: Vec<String>) -> Result<bool> {
    let table_names = this
        .list_table_names()
        .await?
        .iter()
        .map(|s| s.to_lowercase())
        .collect::<Vec<_>>();

    Ok(names.iter().all(|n| {
        let name = n.to_lowercase();
        table_names.contains(&name)
    }))
}

/// Returns a list of all tables in the current database.
pub async fn list_table_names(this: &dyn SchemaManager) -> Result<Vec<String>> {
    let sql = this.get_list_tables_sql()?;
    let tables = this.get_connection().fetch_all(sql, params!()).await?;

    Ok(filter_asset_names(
        this.get_connection(),
        this.get_portable_tables_list(tables).await?,
    )
    .iter()
    .map(Asset::get_name)
    .map(Cow::into_owned)
    .collect())
}

/// Lists the tables for this connection.
pub async fn list_tables(this: &dyn SchemaManager) -> Result<Vec<Table>> {
    let mut tables = vec![];
    for table_name in this.list_table_names().await? {
        tables.push(this.list_table_details(&table_name).await?)
    }

    Ok(tables)
}

pub async fn list_table_details(this: &dyn SchemaManager, name: String) -> Result<Table> {
    let columns = this.list_table_columns(&name, None).await?;

    let foreign_keys = if this
        .get_platform()?
        .as_dyn()
        .supports_foreign_key_constraints()
    {
        this.list_table_foreign_keys(&name).await?
    } else {
        vec![]
    };

    let indexes = this.list_table_indexes(&name).await?;

    let mut table = Table::new(Identifier::new(name, false));
    table.add_columns(columns.into_iter());
    table.add_indices(indexes.into_iter());
    table.add_foreign_keys(foreign_keys.into_iter());

    Ok(table)
}

/// Fetches definitions of table columns in the specified database and returns them grouped by table name.
/// # Protected
pub async fn fetch_table_columns_by_table(
    this: &dyn SchemaManager,
    database_name: String,
) -> Result<HashMap<String, Vec<Row>>> {
    fetch_all_associative_grouped(this, this.select_table_columns(&database_name, None).await?)
        .await
}

/// Fetches definitions of index columns in the specified database and returns them grouped by table name.
/// # Protected
pub async fn fetch_index_columns_by_table(
    this: &dyn SchemaManager,
    database_name: String,
) -> Result<HashMap<String, Vec<Row>>> {
    fetch_all_associative_grouped(this, this.select_index_columns(&database_name, None).await?)
        .await
}

async fn fetch_all_associative_grouped<SM: SchemaManager + ?Sized>(
    schema_manager: &SM,
    result: StatementResult,
) -> Result<HashMap<String, Vec<Row>>> {
    let mut data: HashMap<String, Vec<Row>> = HashMap::new();
    for row in result.fetch_all().await? {
        let table_name = schema_manager
            .get_portable_table_definition(&row)
            .await?
            .get_name()
            .into_owned();

        let e = data.entry(table_name);
        match e {
            Occupied(mut e) => {
                e.get_mut().push(row);
            }
            Vacant(e) => {
                e.insert(vec![row]);
            }
        }
    }

    Ok(data)
}

/// Fetches definitions of foreign key columns in the specified database and returns them grouped by table name.
/// # Protected
pub async fn fetch_foreign_key_columns_by_table(
    this: &dyn SchemaManager,
    database_name: String,
) -> Result<HashMap<String, Vec<Row>>> {
    if !this
        .get_platform()?
        .as_dyn()
        .supports_foreign_key_constraints()
    {
        Ok(HashMap::new())
    } else {
        fetch_all_associative_grouped(
            this,
            this.select_foreign_key_columns(&database_name, None)
                .await?,
        )
        .await
    }
}

/// Introspects the table with the given name.
pub async fn introspect_table(this: &dyn SchemaManager, name: String) -> Result<Table> {
    let table = this.list_table_details(name.as_str()).await?;

    if table.get_columns().is_empty() {
        Err(Error::table_does_not_exist(&name))
    } else {
        Ok(table)
    }
}

/// Lists the views this connection has.
pub async fn list_views(this: &dyn SchemaManager) -> Result<Vec<View>> {
    let database = get_database(this.get_connection(), function_name!()).await?;
    let sql = this.get_list_views_sql(&database)?;
    let views = this.get_connection().fetch_all(sql, params!()).await?;

    this.get_portable_views_list(views)
}

/// Lists the foreign keys for the given table.
pub async fn list_table_foreign_keys(
    this: &dyn SchemaManager,
    table: String,
) -> Result<Vec<ForeignKeyConstraint>> {
    let database = get_database(this.get_connection(), function_name!()).await?;
    let sql = this.get_list_table_foreign_keys_sql(&table, &database)?;
    let table_foreign_keys = this.get_connection().fetch_all(sql, params!()).await?;

    this.get_portable_table_foreign_keys_list(table_foreign_keys)
}

pub fn get_portable_table_indexes_list(
    this: &dyn SchemaManager,
    table_indexes: Vec<Row>,
    table_name: String,
) -> Result<Vec<Index>> {
    let mut result = HashMap::new();
    let connection = this.get_connection();
    for table_index in table_indexes {
        let index_name = string_from_value(connection, table_index.get("key_name"))?;
        let key_name = if bool::from(table_index.get("primary")?) {
            "primary".to_string()
        } else {
            index_name.to_lowercase()
        };

        let length = table_index
            .get("length")
            .cloned()
            .and_then(usize::try_from)
            .ok();

        match result.entry(key_name) {
            Vacant(e) => {
                e.insert(IndexOptions {
                    name: index_name,
                    columns: vec![string_from_value(
                        connection,
                        table_index.get("column_name"),
                    )?],
                    unique: !bool::from(table_index.get("non_unique")?),
                    primary: bool::from(table_index.get("primary")?),
                    flags: string_from_value(connection, table_index.get("flags"))?
                        .split(',')
                        .map(|s| s.to_string())
                        .collect(),
                    options_lengths: vec![length],
                    options_where: table_index
                        .get("where")
                        .and_then(|v| match v {
                            Value::String(s) => Ok(s.clone()),
                            _ => Err("invalid".into()),
                        })
                        .ok(),
                });
            }
            Occupied(mut e) => {
                let opts = e.get_mut();
                opts.columns.push(string_from_value(
                    connection,
                    table_index.get("column_name"),
                )?);
                opts.options_lengths.push(length);
            }
        }
    }

    let event_manager = this.get_platform()?.as_dyn().get_event_manager();

    let mut indexes = HashMap::new();
    for (index_key, data) in result {
        let event = event_manager.dispatch_sync(SchemaIndexDefinitionEvent::new(
            &data,
            &table_name,
            this.get_platform()?,
        ))?;
        let index = if event.is_default_prevented() {
            event.index()
        } else {
            Some(data.new_index())
        };

        if let Some(index) = index {
            indexes.insert(index_key, index);
        }
    }

    Ok(indexes.into_values().collect())
}

pub fn get_portable_table_foreign_keys_list(
    this: &dyn SchemaManager,
    table_foreign_keys: Vec<Row>,
) -> Result<Vec<ForeignKeyConstraint>> {
    let mut list = vec![];
    for value in table_foreign_keys {
        list.push(this.get_portable_table_foreign_key_definition(&value)?);
    }

    Ok(list)
}

/// Creates a new foreign key.
pub async fn create_foreign_key(
    this: &dyn SchemaManager,
    foreign_key: ForeignKeyConstraint,
    table_name: &str,
) -> Result<()> {
    let table = this.list_table_details(table_name).await?;

    let mut table_diff = TableDiff::new(table_name, &table);
    table_diff.added_foreign_keys.push(foreign_key);

    this.alter_table(table_diff).await
}

pub fn get_portable_table_column_list(
    this: &dyn SchemaManager,
    table: &str,
    database: &str,
    table_columns: Vec<Row>,
) -> Result<Vec<Column>> {
    let table = table.to_string();
    let database = database.to_string();

    let platform = this.get_platform()?;
    let event_manager = platform.get_event_manager();
    let mut list = vec![];

    for table_column in table_columns {
        let event = event_manager.dispatch_sync(SchemaColumnDefinitionEvent::new(
            &table_column,
            &table,
            &database,
            platform.clone(),
        ))?;

        let column = if event.is_default_prevented() {
            event.column()
        } else {
            Some(this.get_portable_table_column_definition(&table_column)?)
        };

        if column.is_none() {
            continue;
        }

        let column = column.unwrap();
        list.push(column);
    }

    Ok(list)
}

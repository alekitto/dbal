use super::{CreateFlags, DatabasePlatform, DateIntervalUnit, LockMode, TrimMode};
use crate::event::{
    SchemaAlterTableAddColumnEvent, SchemaAlterTableRemoveColumnEvent, SchemaCreateTableEvent,
    SchemaDropTableEvent,
};
use crate::r#type::{Type, TypeManager};
use crate::schema::{
    Asset, CheckConstraint, Column, ColumnData, ColumnDiff, ForeignKeyConstraint,
    ForeignKeyReferentialAction, Identifier, Index, Sequence, Table, TableDiff, TableOptions,
    UniqueConstraint,
};
use crate::{
    Error, Result, SchemaAlterTableChangeColumnEvent, SchemaAlterTableEvent,
    SchemaAlterTableRenameColumnEvent, SchemaCreateTableColumnEvent, TransactionIsolationLevel,
    Value,
};
use itertools::Itertools;
use regex::Regex;
use serde_json::Number;
use std::any::TypeId;

pub fn get_ascii_string_type_declaration_sql<T: DatabasePlatform + ?Sized>(
    this: &T,
    column: &ColumnData,
) -> Result<String> {
    this.get_string_type_declaration_sql(column)
}

pub fn get_string_type_declaration_sql<T: DatabasePlatform + ?Sized>(
    this: &T,
    column: &ColumnData,
) -> Result<String> {
    let fixed = column.fixed.unwrap_or(false);
    this.get_varchar_type_declaration_sql_snippet(column.length, fixed)
}

pub fn get_binary_type_declaration_sql<T: DatabasePlatform + ?Sized>(
    this: &T,
    column: &ColumnData,
) -> Result<String> {
    let fixed = column.fixed.unwrap_or(false);
    this.get_binary_type_declaration_sql_snippet(column.length, fixed)
}

pub fn get_guid_type_declaration_sql<T: DatabasePlatform + ?Sized>(
    this: &T,
    column: &ColumnData,
) -> Result<String> {
    let mut column = column.clone();
    let _ = column.length.insert(36);
    let _ = column.fixed.insert(true);

    this.get_string_type_declaration_sql(&column)
}

pub fn get_json_type_declaration_sql<T: DatabasePlatform + ?Sized>(
    this: &T,
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

pub fn get_date_add_seconds_expression<T: DatabasePlatform + ?Sized>(
    this: &T,
    date: &str,
    seconds: i64,
) -> Result<String> {
    this.get_date_arithmetic_interval_expression(date, "+", seconds, DateIntervalUnit::Second)
}

pub fn get_date_sub_seconds_expression<T: DatabasePlatform + ?Sized>(
    this: &T,
    date: &str,
    seconds: i64,
) -> Result<String> {
    this.get_date_arithmetic_interval_expression(date, "-", seconds, DateIntervalUnit::Second)
}

pub fn get_date_add_minutes_expression<T: DatabasePlatform + ?Sized>(
    this: &T,
    date: &str,
    minutes: i64,
) -> Result<String> {
    this.get_date_arithmetic_interval_expression(date, "+", minutes, DateIntervalUnit::Minute)
}

pub fn get_date_sub_minutes_expression<T: DatabasePlatform + ?Sized>(
    this: &T,
    date: &str,
    minutes: i64,
) -> Result<String> {
    this.get_date_arithmetic_interval_expression(date, "-", minutes, DateIntervalUnit::Minute)
}

pub fn get_date_add_hour_expression<T: DatabasePlatform + ?Sized>(
    this: &T,
    date: &str,
    hours: i64,
) -> Result<String> {
    this.get_date_arithmetic_interval_expression(date, "+", hours, DateIntervalUnit::Hour)
}

pub fn get_date_sub_hour_expression<T: DatabasePlatform + ?Sized>(
    this: &T,
    date: &str,
    hours: i64,
) -> Result<String> {
    this.get_date_arithmetic_interval_expression(date, "-", hours, DateIntervalUnit::Hour)
}

pub fn get_date_add_days_expression<T: DatabasePlatform + ?Sized>(
    this: &T,
    date: &str,
    days: i64,
) -> Result<String> {
    this.get_date_arithmetic_interval_expression(date, "+", days, DateIntervalUnit::Day)
}

pub fn get_date_sub_days_expression<T: DatabasePlatform + ?Sized>(
    this: &T,
    date: &str,
    days: i64,
) -> Result<String> {
    this.get_date_arithmetic_interval_expression(date, "-", days, DateIntervalUnit::Day)
}

pub fn get_date_add_weeks_expression<T: DatabasePlatform + ?Sized>(
    this: &T,
    date: &str,
    weeks: i64,
) -> Result<String> {
    this.get_date_arithmetic_interval_expression(date, "+", weeks, DateIntervalUnit::Week)
}

pub fn get_date_sub_weeks_expression<T: DatabasePlatform + ?Sized>(
    this: &T,
    date: &str,
    weeks: i64,
) -> Result<String> {
    this.get_date_arithmetic_interval_expression(date, "-", weeks, DateIntervalUnit::Week)
}

pub fn get_date_add_month_expression<T: DatabasePlatform + ?Sized>(
    this: &T,
    date: &str,
    months: i64,
) -> Result<String> {
    this.get_date_arithmetic_interval_expression(date, "+", months, DateIntervalUnit::Month)
}

pub fn get_date_sub_month_expression<T: DatabasePlatform + ?Sized>(
    this: &T,
    date: &str,
    months: i64,
) -> Result<String> {
    this.get_date_arithmetic_interval_expression(date, "-", months, DateIntervalUnit::Month)
}

pub fn get_date_add_quarters_expression<T: DatabasePlatform + ?Sized>(
    this: &T,
    date: &str,
    quarters: i64,
) -> Result<String> {
    this.get_date_arithmetic_interval_expression(date, "+", quarters, DateIntervalUnit::Quarter)
}

pub fn get_date_sub_quarters_expression<T: DatabasePlatform + ?Sized>(
    this: &T,
    date: &str,
    quarters: i64,
) -> Result<String> {
    this.get_date_arithmetic_interval_expression(date, "-", quarters, DateIntervalUnit::Quarter)
}

pub fn get_date_add_years_expression<T: DatabasePlatform + ?Sized>(
    this: &T,
    date: &str,
    years: i64,
) -> Result<String> {
    this.get_date_arithmetic_interval_expression(date, "+", years, DateIntervalUnit::Year)
}

pub fn get_date_sub_years_expression<T: DatabasePlatform + ?Sized>(
    this: &T,
    date: &str,
    years: i64,
) -> Result<String> {
    this.get_date_arithmetic_interval_expression(date, "-", years, DateIntervalUnit::Year)
}

pub fn get_bit_and_comparison_expression(value1: &str, value2: &str) -> Result<String> {
    Ok(format!("({} & {})", value1, value2))
}

pub fn get_bit_or_comparison_expression(value1: &str, value2: &str) -> Result<String> {
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

pub fn get_read_lock_sql<T: DatabasePlatform + ?Sized>(this: &T) -> Result<String> {
    this.get_for_update_sql()
}

pub fn get_write_lock_sql<T: DatabasePlatform + ?Sized>(this: &T) -> Result<String> {
    this.get_for_update_sql()
}

pub fn get_drop_table_sql<T: DatabasePlatform + Sync>(
    this: &T,
    table_name: &Identifier,
) -> Result<String> {
    let table_arg = table_name.get_quoted_name(this);

    let mut ev = SchemaDropTableEvent::new(table_arg.clone(), this);
    this.get_event_manager().dispatch_sync(&mut ev);

    if ev.is_default_prevented() {
        if let Some(ref sql) = ev.sql {
            return Ok(sql.clone());
        }
    }

    Ok(format!("DROP TABLE {}", table_arg))
}

pub fn get_drop_temporary_table_sql<T: DatabasePlatform + Sync>(
    this: &T,
    table: &Identifier,
) -> Result<String> {
    this.get_drop_table_sql(table)
}

pub fn get_drop_index_sql<T: DatabasePlatform + ?Sized>(
    this: &T,
    index: &Identifier,
) -> Result<String> {
    let index_name = index.get_quoted_name(this);
    Ok(format!("DROP INDEX {}", index_name))
}

pub fn get_drop_constraint_sql<T: DatabasePlatform + ?Sized>(
    this: &T,
    constraint: Identifier,
    table_name: &Identifier,
) -> Result<String> {
    let constraint = constraint.get_quoted_name(this);
    let table_name = table_name.get_quoted_name(this);
    Ok(format!(
        "ALTER TABLE {} DROP CONSTRAINT {}",
        table_name, constraint
    ))
}

pub fn get_drop_foreign_key_sql<T: DatabasePlatform + ?Sized>(
    this: &T,
    foreign_key: &ForeignKeyConstraint,
    table_name: &Identifier,
) -> Result<String> {
    let foreign_key = foreign_key.get_quoted_name(this);
    let table_name = table_name.get_quoted_name(this);
    Ok(format!(
        "ALTER TABLE {} DROP FOREIGN KEY {}",
        table_name, foreign_key
    ))
}

pub fn get_drop_unique_constraint_sql<T: DatabasePlatform + ?Sized>(
    this: &T,
    name: Identifier,
    table_name: &Identifier,
) -> Result<String> {
    this.get_drop_constraint_sql(name, table_name)
}

pub fn get_create_table_sql<T: DatabasePlatform + Sync>(
    this: &T,
    table: Table,
    create_flags: Option<CreateFlags>,
) -> Result<Vec<String>> {
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
                    .insert(index.get_quoted_name(this), index.clone());
                continue;
            }

            options.primary = Some((index.get_quoted_columns(this), index.clone()));
        }

        for unique_constraint in table.get_unique_constraints() {
            options.unique_constraints.insert(
                unique_constraint.get_quoted_name(this),
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
        let mut e = SchemaCreateTableColumnEvent::new(&table, column, this);
        this.get_event_manager().dispatch_sync(&mut e);

        let mut sql = e.get_sql();
        column_sql.append(&mut sql);

        if e.is_default_prevented() {
            continue;
        }

        let mut column_data = column.generate_column_data(this);
        if let Some(p) = &options.primary {
            if p.0.iter().any(|n| n.eq(&column_data.name)) {
                column_data.primary = true;
            }
        }

        columns.push(column_data);
    }

    let mut e = SchemaCreateTableEvent::new(&table, this);
    this.get_event_manager().dispatch_sync(&mut e);

    if e.is_default_prevented() {
        let mut sql = e.get_sql().clone();
        sql.append(&mut column_sql);

        return Ok(sql);
    }

    let mut sql =
        this._get_create_table_sql(table.get_table_name(), columns.as_slice(), &options)?;

    if this.supports_comment_on_statement() {
        if let Some(comment) = table.get_comment() {
            sql.push(this.get_comment_on_table_sql(table.get_table_name(), comment)?);
        }
    }

    sql.append(&mut column_sql);

    if this.supports_comment_on_statement() {
        for column in table.get_columns() {
            let comment = this.get_column_comment(column)?;
            if comment.is_empty() {
                continue;
            }

            sql.push(this.get_comment_on_column_sql(table.get_table_name(), &column, &comment));
        }
    }

    Ok(sql)
}

pub fn get_create_tables_sql<T: DatabasePlatform + Sync>(
    this: &T,
    tables: &[Table],
) -> Result<Vec<String>> {
    let mut sql = vec![];
    for table in tables {
        let mut other_sql =
            this.get_create_table_sql(table.clone(), Some(CreateFlags::CREATE_INDEXES))?;
        sql.append(&mut other_sql);
    }

    for table in tables {
        for fk in table.get_foreign_keys() {
            sql.push(this.get_create_foreign_key_sql(fk, table.get_table_name())?)
        }
    }

    Ok(sql)
}

pub fn get_drop_tables_sql<T: DatabasePlatform + Sync>(
    this: &T,
    tables: &[Table],
) -> Result<Vec<String>> {
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

pub fn get_comment_on_table_sql<T: DatabasePlatform + ?Sized>(
    this: &T,
    table_name: &Identifier,
    comment: &str,
) -> Result<String> {
    Ok(format!(
        "COMMENT ON TABLE {} IS {}",
        table_name.get_quoted_name(this),
        this.quote_string_literal(comment)
    ))
}

pub fn get_comment_on_column_sql<T: DatabasePlatform + ?Sized>(
    this: &T,
    table_name: &Identifier,
    column: &Column,
    comment: &str,
) -> String {
    format!(
        "COMMENT ON COLUMN {}.{} IS {}",
        table_name.get_quoted_name(this),
        column.get_quoted_name(this),
        this.quote_string_literal(comment)
    )
}

pub fn get_inline_column_comment_sql<T: DatabasePlatform + ?Sized>(
    this: &T,
    comment: &str,
) -> Result<String> {
    if !this.supports_inline_column_comments() {
        Err(Error::platform_feature_unsupported(
            "inline column comment unsupported for this platform",
        ))
    } else {
        Ok(format!("COMMENT {}", this.quote_string_literal(comment)))
    }
}

pub fn _get_create_table_sql<T: DatabasePlatform>(
    this: &T,
    name: &Identifier,
    columns: &[ColumnData],
    options: &TableOptions,
) -> Result<Vec<String>> {
    let mut column_list_sql = this.get_column_declaration_list_sql(columns)?;

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
        name.get_quoted_name(this),
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

pub fn get_drop_sequence_sql<T: DatabasePlatform + ?Sized>(
    this: &T,
    sequence: &Sequence,
) -> Result<String> {
    if !this.supports_sequences() {
        Err(Error::platform_feature_unsupported(
            "Sequences are not supported by this platform",
        ))
    } else {
        Ok(format!("DROP SEQUENCE {}", sequence.get_quoted_name(this)))
    }
}

pub fn get_create_index_sql<T: DatabasePlatform + ?Sized>(
    this: &T,
    index: &Index,
    table: &Identifier,
) -> Result<String> {
    let columns = index.get_columns();

    if columns.is_empty() {
        return Err(Error::index_definition_invalid("columns"));
    }

    if index.is_primary() {
        this.get_create_primary_key_sql(index, table)
    } else {
        let table = table.get_quoted_name(this);
        let name = index.get_quoted_name(this);

        Ok(format!(
            "CREATE {}INDEX {} ON {} ({}){}",
            this.get_create_index_sql_flags(index),
            name,
            table,
            this.get_index_field_declaration_list_sql(index)?,
            this.get_partial_index_sql(index)
        ))
    }
}

pub fn get_partial_index_sql<T: DatabasePlatform + ?Sized>(this: &T, index: &Index) -> String {
    if this.supports_partial_indexes() && index.r#where.is_some() {
        format!(" WHERE {}", index.r#where.as_ref().unwrap())
    } else {
        "".to_string()
    }
}

pub fn get_create_index_sql_flags(index: &Index) -> String {
    if index.is_unique() {
        "UNIQUE".to_string()
    } else {
        "".to_string()
    }
}

pub fn get_create_primary_key_sql<T: DatabasePlatform + ?Sized>(
    this: &T,
    index: &Index,
    table: &Identifier,
) -> Result<String> {
    let table = table.get_quoted_name(this);
    Ok(format!(
        "ALTER TABLE {} ADD PRIMARY KEY ({})",
        table,
        this.get_index_field_declaration_list_sql(index)?
    ))
}

pub fn get_create_schema_sql<T: DatabasePlatform + ?Sized>(
    this: &T,
    schema_name: &str,
) -> Result<String> {
    if this.supports_schemas() {
        Err(Error::platform_feature_unsupported("schemas"))
    } else {
        Ok(format!("CREATE SCHEMA {}", schema_name))
    }
}

pub fn get_create_unique_constraint_sql<T: DatabasePlatform + ?Sized>(
    this: &T,
    constraint: &UniqueConstraint,
    table_name: &Identifier,
) -> Result<String> {
    let table = table_name.get_quoted_name(this);
    let query = format!(
        "ALTER TABLE {} ADD CONSTRAINT {} UNIQUE ({})",
        table,
        constraint.get_quoted_name(this),
        constraint.get_quoted_columns(this).join(", ")
    );

    Ok(query)
}

pub fn get_drop_schema_sql<T: DatabasePlatform + ?Sized>(
    this: &T,
    schema_name: &str,
) -> Result<String> {
    if this.supports_schemas() {
        Err(Error::platform_feature_unsupported("schemas"))
    } else {
        Ok(format!("DROP SCHEMA {}", schema_name))
    }
}

pub fn get_creed_type_comment(creed_type: &dyn Type) -> String {
    format!("(CRType:{})", creed_type.get_name())
}

pub fn get_column_comment<T: DatabasePlatform>(this: &T, column: &Column) -> Result<String> {
    let mut comment = column
        .get_comment()
        .as_ref()
        .map(|v| v.clone())
        .unwrap_or_default();
    let column_type = TypeManager::get_instance().get_type(column.get_type())?;
    if column_type.requires_sql_comment_hint(this) {
        comment += &this.get_creed_type_comment(column_type.as_ref());
    }

    Ok(comment)
}

pub fn quote_identifier<T: DatabasePlatform + ?Sized>(this: &T, identifier: &str) -> String {
    identifier
        .split('.')
        .map(|w| this.quote_single_identifier(w))
        .collect::<Vec<String>>()
        .join(".")
}

pub fn quote_single_identifier<T: DatabasePlatform + ?Sized>(this: &T, str: &str) -> String {
    let c = this.get_identifier_quote_character();
    format!("{}{}{}", c, str.replace(c, &c.to_string().repeat(2)), c)
}

pub fn quote_string_literal<T: DatabasePlatform + ?Sized>(this: &T, str: &str) -> String {
    let c = this.get_string_literal_quote_character();
    format!("{}{}{}", c, str.replace(c, &c.repeat(2)), c)
}

pub fn get_string_literal_quote_character() -> &'static str {
    "'"
}

pub fn get_create_foreign_key_sql<T: DatabasePlatform + ?Sized>(
    this: &T,
    foreign_key: &ForeignKeyConstraint,
    table: &Identifier,
) -> Result<String> {
    let table = table.get_quoted_name(this);
    Ok(format!(
        "ALTER TABLE {} ADD {}",
        table,
        this.get_foreign_key_declaration_sql(foreign_key)?
    ))
}

pub fn on_schema_alter_table_add_column<T: DatabasePlatform + Sync>(
    this: &T,
    column: &Column,
    diff: &TableDiff,
    mut column_sql: Vec<String>,
) -> Result<(bool, Vec<String>)> {
    let mut event = SchemaAlterTableAddColumnEvent::new(column, diff, this);
    this.get_event_manager().dispatch_sync(&mut event);

    let mut sql = event.get_sql().clone();
    column_sql.append(&mut sql);

    Ok((event.is_default_prevented(), column_sql))
}

pub fn on_schema_alter_table_remove_column<T: DatabasePlatform + Sync>(
    this: &T,
    column: &Column,
    diff: &TableDiff,
    mut column_sql: Vec<String>,
) -> Result<(bool, Vec<String>)> {
    let mut event = SchemaAlterTableRemoveColumnEvent::new(column, diff, this);
    this.get_event_manager().dispatch_sync(&mut event);

    let mut sql = event.get_sql().clone();
    column_sql.append(&mut sql);

    Ok((event.is_default_prevented(), column_sql))
}

pub fn on_schema_alter_table_change_column<T: DatabasePlatform + Sync>(
    this: &T,
    column_diff: &ColumnDiff,
    diff: &TableDiff,
    mut column_sql: Vec<String>,
) -> Result<(bool, Vec<String>)> {
    let mut event = SchemaAlterTableChangeColumnEvent::new(column_diff, diff, this);
    this.get_event_manager().dispatch_sync(&mut event);

    let mut sql = event.get_sql().clone();
    column_sql.append(&mut sql);

    Ok((event.is_default_prevented(), column_sql))
}

pub fn on_schema_alter_table_rename_column<T: DatabasePlatform + Sync>(
    this: &T,
    old_column_name: &str,
    column: &Column,
    diff: &TableDiff,
    mut column_sql: Vec<String>,
) -> Result<(bool, Vec<String>)> {
    let mut event = SchemaAlterTableRenameColumnEvent::new(old_column_name, column, diff, this);
    this.get_event_manager().dispatch_sync(&mut event);

    let mut sql = event.get_sql().clone();
    column_sql.append(&mut sql);

    Ok((event.is_default_prevented(), column_sql))
}

pub fn on_schema_alter_table<T: DatabasePlatform + Sync>(
    this: &T,
    diff: &TableDiff,
    mut sql: Vec<String>,
) -> Result<(bool, Vec<String>)> {
    let mut event = SchemaAlterTableEvent::new(diff, this);
    this.get_event_manager().dispatch_sync(&mut event);

    let mut alt_sql = event.get_sql().clone();
    sql.append(&mut alt_sql);

    Ok((event.is_default_prevented(), sql))
}

pub fn get_pre_alter_table_index_foreign_key_sql<T: DatabasePlatform + ?Sized>(
    this: &T,
    diff: &TableDiff,
) -> Result<Vec<String>> {
    let table_name = diff.get_name();

    let mut sql = vec![];
    if this.supports_foreign_key_constraints() {
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

pub fn get_post_alter_table_index_foreign_key_sql<T: DatabasePlatform + ?Sized>(
    this: &T,
    diff: &TableDiff,
) -> Result<Vec<String>> {
    let mut sql = vec![];
    let table_name = if let Some(n) = diff.get_new_name() {
        n
    } else {
        diff.get_name()
    };

    if this.supports_foreign_key_constraints() {
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

pub fn get_rename_index_sql<T: DatabasePlatform + ?Sized>(
    this: &T,
    old_index_name: &Identifier,
    index: &Index,
    table_name: &Identifier,
) -> Result<Vec<String>> {
    Ok(vec![
        this.get_drop_index_sql(old_index_name, table_name)?,
        this.get_create_index_sql(index, table_name)?,
    ])
}

pub fn get_column_declaration_list_sql<T: DatabasePlatform>(
    this: &T,
    columns: &[ColumnData],
) -> Result<String> {
    let mut declarations = vec![];
    for column in columns {
        declarations.push(this.get_column_declaration_sql(&column.name, column)?)
    }

    Ok(declarations.join(", "))
}

pub fn get_column_declaration_sql<T: DatabasePlatform>(
    this: &T,
    name: &str,
    column: &ColumnData,
) -> Result<String> {
    let declaration = if column.column_definition.is_some() {
        this.get_custom_type_declaration_sql(column)?
    } else {
        let default = this.get_default_value_declaration_sql(column)?;
        let charset = column
            .charset
            .as_ref()
            .map(|v| format!(" {}", v))
            .unwrap_or_default();
        let collation = column
            .collation
            .as_ref()
            .map(|v| format!(" {}", v))
            .unwrap_or_default();

        let not_null = if column.notnull { " NOT NULL" } else { "" };
        let unique = if column.unique { " UNIQUE" } else { "" };
        let check = if column.check.is_some() {
            format!(" {}", this.get_check_field_declaration_sql(column)?)
        } else {
            "".to_string()
        };

        let type_decl = TypeManager::get_instance()
            .get_type(column.r#type)?
            .get_sql_declaration(column, this)?;
        let mut comment_decl = "".to_string();
        if this.supports_inline_column_comments() {
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

    Ok(format!("{} {}", name, declaration))
}

pub fn get_decimal_type_declaration_sql(column: &ColumnData) -> Result<String> {
    let precision = column.precision.unwrap_or(10);
    let scale = column.scale.unwrap_or(0);

    Ok(format!("NUMERIC({}, {})", precision, scale))
}

pub fn get_default_value_declaration_sql<T: DatabasePlatform + ?Sized>(
    this: &T,
    column: &ColumnData,
) -> Result<String> {
    let default = column.default.clone();
    if matches!(default, Value::NULL) {
        return Ok((if column.notnull { "" } else { " DEFAULT NULL" }).to_string());
    }

    let t = column.r#type;
    if t == TypeId::of::<crate::r#type::IntegerType>() {
        return Ok(format!(" DEFAULT {}", default));
    }

    if (t == TypeId::of::<crate::r#type::DateTimeType>()
        || t == TypeId::of::<crate::r#type::DateTimeTzType>())
        && default == Value::String(this.get_current_timestamp_sql().to_string())
    {
        return Ok(format!(" DEFAULT {}", this.get_current_timestamp_sql()));
    }

    if t == TypeId::of::<crate::r#type::TimeType>()
        && default == Value::String(this.get_current_time_sql().to_string())
    {
        return Ok(format!(" DEFAULT {}", this.get_current_time_sql()));
    }

    if t == TypeId::of::<crate::r#type::DateType>()
        && default == Value::String(this.get_current_date_sql().to_string())
    {
        return Ok(format!(" DEFAULT {}", this.get_current_date_sql()));
    }

    if t == TypeId::of::<crate::r#type::BooleanType>() {
        return Ok(format!(" DEFAULT {}", this.convert_boolean(default)));
    }

    Ok(format!(
        " DEFAULT {}",
        this.quote_string_literal(&default.to_string())
    ))
}

pub fn get_check_declaration_sql<T: DatabasePlatform + ?Sized>(
    this: &T,
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

pub fn get_check_field_declaration_sql<T: DatabasePlatform + ?Sized>(
    this: &T,
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

pub fn get_unique_constraint_declaration_sql<T: DatabasePlatform + ?Sized>(
    this: &T,
    name: &str,
    constraint: &UniqueConstraint,
) -> Result<String> {
    let columns = constraint.get_quoted_columns(this);
    let name = Identifier::new(name, false);

    if columns.is_empty() {
        Err(Error::index_definition_invalid("columns"))
    } else {
        let mut constraint_flags = constraint.get_flags().clone();
        constraint_flags.push("UNIQUE".to_string());

        let constraint_name = name.get_quoted_name(this);
        let column_list_names = this.get_columns_field_declaration_list_sql(columns.as_slice())?;

        Ok(format!(
            "CONSTRAINT {} {} ({})",
            constraint_name,
            constraint_flags.join(" "),
            column_list_names
        ))
    }
}

pub fn get_index_declaration_sql<T: DatabasePlatform + ?Sized>(
    this: &T,
    name: &str,
    index: &Index,
) -> Result<String> {
    let columns = index.get_columns();
    let name = Identifier::new(name, false);

    if columns.is_empty() {
        Err(Error::index_definition_invalid("columns"))
    } else {
        Ok(format!(
            "{}INDEX {} ({}){}",
            this.get_create_index_sql_flags(index),
            name.get_quoted_name(this),
            this.get_index_field_declaration_list_sql(index)?,
            this.get_partial_index_sql(index)
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

pub fn get_index_field_declaration_list_sql<T: DatabasePlatform + ?Sized>(
    this: &T,
    index: &Index,
) -> Result<String> {
    Ok(index.get_quoted_columns(this).join(", "))
}

pub fn get_columns_field_declaration_list_sql(columns: &[String]) -> Result<String> {
    Ok(columns.join(", "))
}

pub fn get_temporary_table_name(table_name: &str) -> Result<String> {
    Ok(table_name.to_string())
}

pub fn get_foreign_key_declaration_sql<T: DatabasePlatform + ?Sized>(
    this: &T,
    foreign_key: &ForeignKeyConstraint,
) -> Result<String> {
    Ok(format!(
        "{}{}",
        this.get_foreign_key_base_declaration_sql(foreign_key)?,
        this.get_advanced_foreign_key_options_sql(foreign_key)?,
    ))
}

pub fn get_advanced_foreign_key_options_sql<T: DatabasePlatform + ?Sized>(
    this: &T,
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

pub fn get_foreign_key_base_declaration_sql<T: DatabasePlatform + ?Sized>(
    this: &T,
    foreign_key: &ForeignKeyConstraint,
) -> Result<String> {
    let sql = if foreign_key.get_name().is_empty() {
        "".to_string()
    } else {
        format!("CONSTRAINT {} ", foreign_key.get_quoted_name(this))
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
        foreign_key.get_quoted_local_columns(this).join(", "),
        foreign_key.get_quoted_foreign_table_name(this),
        foreign_key.get_quoted_foreign_columns(this).join(", "),
    ))
}

pub fn get_column_charset_declaration_sql() -> String {
    "".to_string()
}

pub fn get_column_collation_declaration_sql<T: DatabasePlatform + ?Sized>(
    this: &T,
    collation: &str,
) -> String {
    if this.supports_column_collation() {
        format!("COLLATE {}", collation)
    } else {
        "".to_string()
    }
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

pub fn convert_from_boolean(item: Value) -> Value {
    match item {
        Value::NULL => Value::Boolean(false),
        Value::Int(i) => Value::Boolean(i != 0),
        Value::UInt(u) => Value::Boolean(u != 0),
        Value::String(s) => Value::Boolean(!s.is_empty()),
        Value::Float(f) => Value::Boolean(f != 0.0),
        Value::Boolean(b) => Value::Boolean(b),
        Value::Json(j) => match j {
            serde_json::Value::Null => Value::Boolean(false),
            serde_json::Value::Bool(b) => Value::Boolean(b),
            serde_json::Value::Number(n) => {
                Value::Boolean(n != 0_i64.into() && n != Number::from_f64(0.0).unwrap())
            }
            serde_json::Value::String(s) => Value::Boolean(!s.is_empty()),
            serde_json::Value::Array(a) => Value::Boolean(!a.is_empty()),
            serde_json::Value::Object(_) => Value::Boolean(true),
        },
        _ => Value::Boolean(true),
    }
}

pub fn convert_booleans_to_database_value<T: DatabasePlatform + ?Sized>(
    this: &T,
    item: Value,
) -> Value {
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

pub fn get_create_view_sql(name: &str, sql: &str) -> Result<String> {
    Ok(format!("CREATE VIEW {} AS {}", name, sql))
}

pub fn get_drop_view_sql(name: &str) -> Result<String> {
    Ok(format!("DROP VIEW {}", name))
}

pub fn get_create_database_sql<T: DatabasePlatform + ?Sized>(
    this: &T,
    name: &str,
) -> Result<String> {
    if this.supports_create_drop_database() {
        Err(Error::platform_feature_unsupported("sequence next val"))
    } else {
        Ok(format!("CREATE DATABASE {}", name))
    }
}

pub fn get_drop_database_sql<T: DatabasePlatform + ?Sized>(this: &T, name: &str) -> Result<String> {
    if this.supports_create_drop_database() {
        Err(Error::platform_feature_unsupported("sequence next val"))
    } else {
        Ok(format!("DROP DATABASE {}", name))
    }
}

pub fn get_date_time_tz_type_declaration_sql<T: DatabasePlatform + ?Sized>(
    this: &T,
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

pub fn get_truncate_table_sql<T: DatabasePlatform + ?Sized>(
    this: &T,
    table_name: &Identifier,
) -> String {
    format!("TRUNCATE {}", table_name.get_quoted_name(this))
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

pub fn escape_string_for_like<T: DatabasePlatform + ?Sized>(
    this: &T,
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

pub fn columns_equal<T: DatabasePlatform>(
    this: &T,
    column1: &Column,
    column2: &Column,
) -> Result<bool> {
    let c1 = this.get_column_declaration_sql("", &column1.generate_column_data(this))?;
    let c2 = this.get_column_declaration_sql("", &column2.generate_column_data(this))?;

    Ok(if c1 != c2 {
        false
    } else if this.supports_inline_column_comments() {
        true
    } else if column1
        .get_comment()
        .clone()
        .unwrap_or_else(|| "".to_string())
        != column2
            .get_comment()
            .clone()
            .unwrap_or_else(|| "".to_string())
    {
        false
    } else {
        column1.get_type() == column2.get_type()
    })
}

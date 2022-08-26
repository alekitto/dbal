use crate::driver::postgres::platform::postgresql_platform::AbstractPostgreSQLPlatform;
use crate::error::ErrorKind;
use crate::platform::{default, DateIntervalUnit};
use crate::r#type::{BinaryType, BlobType, Type, TypeManager};
use crate::schema::{
    Asset, Column, ColumnData, ColumnDiff, ForeignKeyConstraint, Identifier, Index, Sequence,
    TableDiff, TableOptions,
};
use crate::{Error, Result, TransactionIsolationLevel, Value};
use itertools::Itertools;
use std::any::TypeId;

// const TRUE_BOOLEAN_LITERALS: [&str; 6] = ["t", "true", "y", "yes", "on", "1"];
const FALSE_BOOLEAN_LITERALS: [&str; 6] = ["f", "false", "n", "no", "off", "0"];

pub fn get_substring_expression(
    string: &str,
    start: usize,
    length: Option<usize>,
) -> Result<String> {
    if let Some(length) = length {
        Ok(format!(
            "SUBSTRING({} FROM {} FOR {})",
            string, start, length
        ))
    } else {
        Ok(format!("SUBSTRING({} FROM {})", string, start))
    }
}

pub fn get_regex_expression() -> Result<String> {
    Ok("SIMILAR TO".to_string())
}

pub fn get_locate_expression<T: AbstractPostgreSQLPlatform + ?Sized>(
    this: &T,
    str: &str,
    substr: &str,
    start_pos: Option<usize>,
) -> Result<String> {
    if let Some(start_pos) = start_pos {
        let str = this.get_substring_expression(str, start_pos, None)?;

        Ok(format!(
            "CASE WHEN (POSITION({} IN {}) = 0 THEN 0 ELSE (POSITION({} IN {}) + {} - 1 END",
            substr, str, substr, str, start_pos
        ))
    } else {
        Ok(format!("POSITION({} IN {})", substr, str))
    }
}

pub fn get_date_arithmetic_interval_expression(
    date: &str,
    operator: &str,
    mut interval: i64,
    mut unit: DateIntervalUnit,
) -> Result<String> {
    if unit == DateIntervalUnit::Quarter {
        interval *= 3;
        unit = DateIntervalUnit::Month;
    }

    Ok(format!(
        "({} {} ({} || {})::interval)",
        date, operator, interval, unit
    ))
}

pub fn get_date_diff_expression(date1: &str, date2: &str) -> Result<String> {
    Ok(format!("(DATE({})-DATE({}))", date1, date2))
}

pub fn get_current_database_expression() -> String {
    "CURRENT_DATABASE".to_string()
}

pub fn get_list_databases_sql() -> Result<String> {
    Ok("SELECT datname FROM pg_database".to_string())
}

pub fn get_list_sequences_sql<T: AbstractPostgreSQLPlatform + ?Sized>(
    this: &T,
    database: &str,
) -> Result<String> {
    Ok(format!(
        "SELECT sequence_name AS relname,
               sequence_schema AS schemaname,
               minimum_value AS min_value,
               increment AS increment_by
        FROM   information_schema.sequences
        WHERE  sequence_catalog = {}
        AND    sequence_schema NOT LIKE 'pg\\_%'
        AND    sequence_schema != 'information_schema'",
        this.quote_string_literal(database)
    ))
}

pub fn get_list_tables_sql() -> Result<String> {
    Ok("SELECT quote_ident(table_name) AS table_name,
                       table_schema AS schema_name
                FROM   information_schema.tables
                WHERE  table_schema NOT LIKE 'pg\\_%'
                AND    table_schema != 'information_schema'
                AND    table_name != 'geometry_columns'
                AND    table_name != 'spatial_ref_sys'
                AND    table_type != 'VIEW'"
        .to_string())
}

pub fn get_list_views_sql() -> Result<String> {
    Ok("SELECT quote_ident(table_name) AS viewname,
               table_schema AS schemaname,
               view_definition AS definition
        FROM   information_schema.views
        WHERE  view_definition IS NOT NULL"
        .to_string())
}

pub fn get_list_table_foreign_keys_sql<T: AbstractPostgreSQLPlatform + ?Sized>(
    this: &T,
    table: &str,
) -> Result<String> {
    Ok(format!("SELECT quote_ident(r.conname) as conname, pg_catalog.pg_get_constraintdef(r.oid, true) as condef
                  FROM pg_catalog.pg_constraint r
                  WHERE r.conrelid =
                  (
                      SELECT c.oid
                      FROM pg_catalog.pg_class c, pg_catalog.pg_namespace n
                      WHERE {} AND n.oid = c.relnamespace
                  )
                  AND r.contype = 'f'",
        get_table_where_clause(this, table, "c", "n")?
    ))
}

pub fn get_list_table_constraints_sql<T: AbstractPostgreSQLPlatform + ?Sized>(
    this: &T,
    table: &str,
) -> Result<String> {
    let table = this.quote_string_literal(&Identifier::new(table, false).get_name());

    Ok(format!(
        r#"
SELECT
    quote_ident(relname) as relname
FROM
    pg_class
WHERE oid IN (
    SELECT indexrelid
    FROM pg_index, pg_class
    WHERE pg_class.relname = {}
        AND pg_class.oid = pg_index.indrelid
        AND (indisunique = 't' OR indisprimary = 't')
    )
"#,
        table
    ))
}

pub fn get_list_table_indexes_sql<T: AbstractPostgreSQLPlatform + ?Sized>(
    this: &T,
    table: &str,
) -> Result<String> {
    Ok(format!(
        r#"SELECT quote_ident(relname) as relname, pg_index.indisunique, pg_index.indisprimary,
                  pg_index.indkey, pg_index.indrelid,
                  pg_get_expr(indpred, indrelid) AS where
            FROM pg_class, pg_index
            WHERE oid IN (
                SELECT indexrelid
                FROM pg_index si, pg_class sc, pg_namespace sn
                WHERE {}
                AND sc.oid=si.indrelid AND sc.relnamespace = sn.oid
            ) AND pg_index.indexrelid = oid'"#,
        get_table_where_clause(this, table, "sc", "sn")?
    ))
}

fn get_table_where_clause<T: AbstractPostgreSQLPlatform + ?Sized>(
    this: &T,
    table: &str,
    class_alias: &str,
    namespace_alias: &str,
) -> Result<String> {
    let where_clause = format!(
        "{}.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast') AND ",
        namespace_alias
    );
    let (schema, table) = if table.contains('.') {
        let (schema, table) = table.split_once('.').unwrap();
        let schema = this.quote_string_literal(schema);

        (schema, table.to_string())
    } else {
        ("ANY(current_schemas(false))".to_string(), table.to_string())
    };

    let table = Identifier::new(table, false);

    Ok(format!(
        "{}{}.relname = {} AND {}.nspname = {}",
        where_clause,
        class_alias,
        this.quote_string_literal(&table.get_name()),
        namespace_alias,
        schema
    ))
}

pub fn get_list_table_columns_sql<T: AbstractPostgreSQLPlatform + ?Sized>(
    this: &T,
    table: &str,
) -> Result<String> {
    Ok(format!(
        r#"
SELECT
    a.attnum,
    quote_ident(a.attname) AS field,
    t.typname AS type,
    format_type(a.atttypid, a.atttypmod) AS complete_type,
    (SELECT tc.collcollate FROM pg_catalog.pg_collation tc WHERE tc.oid = a.attcollation) AS collation,
    (SELECT t1.typname FROM pg_catalog.pg_type t1 WHERE t1.oid = t.typbasetype) AS domain_type,
    (SELECT format_type(t2.typbasetype, t2.typtypmod) FROM
       pg_catalog.pg_type t2 WHERE t2.typtype = 'd' AND t2.oid = a.atttypid) AS domain_complete_type,
    a.attnotnull AS isnotnull,
    (SELECT 't'
    FROM pg_index
    WHERE c.oid = pg_index.indrelid
       AND pg_index.indkey[0] = a.attnum
       AND pg_index.indisprimary = 't'
    ) AS pri,
    (SELECT pg_get_expr(adbin, adrelid)
    FROM pg_attrdef
    WHERE c.oid = pg_attrdef.adrelid
       AND pg_attrdef.adnum=a.attnum
    ) AS default,
    (SELECT pg_description.description
       FROM pg_description WHERE pg_description.objoid = c.oid AND a.attnum = pg_description.objsubid
    ) AS comment
    FROM pg_attribute a, pg_class c, pg_type t, pg_namespace n
    WHERE {}
       AND a.attnum > 0
       AND a.attrelid = c.oid
       AND a.atttypid = t.oid
       AND n.oid = c.relnamespace
    ORDER BY a.attnum
"#,
        get_table_where_clause(this, table, "c", "n")?
    ))
}

pub fn get_advanced_foreign_key_options_sql<T: AbstractPostgreSQLPlatform + ?Sized>(
    this: &T,
    foreign_key: &ForeignKeyConstraint,
) -> Result<String> {
    let mut query = "".to_string();

    if let Some(Value::String(m)) = foreign_key.get_option("match") {
        query += " MATCH ";
        query += m
    }

    query += &default::get_advanced_foreign_key_options_sql(this, foreign_key)?;

    let deferrable = foreign_key
        .get_option("deferrable")
        .cloned()
        .unwrap_or(Value::Boolean(false));
    if deferrable == Value::Boolean(true) {
        query += " DEFERRABLE";
    } else {
        query += " NOT DEFERRABLE";
    }

    let deferred = foreign_key
        .get_option("deferred")
        .cloned()
        .unwrap_or(Value::Boolean(false));
    if deferred == Value::Boolean(true) {
        query += " INITIALLY DEFERRED";
    } else {
        query += " INITIALLY IMMEDIATE";
    }

    Ok(query)
}

pub fn get_alter_table_sql<T: AbstractPostgreSQLPlatform + Sync>(
    this: &T,
    diff: &mut TableDiff,
) -> Result<Vec<String>> {
    let mut sql = vec![];
    let mut comments_sql = vec![];
    let mut column_sql = vec![];

    for column in &diff.added_columns {
        let (res, new_column_sql) =
            this.on_schema_alter_table_add_column(column, diff, column_sql)?;
        column_sql = new_column_sql;
        if res {
            continue;
        }

        let query = format!(
            "ADD {}",
            this.get_column_declaration_sql(
                &column.get_quoted_name(this),
                &column.generate_column_data(this)
            )?
        );
        sql.push(format!(
            "ALTER TABLE {} {}",
            diff.get_name().get_quoted_name(this),
            query
        ));

        let comment = this.get_column_comment(column)?;
        if comment.is_empty() {
            continue;
        }

        comments_sql.push(this.get_comment_on_column_sql(&diff.get_name(), column, &comment));
    }

    for column in &diff.removed_columns {
        let (res, new_column_sql) =
            this.on_schema_alter_table_remove_column(column, diff, column_sql)?;
        column_sql = new_column_sql;
        if res {
            continue;
        }

        let query = format!("DROP {}", column.get_quoted_name(this));
        sql.push(format!(
            "ALTER TABLE {} {}",
            diff.get_name().get_quoted_name(this),
            query
        ));
    }

    for column_diff in &diff.changed_columns {
        let (res, new_column_sql) =
            this.on_schema_alter_table_change_column(column_diff, diff, column_sql)?;
        column_sql = new_column_sql;
        if res || is_unchanged_binary_column(column_diff) {
            continue;
        }

        let old_column_name = column_diff.get_old_column_name().get_quoted_name(this);
        let column = &column_diff.column;

        if column_diff.has_changed("type")
            || column_diff.has_changed("precision")
            || column_diff.has_changed("scale")
            || column_diff.has_changed("fixed")
        {
            let r#type = TypeManager::get_instance().get_type(column.get_type())?;
            let mut column_data = column.generate_column_data(this);
            column_data.autoincrement = Some(false);

            let query = format!(
                "ALTER {} TYPE {}",
                old_column_name,
                r#type.get_sql_declaration(&column_data, this)?
            );
            sql.push(format!(
                "ALTER TABLE {} {}",
                diff.get_name().get_quoted_name(this),
                query
            ));
        }

        if column_diff.has_changed("default") {
            let column_data = column.generate_column_data(this);
            let default_clause = if column_data.default == Value::NULL {
                " DROP DEFAULT".to_string()
            } else {
                format!(
                    " SET{}",
                    this.get_default_value_declaration_sql(&column_data)?
                )
            };
            let query = format!("ALTER {}{}", old_column_name, default_clause);
            sql.push(format!(
                "ALTER TABLE {} {}",
                diff.get_name().get_quoted_name(this),
                query
            ));
        }

        if column_diff.has_changed("notnull") {
            let query = format!(
                "ALTER {} {} NOT NULL",
                old_column_name,
                if column.is_notnull() { "SET" } else { "DROP" }
            );
            sql.push(format!(
                "ALTER TABLE {} {}",
                diff.get_name().get_quoted_name(this),
                query
            ));
        }

        if column_diff.has_changed("autoincrement") {
            if column.is_autoincrement() {
                let seq_name = format!("{}_{}_seq", diff.name, old_column_name);

                sql.push(format!("CREATE SEQUENCE {}", seq_name));
                sql.push(format!(
                    "SELECT setval('{}', (SELECT MAX({}) FROM {}))",
                    seq_name,
                    old_column_name,
                    diff.get_name().get_quoted_name(this)
                ));
                let query = format!(
                    "ALTER {} SET DEFAULT nextval('{}')",
                    old_column_name, seq_name
                );
                sql.push(format!(
                    "ALTER TABLE {} {}",
                    diff.get_name().get_quoted_name(this),
                    query
                ));
            } else {
                let query = format!("ALTER {} DROP DEFAULT", old_column_name);
                sql.push(format!(
                    "ALTER TABLE {} {}",
                    diff.get_name().get_quoted_name(this),
                    query
                ));
            }
        }

        let new_comment = this.get_column_comment(column)?;
        let old_comment = get_old_column_comment(this, column_diff);

        if column_diff.has_changed("comment")
            || (column_diff.from_column.is_some() && old_comment != Some(new_comment.clone()))
        {
            comments_sql.push(this.get_comment_on_column_sql(
                &diff.get_name(),
                column,
                &new_comment,
            ));
        }

        if column_diff.has_changed("length") {
            let query = format!(
                "ALTER {} TYPE {}",
                old_column_name,
                TypeManager::get_instance()
                    .get_type(column.get_type())?
                    .get_sql_declaration(&column.generate_column_data(this), this)?
            );
            sql.push(format!(
                "ALTER TABLE {} {}",
                diff.get_name().get_quoted_name(this),
                query
            ));
        }
    }

    for (old_column_name, column) in &diff.renamed_columns {
        let (res, new_column_sql) = this.on_schema_alter_table_rename_column(
            old_column_name.as_str(),
            column,
            diff,
            column_sql,
        )?;

        column_sql = new_column_sql;
        if res {
            continue;
        }

        let old_column_name = Identifier::new(old_column_name, false);
        sql.push(format!(
            "ALTER TABLE {} RENAME COLUMN {} TO {}",
            diff.get_name().get_quoted_name(this),
            old_column_name.get_quoted_name(this),
            column.get_quoted_name(this)
        ));
    }

    let (res, mut table_sql) = this.on_schema_alter_table(diff, vec![])?;
    if !res {
        sql.append(&mut comments_sql);

        if let Some(new_name) = diff.get_new_name() {
            sql.push(format!(
                "ALTER TABLE {} RENAME TO {}",
                diff.get_name().get_quoted_name(this),
                new_name.get_quoted_name(this)
            ));
        }

        let mut new_sql = this.get_pre_alter_table_index_foreign_key_sql(diff)?;
        new_sql.append(&mut sql);
        new_sql.append(&mut this.get_post_alter_table_index_foreign_key_sql(diff)?);

        sql = new_sql;
    }

    sql.append(&mut table_sql);
    sql.append(&mut column_sql);

    Ok(sql)
}

/// Checks whether a given column diff is a logically unchanged binary type column.
///
/// Used to determine whether a column alteration for a binary type column can be skipped.
/// {@see BinaryType} and {@see BlobType} are mapped to the same database column type on this platform
/// as this platform does not have a native VARBINARY/BINARY column type. Therefore the comparator
/// might detect differences for binary type columns which do not have to be propagated
/// to database as there actually is no difference at database level.
fn is_unchanged_binary_column(column_diff: &ColumnDiff) -> bool {
    let column_type = column_diff.column.get_type();
    if TypeId::of::<BinaryType>() != column_type && TypeId::of::<BlobType>() != column_type {
        return false;
    }

    if let Some(from_column) = &column_diff.from_column {
        let from_column_type = from_column.get_type();
        if TypeId::of::<BinaryType>() != from_column_type
            && TypeId::of::<BlobType>() != from_column_type
        {
            return false;
        }

        column_diff.has_changed("type")
            || column_diff.has_changed("length")
            || column_diff.has_changed("fixed")
    } else if column_diff.has_changed("type") {
        false
    } else {
        column_diff.has_changed("length") || column_diff.has_changed("fixed")
    }
}

pub fn get_rename_index_sql<T: AbstractPostgreSQLPlatform + ?Sized>(
    this: &T,
    old_index_name: &Identifier,
    index: &Index,
    table_name: &Identifier,
) -> Result<Vec<String>> {
    let mut old_index_name = old_index_name.get_quoted_name(this);
    let table_name = table_name.get_quoted_name(this);
    if table_name.contains('.') {
        let schema = table_name.split_once('.').unwrap().0;
        old_index_name = format!("{}.{}", schema, old_index_name);
    }

    Ok(vec![format!(
        "ALTER INDEX {} RENAME TO {}",
        old_index_name,
        index.get_quoted_name(this)
    )])
}

pub fn get_comment_on_column_sql<T: AbstractPostgreSQLPlatform + ?Sized>(
    this: &T,
    table_name: &Identifier,
    column: &Column,
    comment: &str,
) -> String {
    let comment = if comment.is_empty() {
        "NULL".to_string()
    } else {
        this.quote_string_literal(comment)
    };
    format!(
        "COMMENT ON COLUMN {}.{} IS {}",
        table_name.get_quoted_name(this),
        column.get_quoted_name(this),
        comment
    )
}

pub fn get_create_sequence_sql<T: AbstractPostgreSQLPlatform + ?Sized>(
    this: &T,
    sequence: &Sequence,
) -> Result<String> {
    Ok(format!(
        "CREATE SEQUENCE {} INCREMENT BY {} MINVALUE {} START {} {}",
        sequence.get_quoted_name(this),
        sequence.get_allocation_size(),
        sequence.initial_value(),
        sequence.initial_value(),
        get_sequence_cache_sql(sequence)
    ))
}

pub fn get_alter_sequence_sql<T: AbstractPostgreSQLPlatform + ?Sized>(
    this: &T,
    sequence: &Sequence,
) -> Result<String> {
    Ok(format!(
        "ALTER SEQUENCE {} INCREMENT BY {} {}",
        sequence.get_quoted_name(this),
        sequence.get_allocation_size(),
        get_sequence_cache_sql(sequence)
    ))
}

/// Cache definition for sequences
pub fn get_sequence_cache_sql(sequence: &Sequence) -> String {
    let cache = sequence.get_cache().unwrap_or(0);
    if cache > 1 {
        format!(" CACHE {} ", cache)
    } else {
        String::new()
    }
}

pub fn get_drop_sequence_sql<T: AbstractPostgreSQLPlatform + ?Sized>(
    this: &T,
    sequence: &Sequence,
) -> Result<String> {
    default::get_drop_sequence_sql(this, sequence).map(|sql| sql + " CASCADE")
}

pub fn get_drop_foreign_key_sql<T: AbstractPostgreSQLPlatform + ?Sized>(
    this: &T,
    foreign_key: &ForeignKeyConstraint,
    table_name: &Identifier,
) -> Result<String> {
    this.get_drop_constraint_sql(
        Identifier::new(foreign_key.get_name(), foreign_key.is_quoted()),
        table_name,
    )
}

pub fn _get_create_table_sql<T: AbstractPostgreSQLPlatform>(
    this: &T,
    name: &Identifier,
    columns: &[ColumnData],
    options: &TableOptions,
) -> Result<Vec<String>> {
    let mut query_fields = this.get_column_declaration_list_sql(columns)?;
    if let Some((primary, _)) = &options.primary {
        let mut key_columns = primary.iter().unique();
        query_fields += &format!(", PRIMARY KEY({})", key_columns.join(", "));
    }

    let mut sql = vec![format!(
        "CREATE TABLE {} ({})",
        name.get_quoted_name(this),
        query_fields
    )];
    for index in options.indexes.values() {
        sql.push(this.get_create_index_sql(index, name)?);
    }

    for unique_constraint in options.unique_constraints.values() {
        sql.push(this.get_create_unique_constraint_sql(unique_constraint, name)?);
    }

    for foreign_key in &options.foreign_keys {
        sql.push(this.get_create_foreign_key_sql(foreign_key, name)?);
    }

    Ok(sql)
}

/// Converts a single boolean value.
///
/// First converts the value to its native PHP boolean type
/// and passes it to the given callback function to be reconverted
/// into any custom representation.
fn convert_single_boolean_value(
    value: &Value,
    callback: fn(Option<bool>) -> String,
) -> Result<Value> {
    let conv = match value {
        Value::NULL => None,
        Value::String(s) => {
            let s = s.trim().to_lowercase();
            if s == "true" {
                Some(true)
            } else if s == "false" {
                Some(false)
            } else {
                Err(Error::new(
                    ErrorKind::ConversionFailed,
                    format!("Unrecognized boolean literal '{:?}'", value),
                ))?
            }
        }
        Value::Boolean(b) => Some(*b),
        Value::Json(j) => match j {
            serde_json::Value::Null => None,
            serde_json::Value::Bool(b) => Some(*b),
            serde_json::Value::String(s) => {
                let s = s.trim().to_lowercase();
                if s == "true" {
                    Some(true)
                } else if s == "false" {
                    Some(false)
                } else {
                    Err(Error::new(
                        ErrorKind::ConversionFailed,
                        format!("Unrecognized boolean literal '{:?}'", value),
                    ))?
                }
            }
            _ => Err(Error::new(
                ErrorKind::ConversionFailed,
                format!("Unrecognized boolean literal '{:?}'", value),
            ))?,
        },
        _ => Err(Error::new(
            ErrorKind::ConversionFailed,
            format!("Unrecognized boolean literal '{:?}'", value),
        ))?,
    };

    Ok(Value::String(callback(conv)))
}

/// Converts one or multiple boolean values.
///
/// First converts the value(s) to their native PHP boolean type
/// and passes them to the given callback function to be reconverted
/// into any custom representation.
fn do_convert_booleans(item: &Value, callback: fn(Option<bool>) -> String) -> Result<Value> {
    match item {
        Value::Array(v) => Ok(Value::Array(
            v.iter()
                .map(|e| convert_single_boolean_value(e, callback))
                .try_collect()?,
        )),
        _ => Ok(convert_single_boolean_value(item, callback)?),
    }
}

pub fn convert_boolean(item: Value) -> Result<Value> {
    do_convert_booleans(&item, |value| {
        if let Some(value) = value {
            if value {
                "true"
            } else {
                "false"
            }
        } else {
            "NULL"
        }
        .to_string()
    })
}

pub fn convert_from_boolean(item: &Value) -> Value {
    match item {
        Value::Boolean(_) => item.clone(),
        Value::String(str) => {
            if FALSE_BOOLEAN_LITERALS.contains(&str.as_str()) {
                Value::Boolean(false)
            } else {
                default::convert_from_boolean(item)
            }
        }
        _ => default::convert_from_boolean(item),
    }
}

pub fn get_sequence_next_val_sql(sequence: &str) -> Result<String> {
    Ok(format!("SELECT NEXTVAL({})", sequence))
}

pub fn get_set_transaction_isolation_sql<T: AbstractPostgreSQLPlatform + ?Sized>(
    this: &T,
    level: TransactionIsolationLevel,
) -> Result<String> {
    Ok(format!(
        "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {}",
        this.get_transaction_isolation_level_sql(level)
    ))
}

pub fn get_boolean_type_declaration_sql() -> Result<String> {
    Ok("BOOLEAN".to_string())
}

pub fn get_integer_type_declaration_sql(column: &ColumnData) -> Result<String> {
    if column.autoincrement.unwrap_or(false) {
        Ok("SERIAL".to_string())
    } else {
        Ok("INT".to_string())
    }
}

pub fn get_bigint_type_declaration_sql(column: &ColumnData) -> Result<String> {
    if column.autoincrement.unwrap_or(false) {
        Ok("BIGSERIAL".to_string())
    } else {
        Ok("BIGINT".to_string())
    }
}

pub fn get_smallint_type_declaration_sql(column: &ColumnData) -> Result<String> {
    if column.autoincrement.unwrap_or(false) {
        Ok("SMALLSERIAL".to_string())
    } else {
        Ok("SMALLINT".to_string())
    }
}

pub fn get_guid_type_declaration_sql() -> Result<String> {
    Ok("UUID".to_string())
}

pub fn get_date_time_type_declaration_sql() -> Result<String> {
    Ok("TIMESTAMP(0) WITHOUT TIME ZONE".to_string())
}

pub fn get_date_time_tz_type_declaration_sql() -> Result<String> {
    Ok("TIMESTAMP(0) WITH TIME ZONE".to_string())
}

pub fn get_date_type_declaration_sql() -> Result<String> {
    Ok("DATE".to_string())
}

pub fn get_time_type_declaration_sql() -> Result<String> {
    Ok("TIME(0) WITHOUT TIME ZONE".to_string())
}

pub fn get_varchar_type_declaration_sql_snippet(
    length: Option<usize>,
    fixed: bool,
) -> Result<String> {
    let length = length.unwrap_or(255);

    Ok(if fixed {
        format!("CHAR({})", length)
    } else {
        format!("VARCHAR({})", length)
    })
}

pub fn get_binary_type_declaration_sql_snippet() -> Result<String> {
    Ok("BYTEA".to_string())
}

pub fn get_clob_type_declaration_sql() -> Result<String> {
    Ok("TEXT".to_string())
}

pub fn get_date_time_tz_format_string() -> &'static str {
    "Y-m-d H:i:sO"
}

pub fn get_empty_identity_insert_sql(
    quoted_table_name: &str,
    quoted_identifier_column_name: &str,
) -> String {
    format!(
        "INSERT INTO {} ({}) VALUES (DEFAULT)",
        quoted_table_name, quoted_identifier_column_name
    )
}

pub fn get_truncate_table_sql<T: AbstractPostgreSQLPlatform + ?Sized>(
    this: &T,
    table_name: &Identifier,
    cascade: bool,
) -> String {
    let mut sql = format!("TRUNCATE {}", table_name.get_quoted_name(this));
    if cascade {
        sql += " CASCADE";
    }

    sql
}

pub fn get_read_lock_sql() -> Result<String> {
    Ok("FOR SHARE".to_string())
}

pub fn get_blob_type_declaration_sql() -> Result<String> {
    Ok("BYTEA".to_string())
}

pub fn get_default_value_declaration_sql<T: AbstractPostgreSQLPlatform + ?Sized>(
    this: &T,
    column: &ColumnData,
) -> Result<String> {
    if column.autoincrement.unwrap_or(false) {
        Ok("".to_string())
    } else {
        default::get_default_value_declaration_sql(this, column)
    }
}

pub fn get_column_collation_declaration_sql<T: AbstractPostgreSQLPlatform + ?Sized>(
    this: &T,
    collation: &str,
) -> String {
    format!("COLLATE {}", this.quote_single_identifier(collation))
}

pub fn get_json_type_declaration_sql(column: &ColumnData) -> Result<String> {
    Ok(if column.jsonb.unwrap_or(false) {
        "JSONB"
    } else {
        "JSON"
    }
    .to_string())
}

fn get_old_column_comment<T: AbstractPostgreSQLPlatform>(
    this: &T,
    column_diff: &ColumnDiff,
) -> Option<String> {
    column_diff
        .from_column
        .as_ref()
        .map(|c| this.get_column_comment(c).unwrap_or_else(|_| String::new()))
}

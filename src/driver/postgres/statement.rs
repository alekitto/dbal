use super::driver::Driver;
use crate::driver::postgres::rows::PostgreSQLRowsIterator;
use crate::driver::statement_result::StatementResult;
use crate::error::{Error, StdError};
use crate::parameter_type::ParameterType;
use crate::{AsyncResult, Parameter, ParameterIndex, Parameters, Result, Rows, Value};
use dashmap::DashMap;
use log::debug;
use sqlparser::ast::{Expr, VisitMut, VisitorMut};
use sqlparser::dialect::{Dialect, GenericDialect, MySqlDialect, PostgreSqlDialect};
use sqlparser::parser::Parser;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter, Write};
use std::marker::PhantomData;
use std::ops::ControlFlow;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio_postgres::types::private::BytesMut;
use tokio_postgres::types::{Format, IsNull, ToSql, Type, to_sql_checked};

pub struct Statement<'conn> {
    pub(super) connection: &'conn Driver,
    pub(super) sql: String,
    parameters: DashMap<ParameterIndex, Parameter>,
    row_count: AtomicUsize,
    phantom_data: PhantomData<&'conn Self>,
}

fn bytes_to_binary(
    value: &Value,
    ty: &Type,
    out: &mut BytesMut,
) -> core::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
    if matches!(*ty, Type::BYTEA) {
        match value {
            Value::Bytes(b) => {
                out.extend_from_slice(b.as_slice());
                Ok(IsNull::No)
            }
            _ => Err(Box::new(StdError::from(Error::postgres_type_mismatch()))),
        }
    } else {
        Err(Box::new(StdError::from(Error::postgres_type_mismatch())))
    }
}

impl ToSql for Parameter {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> core::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match self.value_type {
            ParameterType::Null => Ok(IsNull::Yes),
            ParameterType::Integer => match &self.value {
                Value::Int(val) => {
                    out.write_str(&val.to_string())?;
                    Ok(IsNull::No)
                }
                Value::UInt(val) => {
                    out.write_str(&val.to_string())?;
                    Ok(IsNull::No)
                }
                _ => Err(Box::new(StdError::from(Error::postgres_type_mismatch()))),
            },
            ParameterType::String | ParameterType::Ascii => match &self.value {
                Value::String(val) => val.as_str().to_sql(ty, out),
                Value::DateTime(val) => val.to_sql(ty, out),
                Value::Json(val) => <String as ToSql>::to_sql(&val.to_string(), ty, out),
                Value::Uuid(val) => <String as ToSql>::to_sql(&val.to_string(), ty, out),
                _ => Err(Box::new(StdError::from(Error::postgres_type_mismatch()))),
            },
            ParameterType::LargeObject => bytes_to_binary(&self.value, ty, out),
            ParameterType::Float => match &self.value {
                Value::Float(val) => val.to_sql(ty, out),
                _ => Err(Box::new(StdError::from(Error::postgres_type_mismatch()))),
            },
            ParameterType::Boolean => match &self.value {
                Value::Boolean(val) => val.to_sql(ty, out),
                _ => Err(Box::new(StdError::from(Error::postgres_type_mismatch()))),
            },
            ParameterType::Binary => bytes_to_binary(&self.value, ty, out),
        }
    }

    fn accepts(_: &Type) -> bool {
        true
    }

    fn encode_format(&self, _ty: &Type) -> Format {
        match self.value_type {
            ParameterType::String | ParameterType::Integer => Format::Text,
            _ => Format::Binary,
        }
    }

    to_sql_checked!();
}

impl<'conn> Statement<'conn> {
    pub fn new(connection: &'conn Driver, sql: &str) -> Statement<'conn> {
        Statement {
            connection,
            sql: sql.to_string(),
            parameters: DashMap::new(),
            row_count: AtomicUsize::new(usize::MAX),
            phantom_data: PhantomData,
        }
    }

    async fn prepare_statement(
        &'conn self,
        params: Vec<(ParameterIndex, Parameter)>,
    ) -> Result<(tokio_postgres::Statement, Vec<Parameter>)> {
        let mut raw_params = Vec::with_capacity(params.len());
        let sql = self.sql.clone();

        if !params.is_empty() {
            let positional = params
                .iter()
                .any(|(i, _)| matches!(i, ParameterIndex::Positional(_)));
            let named = params
                .iter()
                .any(|(i, _)| matches!(i, ParameterIndex::Named(_)));
            if named && positional {
                return Err(Error::mixed_parameters_types());
            }

            let mut named_map: HashMap<String, usize> = HashMap::new();
            for (i, p) in params {
                if let ParameterIndex::Named(name) = i {
                    named_map.insert(name, raw_params.len() + 1);
                }

                raw_params.push(p);
            }
        }

        // We should parse the SQL query in order to replace the "?" or named parameters
        // with the postgresql indexed parameters ($1, $2, ...)
        let sql = Self::rewrite_placeholders(&sql);
        let statement = self.connection.client.prepare(&sql).await?;

        Ok((statement, raw_params))
    }

    fn rewrite_placeholders(sql: &str) -> Cow<'_, str> {
        let postgres = PostgreSqlDialect {};
        let mysql = MySqlDialect {};
        let generic = GenericDialect {};
        let dialects: [&dyn Dialect; 3] = [&postgres, &mysql, &generic];
        let mut last_error = None;

        for dialect in dialects {
            match Parser::parse_sql(dialect, sql) {
                Ok(mut statements) => {
                    let mut rewriter = PlaceholderRewriter::new();
                    let _ = VisitMut::visit(&mut statements, &mut rewriter);

                    if !rewriter.replaced {
                        return Cow::Borrowed(sql);
                    }

                    let rewritten = statements
                        .into_iter()
                        .map(|statement| statement.to_string())
                        .collect::<Vec<_>>()
                        .join("; ");

                    let trimmed_start = sql.trim_start_matches(char::is_whitespace);
                    let leading_len = sql.len() - trimmed_start.len();
                    let leading_ws = &sql[..leading_len];
                    let trimmed_end = trimmed_start.trim_end_matches(char::is_whitespace);
                    let trailing_ws = &trimmed_start[trimmed_end.len()..];
                    let had_semicolon = trimmed_end.ends_with(';');

                    let mut output = String::with_capacity(sql.len() + 8);
                    output.push_str(leading_ws);
                    output.push_str(&rewritten);

                    if had_semicolon {
                        output.push(';');
                    }

                    output.push_str(trailing_ws);

                    return Cow::Owned(output);
                }
                Err(err) => last_error = Some(err),
            }
        }

        if let Some(err) = last_error {
            debug!("failed to parse SQL for placeholder rewrite: {err}");
        }

        Cow::Borrowed(sql)
    }

    async fn internal_query(&'conn self, params: Vec<(ParameterIndex, Parameter)>) -> Result<Rows> {
        let (statement, raw_params) = self.prepare_statement(params).await?;
        let row_stream = self
            .connection
            .client
            .query_raw(&statement, raw_params)
            .await?;

        let iterator = PostgreSQLRowsIterator::new(row_stream, &statement)?;
        let rows = Rows::new(iterator.columns().clone(), 0, None, Box::pin(iterator));
        self.row_count.store(rows.len(), Ordering::SeqCst);

        Ok(rows)
    }

    async fn internal_execute(
        &'conn self,
        params: Vec<(ParameterIndex, Parameter)>,
    ) -> Result<usize> {
        let (statement, raw_params) = self.prepare_statement(params).await?;
        let affected_rows = self
            .connection
            .client
            .execute_raw(&statement, raw_params)
            .await? as usize;

        self.row_count.store(affected_rows, Ordering::SeqCst);
        Ok(affected_rows)
    }
}

impl<'conn> Debug for Statement<'conn> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgreSQL Statement")
            .field("sql", &self.sql)
            .field("parameters", &self.parameters)
            .finish()
    }
}

impl<'conn> crate::driver::statement::Statement<'conn> for Statement<'conn> {
    fn bind_value(&self, param: ParameterIndex, value: Parameter) -> Result<()> {
        self.parameters.insert(param, value);
        Ok(())
    }

    fn query(&self, params: Parameters) -> AsyncResult<'_, StatementResult> {
        let params = Vec::from(params);

        Box::pin(async move { Ok(StatementResult::new(self.internal_query(params).await?)) })
    }

    fn query_owned(
        self: Box<Self>,
        params: Vec<(ParameterIndex, Parameter)>,
    ) -> AsyncResult<'conn, StatementResult> {
        Box::pin(async move { Ok(StatementResult::new(self.internal_query(params).await?)) })
    }

    fn execute(&self, params: Parameters) -> AsyncResult<'_, usize> {
        let params = Vec::from(params);

        Box::pin(async move { self.internal_execute(params).await })
    }

    fn execute_owned(
        self: Box<Self>,
        params: Vec<(ParameterIndex, Parameter)>,
    ) -> AsyncResult<'conn, usize> {
        Box::pin(async move { self.internal_execute(params).await })
    }

    fn row_count(&self) -> usize {
        self.row_count.load(Ordering::SeqCst)
    }
}

struct PlaceholderRewriter {
    next_index: usize,
    replaced: bool,
}

impl PlaceholderRewriter {
    fn new() -> Self {
        Self {
            next_index: 1,
            replaced: false,
        }
    }
}

impl VisitorMut for PlaceholderRewriter {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::Value(v) = expr
            && let sqlparser::ast::Value::Placeholder(placeholder) = &mut v.value
            && placeholder.starts_with('?')
        {
            *placeholder = format!("${}", self.next_index);
            self.next_index += 1;
            self.replaced = true;
        }

        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrites_placeholders_for_postgres() {
        let sql = "SELECT ? FROM cache_entries WHERE id = ?";
        let rewritten = Statement::rewrite_placeholders(sql);
        assert_eq!(
            rewritten.as_ref(),
            "SELECT $1 FROM cache_entries WHERE id = $2"
        );
    }

    #[test]
    fn ignores_question_marks_inside_literals() {
        let sql = "SELECT '?' AS literal, ? AS param";
        let rewritten = Statement::rewrite_placeholders(sql);
        assert_eq!(rewritten.as_ref(), "SELECT '?' AS literal, $1 AS param");
    }

    #[test]
    fn falls_back_to_original_when_parse_fails() {
        let sql = "SELECT ? FROM";
        let rewritten = Statement::rewrite_placeholders(sql);
        assert!(matches!(
            rewritten,
            Cow::Borrowed(returned) if std::ptr::eq(returned, sql)
        ));
    }
}

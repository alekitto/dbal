use super::driver::Driver;
use crate::driver::postgres::rows::PostgreSQLRowsIterator;
use crate::driver::statement_result::StatementResult;
use crate::error::{Error, StdError};
use crate::parameter_type::ParameterType;
use crate::{AsyncResult, Parameter, ParameterIndex, Parameters, Result, Rows, Value};
use dashmap::DashMap;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio_postgres::types::private::BytesMut;
use tokio_postgres::types::{to_sql_checked, IsNull, ToSql, Type};

pub struct Statement<'conn> {
    pub(super) connection: &'conn Driver,
    pub(super) sql: String,
    parameters: DashMap<ParameterIndex, Parameter>,
    row_count: AtomicUsize,
    phantom_data: PhantomData<&'conn Self>,
}

fn type_to_sql(type_: ParameterType) -> Type {
    match type_ {
        ParameterType::Null => Type::INT4,
        ParameterType::Integer => Type::INT8,
        ParameterType::String => Type::VARCHAR,
        ParameterType::LargeObject => Type::BYTEA,
        ParameterType::Float => Type::FLOAT8,
        ParameterType::Boolean => Type::BOOL,
        ParameterType::Binary => Type::BYTEA,
        ParameterType::Ascii => Type::VARCHAR,
    }
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
                Value::Int(val) => <i64 as ToSql>::to_sql(val, ty, out),
                Value::UInt(val) => <i64 as ToSql>::to_sql(&(*val as i64), ty, out),
                _ => Err(Box::new(StdError::from(Error::postgres_type_mismatch()))),
            },
            ParameterType::String | ParameterType::Ascii => match &self.value {
                Value::String(val) => <&str as ToSql>::to_sql(&val.as_str(), ty, out),
                Value::DateTime(val) => <String as ToSql>::to_sql(&val.to_rfc3339(), ty, out),
                Value::Json(val) => <String as ToSql>::to_sql(&val.to_string(), ty, out),
                Value::Uuid(val) => <String as ToSql>::to_sql(&val.to_string(), ty, out),
                _ => Err(Box::new(StdError::from(Error::postgres_type_mismatch()))),
            },
            ParameterType::LargeObject => bytes_to_binary(&self.value, ty, out),
            ParameterType::Float => match &self.value {
                Value::Float(val) => <f64 as ToSql>::to_sql(val, ty, out),
                _ => Err(Box::new(StdError::from(Error::postgres_type_mismatch()))),
            },
            ParameterType::Boolean => match &self.value {
                Value::Boolean(val) => <bool as ToSql>::to_sql(val, ty, out),
                _ => Err(Box::new(StdError::from(Error::postgres_type_mismatch()))),
            },
            ParameterType::Binary => bytes_to_binary(&self.value, ty, out),
        }
    }

    fn accepts(_: &Type) -> bool {
        true
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
            phantom_data: PhantomData::default(),
        }
    }

    async fn prepare_statement(
        &'conn self,
        params: Vec<(ParameterIndex, Parameter)>,
    ) -> Result<(tokio_postgres::Statement, Vec<Parameter>)> {
        let mut types = Vec::with_capacity(params.len());
        let mut raw_params = Vec::with_capacity(params.len());
        for (i, p) in params {
            match i {
                ParameterIndex::Named(_) => return Err(Error::unsupported_named_parameters()),
                ParameterIndex::Positional(pos) => types.insert(pos, type_to_sql(p.value_type)),
            }

            raw_params.push(p);
        }

        let statement = self
            .connection
            .client
            .prepare_typed(&self.sql, types.as_slice())
            .await?;

        Ok((statement, raw_params))
    }

    async fn internal_query(&'conn self, params: Vec<(ParameterIndex, Parameter)>) -> Result<Rows> {
        let (statement, raw_params) = self.prepare_statement(params).await?;
        let row_stream = self
            .connection
            .client
            .query_raw(&statement, raw_params)
            .await?;

        let iterator = PostgreSQLRowsIterator::new(row_stream).await?;
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

    fn query(&self, params: Parameters) -> AsyncResult<StatementResult> {
        let params = Vec::from(params);

        Box::pin(async move { Ok(StatementResult::new(self.internal_query(params).await?)) })
    }

    fn query_owned(
        self: Box<Self>,
        params: Vec<(ParameterIndex, Parameter)>,
    ) -> AsyncResult<'conn, StatementResult> {
        Box::pin(async move { Ok(StatementResult::new(self.internal_query(params).await?)) })
    }

    fn execute(&self, params: Parameters) -> AsyncResult<usize> {
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

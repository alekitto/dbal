use super::driver::Driver;
use crate::driver::mysql::rows::MySQLRowsIterator;
use crate::driver::statement_result::StatementResult;
use crate::error::Error;
use crate::parameter_type::ParameterType;
use crate::{AsyncResult, Parameter, ParameterIndex, Parameters, Result, Rows};
use mysql_async::prelude::*;
use mysql_async::{Params, Value};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::ops::DerefMut;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

pub struct Statement<'conn> {
    pub(super) connection: &'conn Driver,
    pub(super) sql: String,
    parameters: Arc<Mutex<HashMap<ParameterIndex, Parameter>>>,
    row_count: AtomicUsize,
    phantom_data: PhantomData<&'conn Self>,
}

impl TryFrom<Parameters<'_>> for Params {
    type Error = Error;

    fn try_from(params: Parameters) -> Result<Self> {
        if params.is_empty() {
            Ok(Params::Empty)
        } else {
            let mut params = Vec::from(params);
            let positional = params
                .iter()
                .all(|(i, _)| matches!(i, ParameterIndex::Positional(_)));
            let named = params
                .iter()
                .all(|(i, _)| matches!(i, ParameterIndex::Named(_)));

            if positional {
                params.sort_by(|(a_index, _), (b_index, _)| {
                    let a_index = match a_index {
                        ParameterIndex::Positional(pos) => *pos,
                        _ => unreachable!(),
                    };

                    let b_index = match b_index {
                        ParameterIndex::Positional(pos) => *pos,
                        _ => unreachable!(),
                    };

                    a_index.cmp(&b_index)
                });

                let mut result_params = vec![];
                for (_, p) in params {
                    let t = match p.value_type {
                        ParameterType::Null => Value::NULL,
                        ParameterType::Integer | ParameterType::Boolean => {
                            Value::Int(p.try_into()?)
                        }
                        ParameterType::String
                        | ParameterType::LargeObject
                        | ParameterType::Binary
                        | ParameterType::Ascii => Value::Bytes(p.try_into()?),
                        ParameterType::Float => Value::Double(p.try_into()?),
                    };

                    result_params.push(t);
                }

                Ok(Params::Positional(result_params))
            } else if named {
                Ok(Params::Empty)
            } else {
                Err(Error::mixed_parameters_types())
            }
        }
    }
}

impl<'conn> Statement<'conn> {
    pub fn new(connection: &'conn Driver, sql: &str) -> Result<Statement<'conn>> {
        Ok(Statement {
            connection,
            sql: sql.to_string(),
            parameters: Arc::new(Mutex::new(HashMap::new())),
            row_count: AtomicUsize::new(usize::MAX),
            phantom_data: PhantomData,
        })
    }

    async fn internal_query(&'conn self, params: Vec<(ParameterIndex, Parameter)>) -> Result<Rows> {
        let params = self.parameters_to_params(params)?;
        let mut connection = self.connection.connection.lock().await;

        let result = self
            .sql
            .clone()
            .with(params)
            .run(connection.deref_mut())
            .await?;

        let columns = result
            .columns()
            .map(|cols| cols.iter().map(|col| col.name_str().to_string()).collect())
            .unwrap_or_else(Vec::new);

        let last_insert_id = result.last_insert_id().map(|id| id.to_string());
        let iterator = Box::pin(MySQLRowsIterator::new(result).await?);
        self.row_count.store(iterator.len(), Ordering::SeqCst);

        Ok(Rows::new(columns, iterator.len(), last_insert_id, iterator))
    }

    async fn internal_execute(
        &'conn self,
        params: Vec<(ParameterIndex, Parameter)>,
    ) -> Result<usize> {
        let params = self.parameters_to_params(params)?;
        let mut connection = self.connection.connection.lock().await;

        self.sql
            .clone()
            .with(params)
            .ignore(connection.deref_mut())
            .await?;

        let affected_rows = connection.affected_rows() as usize;
        self.row_count.store(affected_rows, Ordering::SeqCst);

        Ok(affected_rows)
    }

    fn parameters_to_params(
        &'conn self,
        params: Vec<(ParameterIndex, Parameter)>,
    ) -> Result<Params> {
        Params::try_from(Parameters::Vec(params))
    }
}

impl<'conn> Debug for Statement<'conn> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MySQL Statement")
            .field("sql", &self.sql)
            .field("parameters", &self.parameters.lock().unwrap())
            .finish()
    }
}

impl<'conn> crate::driver::statement::Statement<'conn> for Statement<'conn> {
    fn bind_value(&self, param: ParameterIndex, value: Parameter) -> Result<()> {
        let mut parameters = self.parameters.lock().unwrap();
        parameters.insert(param, value);

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

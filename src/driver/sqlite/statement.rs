use crate::driver::sqlite::driver::Driver;
use crate::driver::sqlite::rows::SqliteRowsIterator;
use crate::driver::statement_result::StatementResult;
use crate::{AsyncResult, Parameter, ParameterIndex, Parameters, Result, Rows};
use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

pub struct StatementWrapper<'conn>(pub(crate) rusqlite::Statement<'conn>);
unsafe impl<'conn> Sync for StatementWrapper<'conn> {}
unsafe impl<'conn> Send for StatementWrapper<'conn> {}

pub struct Statement<'conn> {
    pub(super) statement: Arc<Mutex<StatementWrapper<'conn>>>,
    row_count: AtomicUsize,
}

impl<'conn> Statement<'conn> {
    pub fn new(connection: &'conn Driver, sql: &str) -> Result<Self> {
        let prepared = connection.connection.0.prepare(sql)?;

        Ok(Statement {
            statement: Arc::new(Mutex::new(StatementWrapper(prepared))),
            row_count: AtomicUsize::new(usize::MAX),
        })
    }

    fn internal_execute(&self, params: Parameters<'_>) -> Result<usize> {
        let params = Vec::from(params);
        self._bind_params(params)?;

        let mut statement = self.statement.lock().unwrap();
        match statement.0.raw_execute() {
            Ok(size) => {
                self.row_count.store(size, Ordering::SeqCst);
                Ok(size)
            }
            Err(e) => match e {
                rusqlite::Error::ExecuteReturnedResults => Ok(0),
                _ => Err(e.into()),
            },
        }
    }

    fn internal_query(&self, params: Parameters<'_>) -> Result<Rows> {
        let params = Vec::from(params);
        self._bind_params(params)?;

        let iterator = SqliteRowsIterator::new(self)?;
        let rows = Rows::new(
            iterator.columns().clone(),
            iterator.len(),
            None,
            Box::pin(iterator),
        );
        self.row_count.store(rows.len(), Ordering::SeqCst);

        Ok(rows)
    }

    fn _bind_params(&self, params: Vec<(ParameterIndex, Parameter)>) -> Result<()> {
        use crate::driver::statement::Statement;
        for (idx, param) in params.into_iter() {
            let result = self.bind_value(idx, param);
            #[allow(clippy::question_mark)]
            if result.is_err() {
                return result;
            }
        }

        Ok(())
    }
}

impl<'conn> Debug for Statement<'conn> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SQLite Statement")
            .field(
                "expanded_sql",
                &self
                    .statement
                    .lock()
                    .unwrap()
                    .0
                    .expanded_sql()
                    .unwrap_or_default(),
            )
            .finish()
    }
}

impl<'conn> crate::driver::statement::Statement<'conn> for Statement<'conn> {
    fn bind_value(&self, idx: ParameterIndex, value: Parameter) -> Result<()> {
        let idx = match idx {
            ParameterIndex::Positional(i) => i,
            ParameterIndex::Named(name) => self
                .statement
                .lock()
                .unwrap()
                .0
                .parameter_index(name.as_str())
                .unwrap()
                .unwrap(),
        };

        self.statement
            .lock()
            .unwrap()
            .0
            .raw_bind_parameter(idx + 1, value)?;
        Ok(())
    }

    fn query(&self, params: Parameters) -> AsyncResult<StatementResult> {
        let result = self.internal_query(params);
        Box::pin(async move { Ok(StatementResult::new(result?)) })
    }

    fn query_owned(
        self: Box<Self>,
        params: Vec<(ParameterIndex, Parameter)>,
    ) -> AsyncResult<'conn, StatementResult> {
        let result = self.internal_query(Parameters::Vec(params));
        Box::pin(async move { Ok(StatementResult::new(result?)) })
    }

    fn execute(&self, params: Parameters) -> AsyncResult<usize> {
        let result = self.internal_execute(params);
        Box::pin(async move { result })
    }

    fn execute_owned(
        self: Box<Self>,
        params: Vec<(ParameterIndex, Parameter)>,
    ) -> AsyncResult<'conn, usize> {
        let result = self.internal_execute(Parameters::Vec(params));
        Box::pin(async move { result })
    }

    fn row_count(&self) -> usize {
        self.row_count.load(Ordering::SeqCst)
    }
}

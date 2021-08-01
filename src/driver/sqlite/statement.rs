use crate::driver::sqlite::driver::Driver;
use crate::driver::sqlite::rows::Rows;
use crate::driver::sqlite::statement_result::StatementResult;
use crate::driver::statement::StatementExecuteResult;
use crate::{Parameter, ParameterIndex, Parameters, Result};
use std::cell::RefCell;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct Statement<'conn> {
    pub(super) statement: RefCell<rusqlite::Statement<'conn>>,
    row_count: AtomicUsize,
}

impl<'conn> Statement<'conn> {
    pub fn new(connection: &'conn Driver, sql: &str) -> Result<Self> {
        let prepared = connection.connection.prepare(sql)?;

        Ok(Statement {
            statement: RefCell::new(prepared),
            row_count: AtomicUsize::new(usize::MAX),
        })
    }

    fn internal_execute(&self, params: Parameters<'_>) -> Result<(Rows, usize)> {
        let params = Vec::from(params);
        self._bind_params(params)?;

        let (row_count, column_count) = {
            let mut statement = self.statement.borrow_mut();
            let row_count = match statement.raw_execute() {
                Ok(value) => Ok(value),
                Err(rusqlite::Error::ExecuteReturnedResults) => Ok(0),
                Err(err) => Err(err),
            }?;

            let column_count = statement.column_count();

            (row_count, column_count)
        };

        self.row_count.store(row_count, Ordering::Relaxed);
        let rows = Rows::new(self)?;

        Ok((rows, column_count))
    }

    fn _bind_params(&self, params: Vec<(ParameterIndex, Parameter)>) -> Result<()> {
        use crate::driver::statement::Statement;
        for (idx, param) in params.into_iter() {
            let result = self.bind_value(idx, param);
            if result.is_err() {
                return result;
            }
        }

        Ok(())
    }
}

impl<'conn> crate::driver::statement::Statement for Statement<'conn> {
    type StatementResult = super::statement_result::StatementResult;

    fn bind_value(&self, idx: ParameterIndex, value: Parameter) -> Result<()> {
        let idx = match idx {
            ParameterIndex::Positional(i) => i as usize,
            ParameterIndex::Named(name) => self
                .statement
                .borrow()
                .parameter_index(name.as_str())
                .unwrap()
                .unwrap(),
        };

        self.statement
            .borrow_mut()
            .raw_bind_parameter(idx + 1, value)?;
        Ok(())
    }

    fn execute(&self, params: Parameters) -> StatementExecuteResult<StatementResult> {
        let result = self.internal_execute(params);
        Box::pin(async move {
            let (rows, column_count) = result?;

            Ok(StatementResult::new(column_count, rows))
        })
    }

    fn row_count(&self) -> usize {
        self.row_count.load(Ordering::Relaxed)
    }
}

use crate::driver::sqlite::driver::Driver;
use crate::driver::sqlite::rows::Rows;
use crate::{Parameter, ParameterIndex, Result, Row};
use fallible_iterator::FallibleIterator;

pub struct Statement<'conn> {
    pub(in crate::driver::sqlite) statement: rusqlite::Statement<'conn>,
    row_count: Option<usize>,
}

impl<'conn> Statement<'conn> {
    pub fn new(connection: &'conn Driver, sql: &str) -> Result<Self> {
        let prepared = connection.connection.prepare(sql)?;

        Ok(Statement {
            statement: prepared,
            row_count: None,
        })
    }

    fn _bind_params(&mut self, params: Vec<(ParameterIndex, Parameter)>) -> Result<()> {
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

impl<'conn, 's> crate::driver::statement::Statement<'s> for Statement<'conn> {
    fn bind_value(&mut self, idx: ParameterIndex, value: Parameter) -> Result<()> {
        let idx = match idx {
            ParameterIndex::Positional(i) => i as usize,
            ParameterIndex::Named(name) => self
                .statement
                .parameter_index(name.as_str())
                .unwrap()
                .unwrap(),
        };

        self.statement.raw_bind_parameter(idx + 1, value)?;
        Ok(())
    }

    fn execute(&mut self, params: Vec<(ParameterIndex, Parameter)>) -> Result<()> {
        self._bind_params(params)?;
        self.row_count = match self.statement.raw_execute() {
            Ok(value) => Ok(Some(value)),
            Err(rusqlite::Error::ExecuteReturnedResults) => Ok(Some(0)),
            Err(err) => Err(err),
        }?;

        Ok(())
    }

    fn row_count(&self) -> usize {
        match &self.row_count {
            None => 0,
            Some(rows) => rows.clone(),
        }
    }

    fn fetch_all(&mut self) -> Result<Vec<Row>> {
        let mut result = Vec::new();
        let mut rows = Rows::new(self);
        while let Some(row) = rows.next()? {
            result.push(row);
        }

        Ok(result)
    }
}

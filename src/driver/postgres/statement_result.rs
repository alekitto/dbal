use super::rows::Rows;
use crate::{Result, Row};
use fallible_iterator::FallibleIterator;
use std::cell::RefCell;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

pub struct StatementResult {
    column_count: usize,
    rows: Arc<RefCell<Rows>>,
}

impl StatementResult {
    pub(super) fn new(column_count: usize, rows: Rows) -> Self {
        Self {
            column_count,
            rows: Arc::new(RefCell::new(rows)),
        }
    }
}

impl Debug for StatementResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgreSQL StatementResult")
            .field("column_count", &self.column_count)
            .field(
                "rows",
                &format!("[ len: {} ]", self.rows.borrow().rows.len()),
            )
            .finish()
    }
}

impl crate::driver::statement_result::StatementResult for StatementResult {
    fn fetch_one(&self) -> Result<Option<Row>> {
        let mut rows = self.rows.borrow_mut();
        rows.next()
    }

    fn fetch_all(&self) -> Result<Vec<Row>> {
        let mut result = Vec::new();
        let mut rows = self.rows.borrow_mut();

        while let Some(row) = rows.next()? {
            result.push(row);
        }

        Ok(result)
    }

    fn column_count(&self) -> usize {
        self.column_count
    }
}

use super::rows::Rows;
use crate::rows::RowsIterator;
use crate::{Row, Rows as _};
use std::fmt::{Debug, Formatter};

pub struct StatementResult {
    column_count: usize,
    length: usize,
    rows: RowsIterator<Rows>,
}

impl StatementResult {
    pub(super) fn new(column_count: usize, rows: Rows) -> Self {
        Self {
            column_count,
            length: rows.len(),
            rows: rows.into_iterator(),
        }
    }
}

impl Debug for StatementResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgreSQL StatementResult")
            .field("column_count", &self.column_count)
            .field("rows", &format!("[ len: {} ]", self.length))
            .finish()
    }
}

impl crate::driver::statement_result::StatementResult for StatementResult {
    fn fetch_one(&mut self) -> Option<&Row> {
        self.rows.next()
    }

    fn fetch_all(self) -> Vec<Row> {
        self.rows.to_vec()
    }

    fn column_count(&self) -> usize {
        self.column_count
    }
}

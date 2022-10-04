use crate::{Result, Row, Rows};
use creed::rows::RowsIterator;
use std::fmt::{Debug, Formatter};
use std::future::Future;

pub struct StatementResult {
    column_count: usize,
    rows: RowsIterator,
    last_insert_id: Option<String>,
}

impl Debug for StatementResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("creed::StatementResult")
            .field("column_count", &self.column_count)
            .field("rows", &format!("[ len: {} ]", self.rows.len()))
            .finish()
    }
}

impl StatementResult {
    pub fn new(rows: Rows) -> Self {
        Self {
            column_count: rows.column_count(),
            last_insert_id: rows.last_insert_id().clone(),
            rows: rows.into_iterator(),
        }
    }

    /// Returns the *NEXT* row of the statement if any.
    /// If the iterator has been consumed fully, [None] is returned.
    pub fn fetch_one(&mut self) -> impl Future<Output = Result<Option<Row>>> + '_ {
        self.rows.next()
    }

    /// Returns all the *REMAINING* rows of the statement if any.
    /// Builds and return a vector of Row objects which can be queried to get the data.
    ///
    /// If the iterator has been consumed partly, only the remaining rows are collected
    /// and returned into the vector.
    /// Consequently, if the statement has been fetched fully, an empty vector is returned.
    pub fn fetch_all(self) -> impl Future<Output = Result<Vec<Row>>> {
        self.rows.to_vec()
    }

    /// Returns the ID of the last inserted row, or the last value from a sequence object,
    /// depending on the underlying driver.
    ///
    /// # Note
    /// This method may not return a meaningful or consistent result across different drivers,
    /// because the underlying database may not even support the notion of AUTO_INCREMENT/IDENTITY
    /// columns or sequences.
    pub fn last_insert_id(&self) -> Option<&str> {
        if let Some(id) = &self.last_insert_id {
            Some(id)
        } else {
            None
        }
    }

    /// Returns the number of columns in the result set
    /// If there is no result set, 0 is returned
    pub fn column_count(&self) -> usize {
        self.column_count
    }
}

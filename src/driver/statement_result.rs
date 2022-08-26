use crate::Row;
use delegate::delegate;
use std::fmt::Debug;

pub trait StatementResult: Debug {
    /// Returns the *NEXT* row of the statement if any.
    /// If the iterator has been consumed fully, [None] is returned.
    fn fetch_one(&mut self) -> Option<&Row>;

    /// Returns all the *REMAINING* rows of the statement if any.
    /// Builds and return a vector of Row objects which can be queried to get the data.
    ///
    /// If the iterator has been consumed partly, only the remaining rows are collected
    /// and returned into the vector.
    /// Consequently, if the statement has been fetched fully, an empty vector is returned.
    fn fetch_all(self) -> Vec<Row>;

    /// Returns the number of columns in the result set
    /// If there is no result set, 0 is returned
    fn column_count(&self) -> usize;
}

impl<T: StatementResult + ?Sized> StatementResult for Box<T> {
    delegate! {
        to (**self) {
            fn fetch_one(&mut self) -> Option<&Row>;
            fn column_count(&self) -> usize;
        }
    }

    fn fetch_all(mut self) -> Vec<Row> {
        let mut result = vec![];
        while let Some(row) = self.fetch_one() {
            result.push(row.clone());
        }

        result
    }
}

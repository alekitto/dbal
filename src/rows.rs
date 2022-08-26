use crate::error::Error;
use crate::{Result, Value};
use fallible_iterator::FallibleIterator;
use std::cmp::Ordering;

pub enum ColumnIndex {
    Name(String),
    Position(usize),
}

impl From<usize> for ColumnIndex {
    fn from(i: usize) -> Self {
        Self::Position(i)
    }
}

impl From<&str> for ColumnIndex {
    fn from(s: &str) -> Self {
        Self::Name(s.to_string())
    }
}

#[derive(Clone, Debug)]
pub struct Row {
    columns: Vec<String>,
    values: Vec<Value>,
}

impl PartialEq for Row {
    fn eq(&self, other: &Self) -> bool {
        self.columns.len() == other.columns.len()
            && self
                .columns
                .iter()
                .enumerate()
                .all(|(index, name)| name == other.columns.get(index).unwrap())
            && self
                .values
                .iter()
                .enumerate()
                .all(|(index, value)| value == other.values.get(index).unwrap())
    }
}

impl Row {
    /// Creates a new row.
    /// Private outside dbal crate.
    pub(crate) fn new(columns: Vec<String>, values: Vec<Value>) -> Self {
        Self { columns, values }
    }

    /// Gets a column by index.
    ///
    /// If an index (string or numeric) is not present, an OutOfBoundsError
    /// error is raised.
    pub fn get<C: Into<ColumnIndex>>(&self, i: C) -> Result<&Value> {
        let i = match i.into() {
            ColumnIndex::Name(name) => {
                let mut result = Err(Error::out_of_bounds(name.clone()));
                for (i, column_name) in self.columns.iter().enumerate() {
                    if Ordering::Equal == name.cmp(column_name) {
                        result = Ok(i);
                    }
                }

                result
            }
            ColumnIndex::Position(index) => Ok(index),
        }?;

        let result = self.values.get(i);
        if let Some(res) = result {
            Ok(res)
        } else {
            Err(Error::out_of_bounds(i))
        }
    }
}

/// Represents a row collection, collected from an executed statements
/// It contains all the raw data from the executed query, being countable
/// and iterable safely
pub trait Rows: FallibleIterator {}

pub(crate) macro rows_impl($t:ty) {
    impl $crate::rows::Rows for $t {}
    impl fallible_iterator::FallibleIterator for $t {
        type Item = Row;
        type Error = crate::error::Error;

        /// Advances the iterator and returns the next value.
        ///
        /// Returns [`None`] when iteration is finished. Individual iterator
        /// implementations may choose to resume iteration, and so calling `next()`
        /// again may or may not eventually start returning [`Some(&Row)`] again at some
        /// point.
        fn next(&mut self) -> std::result::Result<Option<Self::Item>, Self::Error> {
            if self.position >= self.rows.len() {
                return Ok(None);
            }

            let result = self.rows.get(self.position);
            self.position += 1;

            if result.is_none() {
                return Ok(None);
            }

            Ok(Some(result.unwrap().clone()))
        }
    }
}

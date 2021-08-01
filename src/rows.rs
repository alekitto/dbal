use crate::error::Error;
use crate::{Result, Value};
use fallible_iterator::FallibleIterator;
use std::cmp::Ordering;

pub enum ColumnIndex {
    Name(String),
    Position(usize),
}

#[derive(Clone, Debug)]
pub struct Row {
    columns: Vec<String>,
    values: Vec<Value>,
}

impl PartialEq for Row {
    fn eq(&self, other: &Self) -> bool {
        self.columns.len() == other.columns.len()
            && (&self.columns)
                .iter()
                .enumerate()
                .all(|(index, name)| name == other.columns.get(index).unwrap())
            && (&self.values)
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
    pub fn get(&self, i: ColumnIndex) -> Result<&Value> {
        let i = match i {
            ColumnIndex::Name(name) => {
                let mut result = Err(Error::out_of_bounds(name.clone()));
                for (i, column_name) in (&self.columns).iter().enumerate() {
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

use crate::error::Error;
use crate::{Result, Value};
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
                let mut result = Err(Error::out_of_bounds(&name));
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
pub trait Rows: Sized {
    /// Returns the length of number of rows in the rows collection.
    fn len(&self) -> usize;

    /// Whether the rows collection is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns an [`Option`] with a reference to the n-th row if exists
    /// or [`None`] if the index is out-of-bounds.
    fn get(&self, index: usize) -> Option<&Row>;

    fn to_vec(self) -> Vec<Row>;

    fn into_iterator(self) -> RowsIterator<Self> {
        RowsIterator::new(self)
    }
}

pub struct RowsIterator<R: Rows> {
    rows: R,
    length: usize,
    position: usize,
}

impl<R: Rows> RowsIterator<R> {
    fn new(rows: R) -> Self {
        let length = rows.len();

        Self {
            rows,
            length,
            position: 0,
        }
    }

    /// Advances the iterator and returns the next value.
    ///
    /// Returns [`None`] when iteration is finished. Individual iterator
    /// implementations may choose to resume iteration, and so calling `next()`
    /// again may or may not eventually start returning [`Some(&Row)`] again at some
    /// point.
    pub fn next(&mut self) -> Option<&Row> {
        if self.position >= self.length {
            return None;
        }

        let result = self.rows.get(self.position);
        self.position += 1;

        result
    }

    pub fn to_vec(self) -> Vec<Row> {
        self.rows.to_vec()
    }
}

impl<T: Rows> Rows for Box<T> {
    delegate::delegate! {
        to(**self) {
            fn len(&self) -> usize;
            fn is_empty(&self) -> bool;
            fn get(&self, index: usize) -> Option<&Row>;
        }
        to(*self) {
            fn to_vec(self) -> Vec<Row>;
        }
    }

    fn into_iterator(self) -> RowsIterator<Self> {
        RowsIterator::new(self)
    }
}

#[cfg(test)]
mod tests {
    use crate::error::ErrorKind;
    use crate::{Row, Value};
    use tokio_test::assert_err;

    #[test]
    fn test_row_can_be_queried() {
        let row = Row::new(
            vec!["col_1".to_string(), "col_test".to_string()],
            vec![Value::NULL, Value::String("test_value".to_string())],
        );
        assert_eq!(
            row.get("col_1").expect("Failed to retrieve 'col_1' column"),
            &Value::NULL
        );
        assert_eq!(
            row.get(0).expect("Failed to retrieve first column"),
            &Value::NULL
        );
        assert_eq!(
            row.get("col_test")
                .expect("Failed to retrieve 'col_test' column"),
            &Value::String("test_value".to_string())
        );
        assert_eq!(
            row.get(1).expect("Failed to retrieve second column"),
            &Value::String("test_value".to_string())
        );
    }

    #[test]
    fn test_nonexistent_row_name_should_return_an_error() {
        let row = Row::new(
            vec!["col_1".to_string(), "col_test".to_string()],
            vec![Value::NULL, Value::String("test_value".to_string())],
        );
        assert_err!(row.get("col_non_existent"));
        let e = assert_err!(row.get(42));
        assert_eq!(e.kind(), ErrorKind::OutOfBoundsError);
    }

    #[test]
    fn test_rows_are_comparable_with_eq() {
        let row = Row::new(
            vec!["col_1".to_string(), "col_test".to_string()],
            vec![Value::NULL, Value::String("test_value".to_string())],
        );
        let row_2 = Row::new(
            vec!["col_1".to_string(), "col_test".to_string()],
            vec![Value::NULL, Value::String("test_value".to_string())],
        );
        assert_eq!(row, row_2);
    }
}

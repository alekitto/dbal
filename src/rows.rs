use crate::error::Error;
use crate::{Result, Value};
use futures::{Stream, TryStreamExt};
use std::cmp::Ordering;
use std::future::Future;
use std::pin::Pin;

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

pub struct Rows {
    columns: Vec<String>,
    length: usize,
    last_insert_id: Option<String>,
    iterator: Pin<Box<dyn Stream<Item = Result<Row>> + Send>>,
}

/// Represents a row collection, collected from an executed statements
/// It contains all the raw data from the executed query, being countable
/// and iterable safely
impl Rows {
    pub fn new(
        columns: Vec<String>,
        length: usize,
        last_insert_id: Option<String>,
        iterator: Pin<Box<dyn Stream<Item = Result<Row>> + Send>>,
    ) -> Self {
        Self {
            columns,
            length,
            last_insert_id,
            iterator,
        }
    }

    /// Returns the length of number of rows in the rows collection.
    pub fn len(&self) -> usize {
        self.length
    }

    /// Whether the rows collection is empty.
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    pub fn into_iterator(self) -> RowsIterator {
        RowsIterator::new(self)
    }

    pub fn last_insert_id(&self) -> &Option<String> {
        &self.last_insert_id
    }
}

pub struct RowsIterator {
    rows: Rows,
    length: usize,
}

impl RowsIterator {
    fn new(rows: Rows) -> Self {
        let length = rows.len();

        Self { rows, length }
    }

    /// Advances the iterator and returns the next value.
    ///
    /// Returns [`None`] when iteration is finished. Individual iterator
    /// implementations may choose to resume iteration, and so calling `next()`
    /// again may or may not eventually start returning [`Some(&Row)`] again at some
    /// point.
    pub fn next(&mut self) -> impl Future<Output = Result<Option<Row>>> + '_ {
        self.rows.iterator.try_next()
    }

    pub async fn to_vec(self) -> Result<Vec<Row>> {
        self.rows.iterator.try_collect().await
    }

    /// Returns the length of number of rows in the rows collection.
    pub fn len(&self) -> usize {
        self.length
    }

    /// Whether the rows collection is empty.
    pub fn is_empty(&self) -> bool {
        self.length == 0
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

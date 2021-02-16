use super::statement::Statement;
use crate::{Row, Value};
use fallible_iterator::FallibleIterator;
use rusqlite::types::ValueRef;
use rusqlite::Column;

pub struct Rows<'r> {
    columns: Vec<String>,
    column_count: usize,
    rusqlite_rows: rusqlite::Rows<'r>,
}

impl<'r> Rows<'r> {
    pub fn new<'conn>(statement: &'r mut Statement<'conn>) -> Rows<'r> {
        let rows = statement.statement.raw_query();
        let columns: Vec<String> = rows
            .columns()
            .unwrap_or(vec![])
            .into_iter()
            .map(|x: Column| x.name().to_string())
            .collect();
        let column_count = rows.column_count().unwrap_or(0);

        Rows {
            columns,
            column_count,
            rusqlite_rows: rows,
        }
    }
}

impl<'r> FallibleIterator for Rows<'r> {
    type Item = Row;
    type Error = crate::error::Error;

    /// Advances the iterator and returns the next value.
    ///
    /// Returns [`None`] when iteration is finished. Individual iterator
    /// implementations may choose to resume iteration, and so calling `next()`
    /// again may or may not eventually start returning [`Some(&Row)`] again at some
    /// point.
    fn next(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        let result = self.rusqlite_rows.next()?;
        if result.is_none() {
            return Ok(None);
        }

        let row = result.unwrap();
        let mut data_vector: Vec<Value> = Vec::new();
        for i in 0..self.column_count {
            let value = row.get_raw(i);
            data_vector.push(match value {
                ValueRef::Null => Value::NULL,
                ValueRef::Integer(v) => Value::Int(v),
                ValueRef::Real(v) => Value::Float(v),
                ValueRef::Text(v) => Value::String(String::from_utf8(v.to_vec())?),
                ValueRef::Blob(v) => Value::Bytes(v.to_vec()),
            });
        }

        Ok(Some(Row::new(self.columns.clone(), data_vector)))
    }
}

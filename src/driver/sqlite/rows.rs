use super::statement::Statement;
use crate::{Result, Row, Value};
use fallible_iterator::FallibleIterator;
use rusqlite::types::ValueRef;
use rusqlite::Column;

pub struct Rows {
    columns: Vec<String>,
    column_count: usize,
    rows: Vec<Row>,

    position: usize,
}

impl Rows {
    pub(super) fn new(statement: &Statement) -> Result<Rows> {
        let mut statement = statement.statement.borrow_mut();
        let mut rows = statement.raw_query();

        let column_count = rows.column_count().unwrap_or(0);
        let columns: Vec<String> = rows
            .columns()
            .unwrap_or_default()
            .into_iter()
            .map(|x: Column| x.name().to_string())
            .collect();

        let mut result = Vec::new();
        while let Some(row) = rows.next()? {
            let mut data_vector: Vec<Value> = Vec::new();
            for i in 0..column_count {
                let value = row.get_raw(i);
                data_vector.push(match value {
                    ValueRef::Null => Value::NULL,
                    ValueRef::Integer(v) => Value::Int(v),
                    ValueRef::Real(v) => Value::Float(v),
                    ValueRef::Text(v) => Value::String(String::from_utf8(v.to_vec())?),
                    ValueRef::Blob(v) => Value::Bytes(v.to_vec()),
                });
            }

            result.push(Row::new(columns.clone(), data_vector));
        }

        Ok(Rows {
            columns,
            column_count,
            rows: result,
            position: 0,
        })
    }

    pub fn columns(&self) -> Vec<&str> {
        self.columns.iter().map(|n| n.as_str()).collect()
    }

    pub fn column_count(&self) -> usize {
        self.column_count
    }
}

impl FallibleIterator for Rows {
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

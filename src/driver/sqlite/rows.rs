use super::statement::Statement;
use crate::{Result, Row, Value};
use futures::Stream;
use rusqlite::Column;
use rusqlite::types::ValueRef;
use std::pin::Pin;
use std::task::{Context, Poll};

pub(super) struct SqliteRowsIterator {
    columns: Vec<String>,
    length: usize,
    iterator: Box<dyn Iterator<Item = Row> + Send + Sync>,
}

impl SqliteRowsIterator {
    pub(super) fn new(statement: &Statement) -> Result<Self> {
        let mut statement = statement.statement.lock().unwrap();

        let column_count = statement.0.column_count();
        let columns: Vec<String> = statement
            .0
            .columns()
            .into_iter()
            .map(|x: Column| x.name().to_string())
            .collect();

        let mut rows = statement.0.raw_query();
        let mut result = Vec::new();
        while let Some(row) = rows.next()? {
            let mut data_vector: Vec<Value> = Vec::new();
            for i in 0..column_count {
                let value = row.get_ref_unwrap(i);
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

        Ok(Self {
            columns,
            length: result.len(),
            iterator: Box::new(result.into_iter()),
        })
    }

    pub fn columns(&self) -> &Vec<String> {
        &self.columns
    }

    pub fn len(&self) -> usize {
        self.length
    }
}

impl Stream for SqliteRowsIterator {
    type Item = Result<Row>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let next = self.iterator.next();
        if let Some(row) = next {
            Poll::Ready(Some(Ok(row)))
        } else {
            Poll::Ready(None)
        }
    }
}

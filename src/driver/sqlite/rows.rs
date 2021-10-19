use super::statement::Statement;
use crate::{rows::rows_impl, Result, Row, Value};
use rusqlite::types::ValueRef;
use rusqlite::Column;

pub struct Rows {
    columns: Vec<String>,
    column_count: usize,

    pub(crate) rows: Vec<Row>,
    pub(crate) position: usize,
}

rows_impl!(Rows);
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

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn columns(&self) -> Vec<&str> {
        self.columns.iter().map(|n| n.as_str()).collect()
    }

    pub fn column_count(&self) -> usize {
        self.column_count
    }
}

use crate::{Result, Row, Value};
use fallible_iterator::FallibleIterator;
use futures::TryStreamExt;
use std::error::Error;
use std::io::Read;
use tokio_postgres::types::{FromSql, Type};
use tokio_postgres::RowStream;

pub struct Rows {
    columns: Vec<String>,
    column_count: usize,
    rows: Vec<Row>,

    position: usize,
}

impl<'a> FromSql<'a> for Value {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> std::prelude::rust_2015::Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(match ty.clone() {
            Type::CHAR => Value::Int(<i8 as FromSql>::from_sql(ty, raw).unwrap() as i64),
            Type::INT2 => Value::Int(<i16 as FromSql>::from_sql(ty, raw).unwrap() as i64),
            Type::INT4 => Value::Int(<i32 as FromSql>::from_sql(ty, raw).unwrap() as i64),
            Type::INT8 => Value::Int(<i64 as FromSql>::from_sql(ty, raw).unwrap()),
            Type::FLOAT4 => Value::Float(<f32 as FromSql>::from_sql(ty, raw).unwrap() as f64),
            Type::FLOAT8 => Value::Float(<f64 as FromSql>::from_sql(ty, raw).unwrap()),
            Type::CSTRING | Type::VARCHAR => {
                let mut s = String::new();
                let vv = Vec::from(raw);
                vv.as_slice().read_to_string(&mut s)?;

                Value::String(s)
            }
            _ => Value::Bytes(raw.to_vec()),
        })
    }

    fn from_sql_null(
        _: &Type,
    ) -> std::prelude::rust_2015::Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(Value::NULL)
    }

    fn accepts(_: &Type) -> bool {
        true
    }
}

impl Rows {
    pub(super) async fn new(row_stream: RowStream) -> Result<Rows> {
        let mut result = Vec::new();
        let mut columns = Option::None;

        let rows: Vec<tokio_postgres::Row> = row_stream.try_collect().await?;
        for row in rows {
            let mut data_vector: Vec<Value> = Vec::new();
            for i in 0..row.len() {
                if columns.is_none() {
                    let mut c = vec![];
                    for column in row.columns() {
                        c.push(column.name().to_string());
                    }

                    let _ = columns.insert(c);
                }

                let value: Value = row.get(i);
                data_vector.push(value);
            }

            result.push(Row::new(columns.as_ref().unwrap().clone(), data_vector));
        }

        let columns = columns.unwrap_or_else(Vec::new);
        let column_count = columns.len();

        Ok(Self {
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

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
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

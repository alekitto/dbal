use crate::{Result, Row, Value};
use futures::Stream;
use mysql_async::prelude::{ConvIr, FromValue};
use mysql_async::{BinaryProtocol, FromValueError, QueryResult};
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct IrValue {
    value: mysql_async::Value,
    output: Value,
}

impl ConvIr<Value> for IrValue {
    fn new(v: mysql_async::Value) -> core::result::Result<Self, FromValueError> {
        let output = match &v {
            mysql_async::Value::NULL => Value::NULL,
            mysql_async::Value::Bytes(bytes) => {
                if let Ok(str) = String::from_utf8(bytes.clone()) {
                    Value::String(str)
                } else {
                    Value::Bytes(bytes.clone())
                }
            }
            mysql_async::Value::Int(i) => Value::Int(*i),
            mysql_async::Value::UInt(u) => Value::UInt(*u),
            mysql_async::Value::Float(f) => Value::Float(*f as f64),
            mysql_async::Value::Double(f) => Value::Float(*f),
            mysql_async::Value::Date(y, m, d, h, i, s, ms) => Value::String(format!(
                "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:06}",
                y, m, d, h, i, s, ms
            )),
            mysql_async::Value::Time(neg, d, h, i, s, ms) => Value::String(format!(
                "{}{:02}:{:02}:{:02}.{:06}",
                if *neg { "-" } else { "" },
                (d * 24) + (*h as u32),
                i,
                s,
                ms
            )),
        };

        Ok(Self { value: v, output })
    }

    fn commit(self) -> Value {
        self.output
    }

    fn rollback(self) -> mysql_async::Value {
        self.value
    }
}

impl FromValue for Value {
    type Intermediate = IrValue;
}

pub struct MySQLRowsIterator {
    length: usize,
    columns: Vec<String>,
    iter: Box<dyn Iterator<Item = mysql_async::Row> + Send>,
}

impl MySQLRowsIterator {
    pub async fn new(rows: QueryResult<'_, '_, BinaryProtocol>) -> Result<MySQLRowsIterator> {
        let columns = if let Some(cols) = rows.columns() {
            cols.iter().map(|col| col.name_str().to_string()).collect()
        } else {
            vec![]
        };

        let stream = rows.collect_and_drop().await?;
        let length = stream.len();

        Ok(Self {
            length,
            columns,
            iter: Box::new(stream.into_iter()),
        })
    }

    pub fn len(&self) -> usize {
        self.length
    }
}

impl Stream for MySQLRowsIterator {
    type Item = Result<Row>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(if let Some(row) = self.iter.next() {
            let mut data_vector: Vec<Value> = Vec::new();
            for i in 0..row.len() {
                let value: Value = row.get(i).unwrap();
                data_vector.push(value);
            }

            Some(Ok(Row::new(self.columns.clone(), data_vector)))
        } else {
            None
        })
    }
}

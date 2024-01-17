use crate::{Result, Row, Value};
use futures::Stream;
use mysql_async::prelude::FromValue;
use mysql_async::{BinaryProtocol, FromValueError, QueryResult};
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct IrValue {
    output: Value,
}

impl TryFrom<mysql_async::Value> for IrValue {
    type Error = FromValueError;

    fn try_from(v: mysql_async::Value) -> core::result::Result<Self, FromValueError> {
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
            mysql_async::Value::Date(y, m, d, h, i, s, ms) => {
                let Some(date) = chrono::NaiveDate::from_ymd_opt(*y as i32, *m as u32, *d as u32)
                else {
                    return Err(FromValueError(v));
                };
                let Some(time) =
                    chrono::NaiveTime::from_hms_milli_opt(*h as u32, *i as u32, *s as u32, *ms)
                else {
                    return Err(FromValueError(v));
                };

                let Some(dt) = chrono::NaiveDateTime::new(date, time)
                    .and_local_timezone(chrono::Local)
                    .earliest()
                else {
                    return Err(FromValueError(v));
                };

                Value::DateTime(dt)
            }
            mysql_async::Value::Time(neg, d, h, i, s, ms) => Value::String(format!(
                "{}{:02}:{:02}:{:02}.{:06}",
                if *neg { "-" } else { "" },
                (d * 24) + (*h as u32),
                i,
                s,
                ms
            )),
        };

        Ok(Self { output })
    }
}

impl From<IrValue> for Value {
    fn from(value: IrValue) -> Self {
        value.output
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

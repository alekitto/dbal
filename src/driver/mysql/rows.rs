use crate::{rows::rows_impl, Result, Row, Value};
use mysql_async::prelude::{ConvIr, FromValue};
use mysql_async::{BinaryProtocol, FromValueError, QueryResult};

pub struct Rows {
    columns: Vec<String>,
    column_count: usize,

    pub(crate) rows: Vec<Row>,
    pub(crate) position: usize,
}

pub struct IrValue {
    value: mysql_async::Value,
    output: Value,
}

impl ConvIr<Value> for IrValue {
    fn new(v: mysql_async::Value) -> std::prelude::rust_2015::Result<Self, FromValueError> {
        let output = match &v {
            mysql_async::Value::NULL => Value::NULL,
            mysql_async::Value::Bytes(bytes) => Value::Bytes(bytes.clone()),
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

rows_impl!(Rows);
impl Rows {
    pub(super) async fn new(rows: QueryResult<'_, '_, BinaryProtocol>) -> Result<Rows> {
        let mut result = Vec::new();
        let columns = rows
            .columns()
            .map(|cols| cols.iter().map(|col| col.name_str().to_string()).collect())
            .unwrap_or_else(Vec::new);

        rows.for_each_and_drop(|row| {
            let mut data_vector: Vec<Value> = Vec::new();
            for i in 0..row.len() {
                let value: Value = row.get(i).unwrap();
                data_vector.push(value);
            }

            result.push(Row::new(columns.clone(), data_vector));
        })
        .await?;

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

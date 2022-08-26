use crate::{rows, Result, Row, Value};
use futures::TryStreamExt;
use std::error::Error;
use std::io::Read;
use tokio_postgres::types::{FromSql, Kind, Type};
use tokio_postgres::RowStream;

pub struct Rows {
    columns: Vec<String>,
    column_count: usize,
    rows: Vec<Row>,
}

fn simple_type_from_sql(
    ty: &Type,
    raw: &[u8],
) -> core::result::Result<Value, Box<dyn Error + Sync + Send>> {
    Ok(match *ty {
        Type::CHAR => Value::Int(<i8 as FromSql>::from_sql(ty, raw)? as i64),
        Type::INT2 => Value::Int(<i16 as FromSql>::from_sql(ty, raw)? as i64),
        Type::INT4 => Value::Int(<i32 as FromSql>::from_sql(ty, raw)? as i64),
        Type::INT8 => Value::Int(<i64 as FromSql>::from_sql(ty, raw)?),
        Type::FLOAT4 => Value::Float(<f32 as FromSql>::from_sql(ty, raw)? as f64),
        Type::FLOAT8 => Value::Float(<f64 as FromSql>::from_sql(ty, raw)?),
        Type::BOOL => Value::Boolean(<bool as FromSql>::from_sql(ty, raw)?),
        Type::JSON | Type::JSONB => {
            todo!();
        }
        Type::CSTRING
        | Type::NAME
        | Type::VARCHAR
        | Type::DATE
        | Type::TIME
        | Type::TIMETZ
        | Type::TIMESTAMP
        | Type::TIMESTAMPTZ
        | Type::INET
        | Type::TEXT
        | Type::UUID
        | Type::XML
        | Type::REGCLASS
        | Type::REGPROC
        | Type::REGROLE
        | Type::REGPROCEDURE
        | Type::REGOPERATOR
        | Type::REGOPER
        | Type::REGCOLLATION
        | Type::REGCONFIG
        | Type::REGNAMESPACE
        | Type::REGTYPE => {
            let mut s = String::new();
            let vv = Vec::from(raw);
            vv.as_slice().read_to_string(&mut s)?;

            Value::String(s)
        }
        _ => Value::Bytes(raw.to_vec()),
    })
}

impl<'a> FromSql<'a> for Value {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> core::result::Result<Self, Box<dyn Error + Sync + Send>> {
        let item = match ty.kind() {
            Kind::Simple => simple_type_from_sql(ty, raw)?,
            _ => {
                println!("{:?}", ty);
                todo!()
            }
        };

        Ok(item)
    }

    fn from_sql_null(_: &Type) -> core::result::Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(Value::NULL)
    }

    fn accepts(_: &Type) -> bool {
        true
    }
}

impl rows::Rows for Rows {
    fn len(&self) -> usize {
        self.rows.len()
    }

    fn get(&self, index: usize) -> Option<&Row> {
        self.rows.get(index)
    }

    fn to_vec(self) -> Vec<Row> {
        self.rows
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

        let columns = columns.unwrap_or_default();
        let column_count = columns.len();

        Ok(Self {
            columns,
            column_count,
            rows: result,
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

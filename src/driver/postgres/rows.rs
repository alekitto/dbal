use crate::{Error, Result, Row, Value};
use fallible_iterator::FallibleIterator;
use futures::{Stream, StreamExt};
use postgres_protocol::types;
use std::io::Read;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio_postgres::types::{FromSql, Kind, Type};
use tokio_postgres::{RowStream, Statement};

fn simple_type_from_sql(
    ty: &Type,
    raw: &[u8],
) -> core::result::Result<Value, Box<dyn std::error::Error + Sync + Send>> {
    Ok(match *ty {
        Type::CHAR => Value::Int(<i8 as FromSql>::from_sql(ty, raw)? as i64),
        Type::INT2 => Value::Int(<i16 as FromSql>::from_sql(ty, raw)? as i64),
        Type::INT4 => Value::Int(<i32 as FromSql>::from_sql(ty, raw)? as i64),
        Type::INT8 => Value::Int(<i64 as FromSql>::from_sql(ty, raw)?),
        Type::FLOAT4 => Value::Float(<f32 as FromSql>::from_sql(ty, raw)? as f64),
        Type::FLOAT8 => Value::Float(<f64 as FromSql>::from_sql(ty, raw)?),
        Type::BOOL => Value::Boolean(<bool as FromSql>::from_sql(ty, raw)?),
        Type::JSON | Type::JSONB => {
            let mut s = String::new();
            let vv = Vec::from(raw);
            vv.as_slice().read_to_string(&mut s)?;

            let json = serde_json::from_str(&s)?;
            Value::Json(json)
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
    ) -> core::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let item = match ty.kind() {
            Kind::Simple => simple_type_from_sql(ty, raw)?,
            Kind::Array(ty) => {
                let array = types::array_from_sql(raw)?;
                if array.dimensions().count()? > 1 {
                    return Err("array contains too many dimensions".into());
                }

                let out = array
                    .values()
                    .map(|val| {
                        if let Some(val) = val {
                            simple_type_from_sql(ty, val)
                        } else {
                            Ok(Value::NULL)
                        }
                    })
                    .collect::<Vec<_>>()?;

                Value::Array(out)
            }
            _ => {
                println!("{:?}", ty);
                todo!()
            }
        };

        Ok(item)
    }

    fn from_sql_null(
        _: &Type,
    ) -> core::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Value::NULL)
    }

    fn accepts(_: &Type) -> bool {
        true
    }
}

pub struct PostgreSQLRowsIterator {
    row_stream: Pin<Box<RowStream>>,
    columns: Vec<String>,
}

impl PostgreSQLRowsIterator {
    pub fn new(row_stream: RowStream, statement: &Statement) -> Result<Self> {
        let mut columns = vec![];
        for column in statement.columns() {
            columns.push(column.name().to_string());
        }

        let row_stream = Box::pin(row_stream);
        Ok(Self {
            row_stream,
            columns,
        })
    }

    pub fn columns(&self) -> &Vec<String> {
        &self.columns
    }

    fn psql_row_to_row(&self, psql_row: tokio_postgres::Row) -> Row {
        let mut data_vector: Vec<Value> = Vec::new();
        for i in 0..psql_row.len() {
            let value: Value = psql_row.get(i);
            data_vector.push(value);
        }

        Row::new(self.columns.clone(), data_vector)
    }
}

impl Stream for PostgreSQLRowsIterator {
    type Item = Result<Row>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.row_stream.poll_next_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(result)) => Poll::Ready(Some(match result {
                Ok(row) => Ok(self.psql_row_to_row(row)),
                Err(e) => Err(Error::from(e)),
            })),
        }
    }
}

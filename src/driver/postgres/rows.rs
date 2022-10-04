use crate::{Error, Result, Row, Value};
use futures::{Stream, StreamExt};
use std::io::Read;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio_postgres::types::{FromSql, Kind, Type};
use tokio_postgres::RowStream;

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
    ) -> core::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let item = match ty.kind() {
            Kind::Simple => simple_type_from_sql(ty, raw)?,
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
    first_row: Option<tokio_postgres::Row>,
    columns: Vec<String>,
}

impl PostgreSQLRowsIterator {
    pub async fn new(row_stream: RowStream) -> Result<Self> {
        let mut row_stream = Box::pin(row_stream);
        let first_row = row_stream.next().await;
        let first_row = if let Some(result) = first_row {
            Some(result?)
        } else {
            None
        };

        let columns = if let Some(row) = &first_row {
            let mut c = vec![];
            for column in row.columns() {
                c.push(column.name().to_string());
            }

            c
        } else {
            vec![]
        };

        Ok(Self {
            row_stream,
            first_row,
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
        if let Some(first_row) = self.first_row.take() {
            return Poll::Ready(Some(Ok(self.psql_row_to_row(first_row))));
        }

        match Pin::new(&mut self.row_stream).poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(result)) => Poll::Ready(Some(match result {
                Ok(row) => Ok(self.psql_row_to_row(row)),
                Err(e) => Err(Error::from(e)),
            })),
        }
    }
}

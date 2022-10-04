use crate::driver::statement::Statement;
use crate::driver::statement_result::StatementResult;
use crate::platform::DatabasePlatform;
use crate::{AsyncResult, ConnectionOptions, Error, EventDispatcher, Parameters, Result};
use connection::{Connection, DriverConnection};
use std::fmt::Debug;
use std::sync::Arc;

pub mod connection;
pub mod statement;
pub mod statement_result;

#[cfg(feature = "mysql")]
pub mod mysql;

#[cfg(feature = "postgres")]
pub mod postgres;

#[cfg(feature = "sqlite")]
pub mod sqlite;

#[derive(Debug)]
pub struct Driver {
    inner_driver: Box<dyn for<'a> Connection<'a>>,
}

impl Driver {
    pub fn create_with_connection(connection: Box<dyn for<'a> Connection<'a>>) -> Self {
        Self {
            inner_driver: connection,
        }
    }

    pub async fn create(connection_options: &ConnectionOptions) -> Result<Self> {
        let driver = match connection_options.scheme.as_ref().unwrap().as_str() {
            #[cfg(feature = "mysql")]
            "mysql" => Box::new(mysql::driver::Driver::create(connection_options.into()).await?)
                as Box<dyn for<'a> Connection<'a>>,
            #[cfg(feature = "postgres")]
            "psql" | "postgres" | "postgresql" => {
                Box::new(postgres::driver::Driver::create(connection_options.into()).await?)
                    as Box<dyn for<'a> Connection<'a>>
            }
            #[cfg(feature = "sqlite")]
            "sqlite" => Box::new(sqlite::driver::Driver::create(connection_options.into()).await?)
                as Box<dyn for<'a> Connection<'a>>,
            proto => return Err(Error::unknown_driver(proto)),
        };

        Ok(Self {
            inner_driver: driver,
        })
    }

    pub async fn create_platform(
        &self,
        ev: Arc<EventDispatcher>,
    ) -> Box<dyn DatabasePlatform + Send + Sync> {
        self.inner_driver.create_platform(ev).await
    }

    pub fn prepare<St: Into<String>>(&self, sql: St) -> Result<Box<dyn Statement<'_> + '_>> {
        self.inner_driver.prepare(sql.into().as_str())
    }

    /// Executes an SQL statement, returning a result set as a Statement object.
    pub fn query<St: Into<String>>(
        &self,
        sql: St,
        params: Parameters<'_>,
    ) -> AsyncResult<StatementResult> {
        let params = Vec::from(params);
        let prepared = self.prepare(sql);

        Box::pin(async move {
            if prepared.is_err() {
                return Err(prepared.err().unwrap());
            }

            let prepared = prepared.unwrap();
            let query_result = prepared.query(Parameters::Vec(params)).await;
            if query_result.is_err() {
                return Err(query_result.err().unwrap());
            }

            Ok(query_result.unwrap())
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::driver::Driver;
    use crate::{params, ConnectionOptions};

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    async fn can_create_connection() {
        let options =
            ConnectionOptions::try_from(std::env::var("DATABASE_DSN").unwrap().as_ref()).unwrap();
        let connection = Driver::create(&options).await.expect("Must be connected");

        let statement = connection.prepare("SELECT 1").expect("Prepare failed");
        let result = statement.execute(params![]).await;
        assert_eq!(result.is_ok(), true);
    }
}

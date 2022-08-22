use crate::driver::statement::Statement;
use crate::driver::statement_result::StatementResult;
use crate::platform::DatabasePlatform;
use crate::{AsyncResult, ConnectionOptions, EventDispatcher, Parameters, Result};
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
pub enum Driver {
    #[cfg(feature = "mysql")]
    MySQL(mysql::driver::Driver),
    #[cfg(feature = "postgres")]
    Postgres(postgres::driver::Driver),
    #[cfg(feature = "sqlite")]
    Sqlite(sqlite::driver::Driver),
}

impl Driver {
    pub async fn create(connection_options: &ConnectionOptions) -> Result<Self> {
        let driver = match connection_options.scheme.as_ref().unwrap().as_str() {
            #[cfg(feature = "mysql")]
            "mysql" => {
                Driver::MySQL(mysql::driver::Driver::create(connection_options.into()).await?)
            }
            #[cfg(feature = "postgres")]
            "psql" => {
                Driver::Postgres(postgres::driver::Driver::create(connection_options.into()).await?)
            }
            #[cfg(feature = "sqlite")]
            "sqlite" => {
                Driver::Sqlite(sqlite::driver::Driver::create(connection_options.into()).await?)
            }
            _ => unimplemented!(),
        };

        Ok(driver)
    }

    pub async fn create_platform(
        &self,
        ev: Arc<EventDispatcher>,
    ) -> Box<dyn DatabasePlatform + Send + Sync> {
        match self {
            #[cfg(feature = "mysql")]
            Self::MySQL(driver) => driver.create_platform(ev),
            #[cfg(feature = "postgres")]
            Self::Postgres(driver) => driver.create_platform(ev),
            #[cfg(feature = "sqlite")]
            Self::Sqlite(driver) => driver.create_platform(ev),
        }
        .await
    }

    pub fn prepare<St: Into<String>>(&self, sql: St) -> Result<Box<dyn Statement<'_> + '_>> {
        let statement = match self {
            #[cfg(feature = "mysql")]
            Self::MySQL(driver) => {
                Box::new(driver.prepare(sql.into().as_str())?) as Box<dyn Statement<'_>>
            }
            #[cfg(feature = "postgres")]
            Self::Postgres(driver) => {
                Box::new(driver.prepare(sql.into().as_str())?) as Box<dyn Statement<'_>>
            }
            #[cfg(feature = "sqlite")]
            Self::Sqlite(driver) => {
                Box::new(driver.prepare(sql.into().as_str())?) as Box<dyn Statement<'_>>
            }
        };

        Ok(statement)
    }

    /// Executes an SQL statement, returning a result set as a Statement object.
    pub fn query<St: Into<String>>(
        &self,
        sql: St,
        params: Parameters<'_>,
    ) -> AsyncResult<Box<dyn StatementResult>> {
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

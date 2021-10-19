use crate::driver::statement::Statement;
use crate::{AsyncResult, Parameters, Result};
use connection::{Connection, DriverConnection};
use std::marker::PhantomData;
use url::Url;

pub mod connection;
pub mod statement;
pub mod statement_result;

#[cfg(feature = "mysql")]
pub mod mysql;

#[cfg(feature = "postgres")]
pub mod postgres;

#[cfg(feature = "sqlite")]
pub mod sqlite;

pub enum Driver {
    #[cfg(feature = "mysql")]
    MySQL(mysql::driver::Driver),
    #[cfg(feature = "postgres")]
    Postgres(postgres::driver::Driver),
    #[cfg(feature = "sqlite")]
    Sqlite(sqlite::driver::Driver),
}

pub enum DriverStatement<'conn> {
    #[cfg(feature = "mysql")]
    MySQL(mysql::statement::Statement<'conn>),
    #[cfg(feature = "postgres")]
    Postgres(postgres::statement::Statement<'conn>),
    #[cfg(feature = "sqlite")]
    Sqlite(sqlite::statement::Statement<'conn>),
    Null(PhantomData<&'conn Self>),
}

impl DriverStatement<'conn> {
    /// Executes an SQL statement, returning a result set as a Statement object.
    pub async fn query(&self, params: Parameters<'conn>) -> Result<DriverStatementResult> {
        Ok(match self {
            #[cfg(feature = "mysql")]
            DriverStatement::MySQL(statement) => {
                DriverStatementResult::MySQL(statement.query(params).await?)
            }
            #[cfg(feature = "postgres")]
            DriverStatement::Postgres(statement) => {
                DriverStatementResult::Postgres(statement.query(params).await?)
            }
            #[cfg(feature = "sqlite")]
            DriverStatement::Sqlite(statement) => {
                DriverStatementResult::Sqlite(statement.query(params).await?)
            }
            DriverStatement::Null(_) => unreachable!(),
        })
    }

    /// Executes an SQL statement, returning the number of affected rows.
    pub async fn execute(&self, params: Parameters<'conn>) -> Result<usize> {
        Ok(match self {
            #[cfg(feature = "mysql")]
            DriverStatement::MySQL(statement) => statement.execute(params).await?,
            #[cfg(feature = "postgres")]
            DriverStatement::Postgres(statement) => statement.execute(params).await?,
            #[cfg(feature = "sqlite")]
            DriverStatement::Sqlite(statement) => statement.execute(params).await?,
            DriverStatement::Null(_) => unreachable!(),
        })
    }
}

pub enum DriverStatementResult {
    #[cfg(feature = "mysql")]
    MySQL(mysql::statement_result::StatementResult),
    #[cfg(feature = "postgres")]
    Postgres(postgres::statement_result::StatementResult),
    #[cfg(feature = "sqlite")]
    Sqlite(sqlite::statement_result::StatementResult),
}

impl Driver {
    pub async fn create<'a, T>(dsn: T) -> Result<Self>
    where
        T: Into<String>,
    {
        let dsn = dsn.into();

        #[cfg(feature = "sqlite")]
        if dsn.starts_with("sqlite:") {
            let driver = sqlite::driver::Driver::create(dsn).await?;
            return Ok(Self::Sqlite(driver));
        }

        let url = Url::parse(dsn.as_str())?;
        let driver = match url.scheme() {
            #[cfg(feature = "mysql")]
            "mysql" | "mariadb" => {
                Driver::MySQL(mysql::driver::Driver::create(&url.to_string()).await?)
            }
            #[cfg(feature = "postgres")]
            "pg" | "psql" | "postgres" | "postgresql" => {
                let connection_options = postgres::driver::ConnectionOptions::build_from_url(&url);
                Driver::Postgres(postgres::driver::Driver::create(connection_options).await?)
            }
            #[cfg(feature = "sqlite")]
            "sqlite" => Driver::Sqlite(sqlite::driver::Driver::create(url.to_string()).await?),
            _ => unimplemented!(),
        };

        Ok(driver)
    }

    pub fn prepare<St: Into<String>>(&self, sql: St) -> Result<DriverStatement<'_>> {
        let statement = match self {
            #[cfg(feature = "mysql")]
            Self::MySQL(driver) => DriverStatement::MySQL(driver.prepare(sql)?),
            #[cfg(feature = "postgres")]
            Self::Postgres(driver) => DriverStatement::Postgres(driver.prepare(sql)?),
            #[cfg(feature = "sqlite")]
            Self::Sqlite(driver) => DriverStatement::Sqlite(driver.prepare(sql)?),
            _ => unreachable!(),
        };

        Ok(statement)
    }

    /// Executes an SQL statement, returning a result set as a Statement object.
    pub fn query<St: Into<String>>(
        &self,
        sql: St,
        params: Parameters<'_>,
    ) -> AsyncResult<DriverStatementResult> {
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
    use crate::params;

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    async fn can_create_connection() {
        let connection = Driver::create(std::env::var("DATABASE_DSN").unwrap())
            .await
            .expect("Must be connected");

        let statement = connection.prepare("SELECT 1").expect("Prepare failed");
        let result = statement.execute(params![]).await;
        assert_eq!(result.is_ok(), true);
    }
}

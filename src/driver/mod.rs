use crate::driver::statement::Statement;
use crate::{AsyncResult, Parameters, Result};
use connection::{Connection, DriverConnection};
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
    #[cfg(feature = "postgres")]
    Postgres(postgres::driver::Driver),
    #[cfg(feature = "sqlite")]
    Sqlite(sqlite::driver::Driver),
}

pub enum DriverStatement<'conn> {
    #[cfg(feature = "postgres")]
    Postgres(postgres::statement::Statement<'conn>),
    #[cfg(feature = "sqlite")]
    Sqlite(sqlite::statement::Statement<'conn>),
}

impl DriverStatement<'conn> {
    /// Executes an SQL statement, returning a result set as a Statement object.
    pub async fn query(&self, params: Parameters<'conn>) -> Result<DriverStatementResult> {
        Ok(match self {
            DriverStatement::Postgres(statement) => {
                DriverStatementResult::Postgres(statement.query(params).await?)
            }
            DriverStatement::Sqlite(statement) => {
                DriverStatementResult::Sqlite(statement.query(params).await?)
            }
        })
    }

    /// Executes an SQL statement, returning the number of affected rows.
    pub async fn execute(&self, params: Parameters<'conn>) -> Result<usize> {
        Ok(match self {
            DriverStatement::Postgres(statement) => statement.execute(params).await?,
            DriverStatement::Sqlite(statement) => statement.execute(params).await?,
        })
    }
}

pub enum DriverStatementResult {
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
            #[cfg(feature = "postgres")]
            "pg" | "psql" | "postgres" | "postgresql" => {
                let mut username = url.username().to_string();
                if username.is_empty() {
                    username = String::from("postgres");
                }

                let password = url.password().map(String::from);

                let connection_options = postgres::ConnectionOptions {
                    host: url.host().map(|h| h.to_string()),
                    port: url.port(),
                    user: username,
                    password,
                    db_name: {
                        let path = url.path().trim_start_matches('/').to_string();
                        if path.is_empty() {
                            Some(String::from("postgres"))
                        } else {
                            Some(path)
                        }
                    },
                    ssl_mode: postgres::SslMode::None,
                    application_name: {
                        let mut ret = None;
                        for (name, value) in url.query_pairs() {
                            if name == "application_name" {
                                ret = Some(value.to_string());
                                break;
                            }
                        }

                        ret
                    },
                };

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
            #[cfg(feature = "postgres")]
            Self::Postgres(driver) => DriverStatement::Postgres(driver.prepare(sql)?),
            #[cfg(feature = "sqlite")]
            Self::Sqlite(driver) => DriverStatement::Sqlite(driver.prepare(sql)?),
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

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn can_create_sqlite_connection() {
        let connection = Driver::create("sqlite://:memory:")
            .await
            .expect("Must be connected");

        let statement = connection.prepare("SELECT 1").expect("Prepare failed");
        let result = statement.execute(params![]).await;
        assert_eq!(result.is_ok(), true);
    }

    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn can_create_postgres_connection() {
        let postgres_dsn = std::env::var("POSTGRES_DSN")
            .unwrap_or_else(|_| "postgres://localhost/postgres".to_string());

        let connection = Driver::create(postgres_dsn)
            .await
            .expect("Must be connected");

        let statement = connection.prepare("SELECT 1").expect("Prepare failed");
        let result = statement.execute(params![]).await;
        assert_eq!(result.is_ok(), true);
    }
}

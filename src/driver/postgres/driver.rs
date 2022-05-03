use crate::driver::connection::{Connection, DriverConnection};
use crate::{Async, Result};
use regex::Regex;
use std::fmt::{Debug, Formatter};
use std::future::Future;
use tokio::task::JoinHandle;
use tokio_postgres::tls::{MakeTlsConnect, TlsStream};
use tokio_postgres::{Client, GenericClient, NoTls, Socket};
use url::Url;

pub enum SslMode {
    None,
}

pub struct ConnectionOptions {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub user: String,
    pub password: Option<String>,
    pub db_name: Option<String>,
    pub ssl_mode: SslMode,
    pub application_name: Option<String>,
}

impl ConnectionOptions {
    pub(crate) fn build_from_url(url: &Url) -> Self {
        let mut username = url.username().to_string();
        if username.is_empty() {
            username = String::from("postgres");
        }

        let password = url.password().map(String::from);

        Self {
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
            ssl_mode: SslMode::None,
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
        }
    }
}

pub struct Driver {
    pub(super) client: Client,
    handle: JoinHandle<()>,
}

impl Debug for Driver {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt("Driver (PostgreSQL) {}", f)
    }
}

impl Driver {
    fn build_dsn(
        options: ConnectionOptions,
    ) -> (
        String,
        impl MakeTlsConnect<Socket, Stream = impl TlsStream + Unpin + Send>,
    ) {
        let mut dsn = String::new();
        if let Some(host) = options.host {
            if !host.is_empty() {
                dsn += &format!("host={} ", host);
            }
        }

        if let Some(port) = options.port {
            dsn += &format!("port={} ", port);
        }

        dsn += &format!("user={} ", options.user);
        if let Some(password) = options.password {
            dsn += &format!("password={} ", password);
        }

        let db_name = options.db_name.unwrap_or_else(|| "postgres".to_string());
        dsn += &format!("dbname={} ", db_name);

        // TODO ssl

        if let Some(application_name) = options.application_name {
            if !application_name.is_empty() {
                dsn += &format!("application_name={} ", application_name);
            }
        }

        (dsn.trim().to_string(), NoTls)
    }
}

impl DriverConnection<ConnectionOptions> for Driver {
    type Output = impl Future<Output = Result<Self>>;

    fn create(params: ConnectionOptions) -> Self::Output {
        let (config, tls) = Self::build_dsn(params);

        async move {
            // Connect to the database.
            let (client, connection) = tokio_postgres::connect(&config, tls).await?;
            let handle = tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("connection error: {}", e);
                }
            });

            Ok(Self { client, handle })
        }
    }
}

impl Drop for Driver {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl<'conn> Connection<'conn> for Driver {
    type Statement = super::statement::Statement<'conn>;

    fn prepare<St: Into<String>>(&'conn self, sql: St) -> Result<Self::Statement> {
        let statement = super::statement::Statement::new(self, sql.into().as_str())?;

        Ok(statement)
    }

    fn server_version(&self) -> Async<Option<String>> {
        Box::pin(async move {
            let row = self
                .client
                .client()
                .query_one("SELECT version()", &[])
                .await;
            if row.is_err() {
                return None;
            }

            let row = row.unwrap();
            let version_string: String = row.get(0);

            let pattern = Regex::new(r"\w+ (\d+)\.(\d+)").unwrap();
            pattern.captures(&version_string).map(|captures| {
                format!(
                    "{}.{}",
                    captures.get(0).unwrap().as_str(),
                    captures.get(1).unwrap().as_str()
                )
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::driver::connection::{Connection, DriverConnection};
    use crate::driver::postgres::driver::Driver;
    use crate::driver::postgres::ConnectionOptions;
    use crate::driver::statement_result::StatementResult;
    use crate::rows::ColumnIndex;
    use crate::{params, Row, Value};
    use url::Url;

    #[tokio::test]
    async fn can_connect() {
        let result = Driver::create(ConnectionOptions::build_from_url(
            &Url::parse(&std::env::var("DATABASE_DSN").unwrap()).unwrap(),
        ))
        .await;
        assert_eq!(true, result.is_ok());
    }

    #[tokio::test]
    async fn can_prepare_statements() {
        let connection = Driver::create(ConnectionOptions::build_from_url(
            &Url::parse(&std::env::var("DATABASE_DSN").unwrap()).unwrap(),
        ))
        .await
        .expect("Must be connected");

        let statement = connection.prepare("SELECT 1");
        assert_eq!(statement.is_ok(), true);
        let statement = connection.prepare("NOT_A_COMMAND 1");
        assert_eq!(statement.is_ok(), true);
    }

    #[tokio::test]
    async fn can_fetch_rows() {
        let connection = Driver::create(ConnectionOptions::build_from_url(
            &Url::parse(&std::env::var("DATABASE_DSN").unwrap()).unwrap(),
        ))
        .await
        .expect("Must be connected");

        let statement = connection.query("SELECT 1 + 1", params![]).await;
        assert_eq!(statement.is_ok(), true);
        let statement = statement.unwrap();
        let row: Row = statement.fetch_one().unwrap().unwrap();

        assert_eq!(row.get(ColumnIndex::Position(0)).unwrap(), &Value::Int(2));
    }
}

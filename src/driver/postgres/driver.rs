use crate::connection_options::SslMode;
use crate::driver::connection::{Connection, DriverConnection};
use crate::driver::postgres::platform::PostgreSQLPlatform;
use crate::driver::statement::Statement;
use crate::platform::DatabasePlatform;
use crate::sync::JoinHandle;
use crate::{Async, EventDispatcher, Result};
use regex::Regex;
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::sync::Arc;
use tokio_postgres::tls::{MakeTlsConnect, TlsStream};
use tokio_postgres::{Client, GenericClient, NoTls, Socket};
use url::Url;

pub struct ConnectionOptions {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub user: String,
    pub password: Option<String>,
    pub db_name: Option<String>,
    pub ssl_mode: SslMode,
    pub application_name: Option<String>,
}

impl From<&crate::ConnectionOptions> for ConnectionOptions {
    fn from(opts: &crate::ConnectionOptions) -> Self {
        Self {
            host: opts.host.as_ref().cloned(),
            port: opts.port.as_ref().copied(),
            user: opts
                .username
                .as_ref()
                .cloned()
                .unwrap_or_else(|| "postgres".to_string()),
            password: opts.password.as_ref().cloned(),
            db_name: opts.database_name.as_ref().cloned(),
            ssl_mode: opts.ssl_mode,
            application_name: opts.application_name.as_ref().cloned(),
        }
    }
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
        f.debug_struct("Driver (PostgreSQL)")
            .finish_non_exhaustive()
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
            let handle = crate::sync::spawn(async move {
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
    fn create_platform(
        &self,
        ev: Arc<EventDispatcher>,
    ) -> Async<Box<dyn DatabasePlatform + Send + Sync>> {
        Box::pin(async move {
            Box::new(PostgreSQLPlatform::new(ev)) as Box<(dyn DatabasePlatform + Send + Sync)>
        })
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

    fn prepare(&'conn self, sql: &str) -> Result<Box<dyn Statement + 'conn>> {
        Ok(Box::new(super::statement::Statement::new(self, sql)))
    }
}

#[cfg(test)]
mod tests {
    use crate::driver::connection::{Connection, DriverConnection};
    use crate::driver::postgres::driver::Driver;
    use crate::driver::postgres::ConnectionOptions;
    use crate::rows::ColumnIndex;
    use crate::{params, Result, Value};
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
    async fn can_fetch_rows() -> Result<()> {
        let connection = Driver::create(ConnectionOptions::build_from_url(
            &Url::parse(&std::env::var("DATABASE_DSN").unwrap()).unwrap(),
        ))
        .await
        .expect("Must be connected");

        let statement = connection.query("SELECT 1 + 1", params![]).await;
        assert_eq!(statement.is_ok(), true);
        let mut statement = statement.unwrap();
        let row = statement.fetch_one().await?.unwrap();

        assert_eq!(row.get(ColumnIndex::Position(0)).unwrap(), &Value::Int(2));

        Ok(())
    }
}

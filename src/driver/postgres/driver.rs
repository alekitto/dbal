use crate::driver::connection::{Connection, DriverConnection};
use crate::Result;
use std::future::Future;
use tokio_postgres::tls::{MakeTlsConnect, TlsStream};
use tokio_postgres::{Client, NoTls, Socket};

enum SslMode {
    None,
}

pub struct ConnectionOptions {
    host: Option<String>,
    port: Option<u16>,
    db_name: Option<String>,
    ssl_mode: SslMode,
    application_name: Option<String>,
}

pub struct Driver {
    pub(super) client: Client,
}

impl Driver {
    fn build_dsn(
        options: ConnectionOptions,
    ) -> (
        String,
        impl MakeTlsConnect<Socket, Stream = impl TlsStream + Unpin + Send>,
    ) {
        let mut dsn = "pgsql:".to_string();
        if let Some(host) = options.host {
            if !host.is_empty() {
                dsn += &format!("host={};", host);
            }
        }

        if let Some(port) = options.port {
            dsn += &format!("port={};", port);
        }

        let db_name = options.db_name.unwrap_or_else(|| "postgres".to_string());
        dsn += &format!("dbname={};", db_name);

        // TODO ssl

        if let Some(application_name) = options.application_name {
            if !application_name.is_empty() {
                dsn += &format!("application_name={};", application_name);
            }
        }

        (dsn, NoTls)
    }
}

impl DriverConnection<ConnectionOptions> for Driver {
    type Output = impl Future<Output = Result<Self>>;

    fn create(params: ConnectionOptions) -> Self::Output {
        let (config, tls) = Self::build_dsn(params);

        async move {
            // Connect to the database.
            let (client, connection) = tokio_postgres::connect(&config, tls).await?;
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("connection error: {}", e);
                }
            });

            Ok(Self { client })
        }
    }
}

impl<'conn> Connection<'conn> for Driver {
    type Statement = super::statement::Statement<'conn>;

    fn prepare<St: Into<String>>(&'conn self, sql: St) -> Result<Self::Statement> {
        let statement = super::statement::Statement::new(self, sql.into().as_str())?;

        Ok(statement)
    }
}

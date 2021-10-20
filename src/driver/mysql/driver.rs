use crate::driver::connection::{Connection, DriverConnection};
use crate::driver::server_info_aware_connection::ServerInfoAwareConnection;
use crate::{Async, Result};
use mysql_async::{Conn, Opts, OptsBuilder};
use std::future::Future;
use std::sync::Arc;
use tokio::sync::Mutex;
use url::Url;

pub struct Driver {
    pub(super) connection: Arc<Mutex<Conn>>,
}

pub struct ConnectionOptions {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub user: String,
    pub password: Option<String>,
    pub db_name: Option<String>,
}

impl ConnectionOptions {
    pub(crate) fn build_from_url(url: &Url) -> Self {
        let mut username = url.username().to_string();
        if username.is_empty() {
            username = String::from("root");
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
                    None
                } else {
                    Some(path)
                }
            },
        }
    }
}

impl DriverConnection<ConnectionOptions> for Driver {
    type Output = impl Future<Output = Result<Self>>;

    fn create(opts: ConnectionOptions) -> Self::Output {
        let opts_builder = OptsBuilder::default()
            .user(Some(&opts.user))
            .pass(opts.password)
            .ip_or_hostname(opts.host.unwrap_or_else(|| "localhost".to_string()))
            .tcp_port(opts.port.unwrap_or(3306))
            .db_name(opts.db_name);

        let opts = Opts::from(opts_builder);
        async move {
            let connection = Conn::new(opts).await?;

            Ok(Self {
                connection: Arc::new(Mutex::new(connection)),
            })
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

impl<'conn> ServerInfoAwareConnection<'conn> for Driver {
    fn server_version(&self) -> Async<Option<String>> {
        Box::pin(async move {
            let connection = self.connection.lock().await;
            let (major, minor, patch) = connection.server_version();

            Some(format!("{}.{}.{}", major, minor, patch))
        })
    }
}

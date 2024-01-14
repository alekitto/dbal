use crate::connection_options::SslMode;
use crate::driver::connection::{Connection, DriverConnection};
use crate::driver::mysql::platform;
use crate::driver::statement::Statement;
use crate::platform::DatabasePlatform;
use crate::sync::Mutex;
use crate::tls::DbalTls;
use crate::{Async, AsyncResult, EventDispatcher, Result};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, OptsBuilder};
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use url::Url;
use version_compare::{compare_to, Cmp};

pub struct Driver {
    pub(super) connection: Mutex<Conn>,
}

impl Debug for Driver {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Driver (MySQL)").finish_non_exhaustive()
    }
}

pub struct ConnectionOptions {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub user: String,
    pub password: Option<String>,
    pub db_name: Option<String>,
    pub ssl_mode: SslMode,
    pub ssl_cert: Option<String>,
    pub ssl_key: Option<String>,
    pub ssl_rootcert: Option<String>,
    pub ssl_crl: Option<String>,
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
                .unwrap_or_else(|| "root".to_string()),
            password: opts.password.as_ref().cloned(),
            db_name: opts.database_name.as_ref().cloned(),
            ssl_mode: opts.ssl_mode,
            ssl_cert: opts.ssl_cert.clone(),
            ssl_key: opts.ssl_key.clone(),
            ssl_rootcert: opts.ssl_rootcert.clone(),
            ssl_crl: opts.ssl_crl.clone(),
        }
    }
}

impl ConnectionOptions {
    pub fn build_from_url(url: &Url) -> Self {
        let mut username = url.username().to_string();
        if username.is_empty() {
            username = String::from("root");
        }

        let password = url.password().map(String::from);
        let mut ssl_mode = SslMode::Prefer;
        let mut ssl_cert = None;
        let mut ssl_key = None;
        let mut ssl_ca = None;

        for (name, value) in url.query_pairs() {
            match name.deref() {
                "ssl_mode" => {
                    ssl_mode = SslMode::from(name.deref());
                }
                "cert" => {
                    ssl_cert = Some(value.to_string());
                }
                "key" => {
                    ssl_key = Some(value.to_string());
                }
                "ca" => {
                    ssl_ca = Some(value.to_string());
                }
                _ => (),
            }
        }

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
            ssl_mode,
            ssl_cert,
            ssl_key,
            ssl_rootcert: ssl_ca,
            ssl_crl: None,
        }
    }
}

impl DriverConnection<ConnectionOptions> for Driver {
    type Output = impl Future<Output = Result<Self>>;

    fn create(opts: ConnectionOptions) -> Self::Output {
        let tls = DbalTls::new(
            opts.ssl_mode,
            opts.ssl_cert,
            opts.ssl_key,
            opts.ssl_rootcert,
            opts.ssl_crl,
        );
        let ssl_opts = tls.get_mysql_config();

        let opts_builder = OptsBuilder::default()
            .user(Some(&opts.user))
            .pass(opts.password)
            .ip_or_hostname(opts.host.unwrap_or_else(|| "localhost".to_string()))
            .tcp_port(opts.port.unwrap_or(3306))
            .ssl_opts(ssl_opts)
            .db_name(opts.db_name);

        let opts = Opts::from(opts_builder);
        async move {
            Ok(Self {
                connection: Mutex::new(Conn::new(opts).await?),
            })
        }
    }
}

impl<'conn> Connection<'conn> for Driver {
    fn create_platform(
        &self,
        ev: Arc<EventDispatcher>,
    ) -> Async<Box<dyn DatabasePlatform + Send + Sync>> {
        Box::pin(async move {
            let version = self
                .server_version()
                .await
                .unwrap_or_else(|| "5.7.9".to_string());
            let variant = if compare_to(&version, "10.5.2", Cmp::Ge).unwrap_or(false) {
                platform::MySQLVariant::MariaDB
            } else if compare_to(&version, "10", Cmp::Ge).unwrap_or(false) {
                platform::MySQLVariant::MySQL5_6 // MariaDB 10
            } else if compare_to(&version, "8", Cmp::Ge).unwrap_or(false) {
                platform::MySQLVariant::MySQL8_0
            } else if compare_to(&version, "5.7", Cmp::Ge).unwrap_or(false) {
                platform::MySQLVariant::MySQL5_7
            } else {
                platform::MySQLVariant::MySQL5_6
            };

            Box::new(platform::MySQLPlatform::new(variant, ev))
                as Box<dyn DatabasePlatform + Send + Sync>
        })
    }

    fn server_version(&self) -> Async<Option<String>> {
        Box::pin(async move {
            let connection = self.connection.lock().await;
            let (major, minor, mut patch) = connection.server_version();

            if major == 5 && minor == 7 {
                patch = 9
            } else if major < 10 {
                patch = 0
            }

            Some(format!("{}.{}.{}", major, minor, patch))
        })
    }

    fn prepare(&'conn self, sql: &str) -> Result<Box<dyn Statement + 'conn>> {
        let statement = super::statement::Statement::new(self, sql)?;

        Ok(Box::new(statement))
    }

    fn begin_transaction(&'conn self) -> AsyncResult<()> {
        Box::pin(async move {
            let mut connection = self.connection.lock().await;
            connection.deref_mut().query_drop("BEGIN").await?;

            Ok(())
        })
    }

    fn commit(&'conn self) -> AsyncResult<()> {
        Box::pin(async move {
            let mut connection = self.connection.lock().await;
            connection.deref_mut().query_drop("COMMIT").await?;

            Ok(())
        })
    }

    fn roll_back(&'conn self) -> AsyncResult<()> {
        Box::pin(async move {
            let mut connection = self.connection.lock().await;
            connection.deref_mut().query_drop("ROLLBACK").await?;

            Ok(())
        })
    }
}

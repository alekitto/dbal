use crate::driver::connection::{Connection, DriverConnection};
use crate::driver::mysql::platform;
use crate::driver::mysql::platform::MySQLPlatform;
use crate::driver::statement::Statement;
use crate::error::ErrorKind;
use crate::platform::DatabasePlatform;
use crate::{Async, Error, EventDispatcher, Result};
use mysql_async::{Conn, Opts, OptsBuilder};
use regex::Regex;
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::sync::Arc;
use tokio::sync::Mutex;
use url::Url;
use version_compare::{compare_to, Cmp};

pub struct Driver {
    pub(super) connection: Arc<Mutex<Conn>>,
}

impl Debug for Driver {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt("Driver (MySQL) {}", f)
    }
}

pub struct ConnectionOptions {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub user: String,
    pub password: Option<String>,
    pub db_name: Option<String>,
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
        }
    }
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

/**
 * Get a normalized 'version number' from the server string
 * returned by Oracle MySQL servers.
 *
 * @param string $versionString Version string returned by the driver, i.e. '5.7.10'
 *
 * @throws Exception
 */
fn get_oracle_mysql_version_number(version_string: String) -> Result<String> {
    let rx = Regex::new(r"^(?P<major>\d+)(?:\.(?P<minor>\d+)(?:\.(?P<patch>\d+))?)?")?;
    let version_parts = rx.captures(&version_string).ok_or(Error::new(
        ErrorKind::UnknownError,
        "mysql: invalid version string",
    ))?;

    let major_version = version_parts.name("major").unwrap().as_str();
    let minor_version = version_parts
        .name("minor")
        .map(|m| m.as_str())
        .unwrap_or("0");
    let patch_version = version_parts
        .name("minor")
        .map(|m| m.as_str())
        .unwrap_or_else(|| {
            if major_version == "5" && minor_version == "7" {
                "9"
            } else {
                "0"
            }
        });

    Ok(format!(
        "{}.{}.{}",
        major_version, minor_version, patch_version
    ))
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
            if version.contains("mariadb")
                && compare_to(version.clone(), "10.2.7", Cmp::Ge).unwrap_or(false)
            {
                Box::new(platform::MySQLPlatform::new(
                    platform::MySQLVariant::MariaDB,
                    ev,
                )) as Box<dyn DatabasePlatform + Send + Sync>
            } else {
                let version = get_oracle_mysql_version_number(version)
                    .unwrap_or_else(|_| "5.7.9".to_string());
                if compare_to(version, "8", Cmp::Ge).unwrap_or(false) {
                    Box::new(platform::MySQLPlatform::new(
                        platform::MySQLVariant::MySQL80,
                        ev,
                    )) as Box<dyn DatabasePlatform + Send + Sync>
                } else {
                    Box::new(platform::MySQLPlatform::new(
                        platform::MySQLVariant::MySQL,
                        ev,
                    )) as Box<dyn DatabasePlatform + Send + Sync>
                }
            }
        })
    }

    fn server_version(&self) -> Async<Option<String>> {
        Box::pin(async move {
            let connection = self.connection.lock().await;
            let (major, minor, patch) = connection.server_version();

            Some(format!("{}.{}.{}", major, minor, patch))
        })
    }

    fn prepare(&'conn self, sql: &str) -> Result<Box<dyn Statement + 'conn>> {
        let statement = super::statement::Statement::new(self, sql)?;

        Ok(Box::new(statement))
    }
}

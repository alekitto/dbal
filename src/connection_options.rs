use crate::platform::DatabasePlatform;
use crate::util::PlatformBox;
use crate::Error;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use url::Url;

#[derive(Clone, Copy, Debug)]
pub enum SslMode {
    None,
}

impl Default for SslMode {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Clone, Default)]
pub struct ConnectionOptions {
    pub scheme: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub file_path: Option<String>, // SQLite
    pub database_name: Option<String>,
    pub database_name_suffix: Option<String>,
    pub platform: Option<PlatformBox>,
    pub ssl_mode: SslMode,
    pub application_name: Option<String>, // PostgreSQL

                                          // TODO: replica/primary
}

impl ConnectionOptions {
    pub fn with_scheme(mut self, schema: Option<String>) -> Self {
        self.scheme = schema;
        self
    }

    pub fn with_username(mut self, username: Option<String>) -> Self {
        self.username = username;
        self
    }

    pub fn with_password(mut self, password: Option<String>) -> Self {
        self.password = password;
        self
    }

    pub fn with_host(mut self, host: Option<String>) -> Self {
        self.host = host;
        self
    }

    pub fn with_port(mut self, port: Option<u16>) -> Self {
        self.port = port;
        self
    }

    pub fn with_file_path(mut self, file_path: Option<String>) -> Self {
        self.file_path = file_path;
        self
    }

    pub fn with_database_name(mut self, database_name: Option<String>) -> Self {
        self.database_name = database_name;
        self
    }

    pub fn with_database_name_suffix(mut self, database_name_suffix: Option<String>) -> Self {
        self.database_name_suffix = database_name_suffix;
        self
    }

    pub fn with_platform(
        mut self,
        platform: Option<Box<(dyn DatabasePlatform + Sync + Send)>>,
    ) -> Self {
        self.platform = platform.map(Arc::new);
        self
    }

    pub fn with_ssl_mode(mut self, ssl_mode: SslMode) -> Self {
        self.ssl_mode = ssl_mode;
        self
    }

    pub fn with_application_name(mut self, application_name: Option<String>) -> Self {
        self.application_name = application_name;
        self
    }
}

impl TryFrom<&str> for ConnectionOptions {
    type Error = Error;

    fn try_from(dsn: &str) -> Result<Self, Self::Error> {
        let dsn = dsn.to_string();
        let options = Self::default();

        #[cfg(feature = "sqlite")]
        if dsn.eq("sqlite://:memory:") {
            return Ok(options
                .with_scheme(Some("sqlite".to_string()))
                .with_host(Some(":memory:".to_string())));
        }

        let url = Url::parse(dsn.as_str())?;
        let query_params: HashMap<Cow<str>, Cow<str>> = url.query_pairs().collect();
        let username = url.username();
        let db_name = url.path().trim_start_matches('/');

        match url.scheme() {
            #[cfg(not(feature = "mysql"))]
            platform @ "mysql" | platform @ "mariadb" => {
                Err(Error::platform_not_compiled(platform))
            }
            #[cfg(feature = "mysql")]
            "mysql" | "mariadb" => Ok(options
                .with_scheme(Some("mysql".to_string()))
                .with_username(if username.is_empty() {
                    None
                } else {
                    Some(username.to_string())
                })
                .with_password(url.password().map(String::from))
                .with_host(url.host_str().map(String::from))
                .with_port(url.port().or(Some(3306)))
                .with_database_name(Some(db_name.to_string()))
                .with_database_name_suffix(
                    query_params.get("dbname_suffix").map(|s| s.to_string()),
                )),
            #[cfg(not(feature = "postgres"))]
            platform @ "pg"
            | platform @ "psql"
            | platform @ "postgres"
            | platform @ "postgresql" => Err(Error::platform_not_compiled(platform)),
            #[cfg(feature = "postgres")]
            "pg" | "psql" | "postgres" | "postgresql" => {
                let username = if username.is_empty() {
                    "postgres"
                } else {
                    username
                };

                let db_name = if db_name.is_empty() {
                    "postgres"
                } else {
                    db_name
                };

                Ok(options
                    .with_scheme(Some("psql".to_string()))
                    .with_username(Some(username.to_string()))
                    .with_password(url.password().map(String::from))
                    .with_host(url.host_str().map(String::from))
                    .with_port(url.port().or(Some(5432)))
                    .with_database_name(Some(db_name.to_string()))
                    .with_database_name_suffix(
                        query_params.get("dbname_suffix").map(|s| s.to_string()),
                    )
                    .with_application_name(
                        query_params.get("application_name").map(|s| s.to_string()),
                    ))
            }
            #[cfg(not(feature = "sqlite"))]
            platform @ "sqlite" => Err(Error::platform_not_compiled(platform)),
            #[cfg(feature = "sqlite")]
            "sqlite" => Ok(options
                .with_scheme(Some("sqlite".to_string()))
                .with_file_path(Some(url.path().to_string()))),
            scheme => Err(Error::unknown_driver(scheme)),
        }
    }
}

impl Debug for ConnectionOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionOptions")
            .field("scheme", &self.scheme)
            .field("username", &self.username)
            .field("password", &self.password)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("file_path", &self.file_path)
            .field("database_name", &self.database_name)
            .field("database_name_suffix", &self.database_name_suffix)
            .field("ssl_mode", &self.ssl_mode)
            .field("application_name", &self.application_name)
            .finish()
    }
}

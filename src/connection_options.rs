use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use url::{ParseError, Url};

#[derive(Clone, Copy, Debug)]
pub enum SslMode {
    None,
}

impl Default for SslMode {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Default)]
pub struct ConnectionOptions {
    pub scheme: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub file_path: Option<String>, // SQLite
    pub database_name: Option<String>,
    pub database_name_suffix: Option<String>,
    pub ssl_mode: SslMode,
    pub application_name: Option<String>, // PostgreSQL

                                          // TODO: replica/primary
}

impl ConnectionOptions {
    pub fn with_scheme(mut self, schema: Option<String>) -> Self {
        self.scheme = schema.into();
        self
    }

    pub fn with_username(mut self, username: Option<String>) -> Self {
        self.username = username.into();
        self
    }

    pub fn with_password(mut self, password: Option<String>) -> Self {
        self.password = password.into();
        self
    }

    pub fn with_host(mut self, host: Option<String>) -> Self {
        self.host = host.into();
        self
    }

    pub fn with_port(mut self, port: Option<u16>) -> Self {
        self.port = port.into();
        self
    }

    pub fn with_file_path(mut self, file_path: Option<String>) -> Self {
        self.file_path = file_path.into();
        self
    }

    pub fn with_database_name(mut self, database_name: Option<String>) -> Self {
        self.database_name = database_name.into();
        self
    }

    pub fn with_database_name_suffix(mut self, database_name_suffix: Option<String>) -> Self {
        self.database_name_suffix = database_name_suffix.into();
        self
    }

    pub fn with_ssl_mode(mut self, ssl_mode: SslMode) -> Self {
        self.ssl_mode = ssl_mode;
        self
    }

    pub fn with_application_name(mut self, application_name: Option<String>) -> Self {
        self.application_name = application_name.into();
        self
    }
}

impl TryFrom<&str> for ConnectionOptions {
    type Error = ParseError;

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

        let options = match url.scheme() {
            #[cfg(feature = "mysql")]
            "mysql" | "mariadb" => options
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
                ),
            #[cfg(feature = "postgres")]
            "pg" | "psql" | "postgres" | "postgresql" => options
                .with_scheme(Some("psql".to_string()))
                .with_username(Some(
                    if username.is_empty() {
                        "postgres"
                    } else {
                        username
                    }
                    .to_string(),
                ))
                .with_password(url.password().map(String::from))
                .with_host(url.host_str().map(String::from))
                .with_port(url.port().or(Some(5432)))
                .with_database_name(Some(
                    if db_name.is_empty() {
                        "postgres"
                    } else {
                        db_name
                    }
                    .to_string(),
                ))
                .with_database_name_suffix(query_params.get("dbname_suffix").map(|s| s.to_string()))
                .with_application_name(query_params.get("application_name").map(|s| s.to_string())),
            #[cfg(feature = "sqlite")]
            "sqlite" => options
                .with_scheme(Some("sqlite".to_string()))
                .with_file_path(Some(url.path().to_string())),
            _ => unimplemented!(),
        };

        Ok(options)
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

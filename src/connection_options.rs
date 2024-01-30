use crate::platform::DatabasePlatform;
use crate::util::PlatformBox;
use crate::Error;
#[cfg(any(feature = "mysql", feature = "postgres"))]
use percent_encoding::percent_decode_str;
#[cfg(any(feature = "mysql", feature = "postgres"))]
use std::borrow::Cow;
#[cfg(any(feature = "mysql", feature = "postgres"))]
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use url::Url;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SslMode {
    None,
    Allow,
    Prefer,
    Require,
    VerifyCa,
    VerifyFull,
}

impl Default for SslMode {
    fn default() -> Self {
        Self::None
    }
}

impl<T: AsRef<str>> From<T> for SslMode {
    fn from(value: T) -> Self {
        match value.as_ref().to_lowercase().as_str() {
            "none" => SslMode::None,
            "allow" => SslMode::Allow,
            "prefer" => SslMode::Prefer,
            "require" => SslMode::Require,
            "verify_ca" | "verify-ca" => SslMode::VerifyCa,
            "verify_full" | "verify-full" => SslMode::VerifyFull,
            _ => SslMode::Prefer,
        }
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
    pub ssl_cert: Option<String>,
    pub ssl_key: Option<String>,
    pub ssl_rootcert: Option<String>,
    pub ssl_crl: Option<String>,
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

    pub fn with_ssl_cert(mut self, ssl_cert: Option<String>) -> Self {
        self.ssl_cert = ssl_cert;
        self
    }

    pub fn with_ssl_key(mut self, ssl_key: Option<String>) -> Self {
        self.ssl_key = ssl_key;
        self
    }

    pub fn with_ssl_ca(mut self, ssl_ca: Option<String>) -> Self {
        self.ssl_rootcert = ssl_ca;
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
        #[cfg(any(feature = "mysql", feature = "postgres"))]
        let query_params: HashMap<Cow<str>, Cow<str>> = url.query_pairs().collect();
        #[cfg(any(feature = "mysql", feature = "postgres"))]
        let username = percent_decode_str(url.username())
            .decode_utf8()?
            .to_string();
        #[cfg(any(feature = "mysql", feature = "postgres"))]
        let password = {
            if let Some(password) = url.password() {
                Some(percent_decode_str(password).decode_utf8()?.to_string())
            } else {
                None
            }
        };
        #[cfg(any(feature = "mysql", feature = "postgres"))]
        let db_name = url.path().trim_start_matches('/');

        #[cfg(any(feature = "mysql", feature = "postgres"))]
        let ssl_mode = if let Some(ssl) = query_params.get("ssl_mode").map(|s| s.to_string()) {
            SslMode::from(ssl)
        } else {
            SslMode::Prefer
        };

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
                    Some(username)
                })
                .with_password(password)
                .with_host(url.host_str().map(String::from))
                .with_port(url.port().or(Some(3306)))
                .with_ssl_mode(ssl_mode)
                .with_ssl_cert(query_params.get("cert").map(|s| s.to_string()))
                .with_ssl_key(query_params.get("key").map(|s| s.to_string()))
                .with_ssl_ca(query_params.get("ca").map(|s| s.to_string()))
                .with_database_name(Some(db_name.to_string()))
                .with_database_name_suffix(
                    query_params.get("dbname_suffix").map(|s| s.to_string()),
                )),
            #[cfg(not(feature = "postgres"))]
            platform @ "pg"
            | platform @ "pgsql"
            | platform @ "psql"
            | platform @ "postgres"
            | platform @ "postgresql" => Err(Error::platform_not_compiled(platform)),
            #[cfg(feature = "postgres")]
            "pg" | "psql" | "pgsql" | "postgres" | "postgresql" => {
                let username = if username.is_empty() {
                    "postgres".to_string()
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
                    .with_username(Some(username))
                    .with_password(password)
                    .with_host(url.host_str().map(String::from))
                    .with_port(url.port().or(Some(5432)))
                    .with_ssl_mode(ssl_mode)
                    .with_ssl_cert(query_params.get("cert").map(|s| s.to_string()))
                    .with_ssl_key(query_params.get("key").map(|s| s.to_string()))
                    .with_ssl_ca(query_params.get("ca").map(|s| s.to_string()))
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

#[cfg(test)]
mod tests {
    use crate::{ConnectionOptions, Result};

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[test]
    pub fn should_parse_dsn_correctly_for_mysql() -> Result<()> {
        let dsn = "mysql://root:p4ssw0rd@mysql.domain.example.com:3300/my_db?ca=root.crt&ssl_mode=require";

        #[cfg(feature = "mysql")]
        {
            let opts = ConnectionOptions::try_from(dsn)?;
            assert_eq!(opts.username.as_deref(), Some("root"));
            assert_eq!(opts.password.as_deref(), Some("p4ssw0rd"));
            assert_eq!(opts.host.as_deref(), Some("mysql.domain.example.com"));
            assert_eq!(opts.port, Some(3300));
            assert_eq!(opts.database_name.as_deref(), Some("my_db"));
            assert_eq!(opts.ssl_rootcert.as_deref(), Some("root.crt"));
            assert_eq!(opts.ssl_mode, super::SslMode::Require);
        }

        #[cfg(not(feature = "mysql"))]
        {
            let opts = ConnectionOptions::try_from(dsn);
            assert!(opts.is_err());
            assert!(matches!(
                opts.unwrap_err().kind(),
                crate::error::ErrorKind::PlatformNotCompiled
            ));
        }

        Ok(())
    }

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[test]
    pub fn should_parse_dsn_correctly_for_postgresql() -> Result<()> {
        let dsn = "postgres://u%24er:p%23ssw%40rd@postgres.network.internal:4456/my_db?ca=root.crt&ssl_mode=require";

        #[cfg(feature = "postgres")]
        {
            let opts = ConnectionOptions::try_from(dsn)?;
            assert_eq!(opts.username.as_deref(), Some("u$er"));
            assert_eq!(opts.password.as_deref(), Some("p#ssw@rd"));
            assert_eq!(opts.host.as_deref(), Some("postgres.network.internal"));
            assert_eq!(opts.port, Some(4456));
            assert_eq!(opts.database_name.as_deref(), Some("my_db"));
            assert_eq!(opts.ssl_rootcert.as_deref(), Some("root.crt"));
            assert_eq!(opts.ssl_mode, super::SslMode::Require);
        }

        #[cfg(not(feature = "postgres"))]
        {
            let opts = ConnectionOptions::try_from(dsn);
            assert!(opts.is_err());
            assert!(matches!(
                opts.unwrap_err().kind(),
                crate::error::ErrorKind::PlatformNotCompiled
            ));
        }

        Ok(())
    }

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[test]
    pub fn should_parse_dsn_correctly_for_sqlite() -> Result<()> {
        let dsn = "sqlite:///home/user/test.db";

        #[cfg(feature = "sqlite")]
        {
            let opts = ConnectionOptions::try_from(dsn)?;
            assert_eq!(opts.file_path.as_deref(), Some("/home/user/test.db"));
        }

        #[cfg(not(feature = "sqlite"))]
        {
            let opts = ConnectionOptions::try_from(dsn);
            assert!(opts.is_err());
            assert!(matches!(
                opts.unwrap_err().kind(),
                crate::error::ErrorKind::PlatformNotCompiled
            ));
        }

        Ok(())
    }
}

#[cfg(feature = "postgres")]
pub(crate) mod postgresql;

use crate::connection_options::SslMode;
#[cfg(feature = "postgres")]
pub use postgresql::PgMaybeTlsStream;

pub struct DbalTls {
    mode: SslMode,
    ssl_cert: Option<String>,
    ssl_key: Option<String>,
    ssl_rootcert: Option<String>,
    #[allow(dead_code)]
    ssl_crl: Option<String>,
}

impl DbalTls {
    pub fn new(
        ssl_mode: SslMode,
        ssl_cert: Option<String>,
        ssl_key: Option<String>,
        ssl_rootcert: Option<String>,
        ssl_crl: Option<String>,
    ) -> Self {
        Self {
            mode: ssl_mode,
            ssl_cert,
            ssl_key,
            ssl_rootcert,
            ssl_crl,
        }
    }

    #[cfg(feature = "postgres")]
    pub fn get_pg_config(&self) -> postgresql::TlsConfig {
        postgresql::TlsConfig {
            mode: self.mode,
            ssl_cert: self.ssl_cert.clone(),
            ssl_key: self.ssl_key.clone(),
            ssl_rootcert: self.ssl_rootcert.clone(),
            ssl_crl: self.ssl_crl.clone(),
        }
    }

    #[cfg(feature = "mysql")]
    pub fn get_mysql_config(&self) -> Option<mysql_async::SslOpts> {
        use mysql_async::{ClientIdentity, SslOpts};
        use std::path::PathBuf;

        if self.mode == SslMode::None || self.mode == SslMode::Prefer || self.mode == SslMode::Allow
        {
            None
        } else {
            let mut ssl_opts = SslOpts::default();
            if self.mode == SslMode::Require {
                ssl_opts = ssl_opts
                    .with_danger_accept_invalid_certs(true)
                    .with_danger_skip_domain_validation(true);
            } else if self.mode == SslMode::VerifyCa {
                ssl_opts = ssl_opts.with_danger_skip_domain_validation(true);
            }

            if let Some(ca) = self.ssl_rootcert.as_ref() {
                ssl_opts = ssl_opts.with_root_cert_path(Some(PathBuf::from(ca)));
            }

            if let (Some(cert), Some(key)) = (self.ssl_cert.as_ref(), self.ssl_key.as_ref()) {
                #[cfg(feature = "rustls")]
                {
                    ssl_opts = ssl_opts.with_client_identity(Some(ClientIdentity::new(
                        PathBuf::from(cert),
                        PathBuf::from(key),
                    )));
                }
                #[cfg(feature = "native-tls")]
                {
                    ssl_opts = ssl_opts.with_client_identity(Some(
                        ClientIdentity::new(PathBuf::from(cert)).with_password(key.clone()),
                    ));
                }
                #[cfg(not(any(feature = "rustls", feature = "native-tls")))]
                {
                    panic!("You must enable one of rustls or native-tls features");
                }
            }

            Some(ssl_opts)
        }
    }
}

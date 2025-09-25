use crate::connection_options::SslMode;
use crate::error::StdError;
use std::future::Future;
use std::io;
use std::pin::{Pin, pin};
use std::task::{Context, Poll};
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};
use tokio::net::{TcpStream, UnixStream};
use tokio_postgres::tls::{ChannelBinding, MakeTlsConnect, TlsConnect, TlsStream};

pub enum PgMaybeTlsStream {
    Raw(TcpStream),
    #[cfg(unix)]
    Unix(UnixStream),
    #[cfg(feature = "rustls")]
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
    #[cfg(feature = "native-tls")]
    Tls(tokio_native_tls::TlsStream<TcpStream>),
}

impl From<TcpStream> for PgMaybeTlsStream {
    fn from(value: TcpStream) -> Self {
        Self::Raw(value)
    }
}

impl AsyncRead for PgMaybeTlsStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Raw(s) => {
                let pinned = pin!(s);
                pinned.poll_read(cx, buf)
            }
            #[cfg(unix)]
            Self::Unix(s) => {
                let pinned = pin!(s);
                pinned.poll_read(cx, buf)
            }
            #[cfg(any(feature = "rustls", feature = "native-tls"))]
            Self::Tls(s) => {
                let pinned = pin!(s);
                pinned.poll_read(cx, buf)
            }
        }
    }
}

impl AsyncWrite for PgMaybeTlsStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut *self {
            Self::Raw(s) => {
                let pinned = pin!(s);
                pinned.poll_write(cx, buf)
            }
            #[cfg(unix)]
            Self::Unix(s) => {
                let pinned = pin!(s);
                pinned.poll_write(cx, buf)
            }
            #[cfg(any(feature = "rustls", feature = "native-tls"))]
            Self::Tls(s) => {
                let pinned = pin!(s);
                pinned.poll_write(cx, buf)
            }
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Raw(s) => {
                let pinned = pin!(s);
                pinned.poll_flush(cx)
            }
            #[cfg(unix)]
            Self::Unix(s) => {
                let pinned = pin!(s);
                pinned.poll_flush(cx)
            }
            #[cfg(any(feature = "rustls", feature = "native-tls"))]
            Self::Tls(s) => {
                let pinned = pin!(s);
                pinned.poll_flush(cx)
            }
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Raw(s) => {
                let pinned = pin!(s);
                pinned.poll_shutdown(cx)
            }
            #[cfg(unix)]
            Self::Unix(s) => {
                let pinned = pin!(s);
                pinned.poll_shutdown(cx)
            }
            #[cfg(any(feature = "rustls", feature = "native-tls"))]
            Self::Tls(s) => {
                let pinned = pin!(s);
                pinned.poll_shutdown(cx)
            }
        }
    }
}

impl TlsStream for PgMaybeTlsStream {
    fn channel_binding(&self) -> ChannelBinding {
        match self {
            Self::Raw(_) => ChannelBinding::none(),
            Self::Unix(_) => ChannelBinding::none(),
            #[cfg(any(feature = "rustls", feature = "native-tls"))]
            Self::Tls(_) => ChannelBinding::none(),
        }
    }
}

pub struct PgDbalTlsConnect {
    domain: String,
    tls_config: TlsConfig,
}

impl TlsConnect<PgMaybeTlsStream> for PgDbalTlsConnect {
    type Stream = PgMaybeTlsStream;
    type Error = Box<dyn std::error::Error + Sync + Send>;
    type Future = impl Future<Output = Result<Self::Stream, Self::Error>>;

    #[cfg(feature = "rustls")]
    fn connect(self, stream: PgMaybeTlsStream) -> Self::Future {
        let PgMaybeTlsStream::Raw(tcp_stream) = stream else {
            panic!("unexpected");
        };

        Box::pin(async move {
            let config = self
                .tls_config
                .create_client_config()
                .await
                .map_err(StdError::from)?;
            let config = tokio_rustls::TlsConnector::from(std::sync::Arc::new(config));
            let client_conn = config
                .connect(
                    rustls::pki_types::ServerName::try_from(self.domain.clone()).unwrap(),
                    tcp_stream,
                )
                .await?;

            Ok(PgMaybeTlsStream::Tls(Box::new(client_conn)))
        })
    }

    #[cfg(feature = "native-tls")]
    fn connect(self, stream: PgMaybeTlsStream) -> Self::Future {
        Box::pin(async move {
            let PgMaybeTlsStream::Raw(tcp_stream) = stream else {
                panic!("unexpected");
            };

            let connector = self
                .tls_config
                .create_client_config()
                .await
                .map_err(|e| StdError::from(e))?;
            Ok(PgMaybeTlsStream::Tls(
                connector.connect(&self.domain, tcp_stream).await?,
            ))
        })
    }

    #[cfg(not(any(feature = "rustls", feature = "native-tls")))]
    fn connect(self, stream: PgMaybeTlsStream) -> Self::Future {
        Box::pin(async move { Ok(stream) })
    }
}

impl MakeTlsConnect<PgMaybeTlsStream> for super::DbalTls {
    type Stream = PgMaybeTlsStream;
    type TlsConnect = PgDbalTlsConnect;
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn make_tls_connect(&mut self, domain: &str) -> Result<Self::TlsConnect, Self::Error> {
        Ok(PgDbalTlsConnect {
            domain: domain.to_string(),
            tls_config: self.get_pg_config(),
        })
    }
}

#[derive(Clone)]
pub struct TlsConfig {
    pub mode: SslMode,
    pub ssl_cert: Option<String>,
    pub ssl_key: Option<String>,
    pub ssl_rootcert: Option<String>,
    pub ssl_crl: Option<String>,
}

#[cfg(feature = "native-tls")]
impl TlsConfig {
    pub(crate) async fn create_client_config(
        &self,
    ) -> crate::Result<tokio_native_tls::TlsConnector> {
        let mut config = native_tls::TlsConnector::builder();
        if self.mode == SslMode::VerifyCa {
            config.danger_accept_invalid_hostnames(true);
        } else if self.mode != SslMode::VerifyFull {
            config
                .danger_accept_invalid_hostnames(true)
                .danger_accept_invalid_certs(true);
        }

        if let Some(root_cert) = self.ssl_rootcert.as_ref() {
            let mut f = File::open(root_cert).await?;
            let mut buf = vec![];
            f.read_to_end(&mut buf).await?;

            let cert = native_tls::Certificate::from_der(buf.as_slice())
                .or_else(|_| native_tls::Certificate::from_pem(buf.as_slice()))?;

            config.add_root_certificate(cert);
        }

        Ok(tokio_native_tls::TlsConnector::from(
            if self.ssl_key.is_some() && self.ssl_cert.is_some() {
                let path = self.ssl_cert.as_ref().unwrap();
                let mut f = File::open(path).await?;
                let mut cert = vec![];
                f.read_to_end(&mut cert).await?;

                let path = self.ssl_key.as_ref().unwrap();
                let mut f = File::open(path).await?;
                let mut key = vec![];
                f.read_to_end(&mut key).await?;

                config
                    .identity(native_tls::Identity::from_pkcs8(
                        cert.as_slice(),
                        key.as_slice(),
                    )?)
                    .build()?
            } else {
                config.build()?
            },
        ))
    }
}

#[cfg(feature = "rustls")]
impl TlsConfig {
    pub(crate) async fn create_root_store(&self) -> crate::Result<rustls::RootCertStore> {
        use itertools::Itertools;

        let mut root_store = rustls::RootCertStore::empty();
        for cert in rustls_native_certs::load_native_certs().unwrap() {
            let _ = root_store.add(cert);
        }

        if let Some(root_cert) = &self.ssl_rootcert {
            let mut f = File::open(root_cert).await?;
            let mut buf = vec![];
            f.read_to_end(&mut buf).await?;

            rustls_pemfile::certs(&mut std::io::BufReader::new(buf.as_slice()))
                .map(|result| match result {
                    Ok(der) => root_store.add(der).map_err(|e| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("could not load PEM file {root_cert:?}: {e}"),
                        )
                    }),
                    Err(err) => Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("could not load PEM file {root_cert:?}: {err}"),
                    )),
                })
                .try_collect::<_, (), _>()?;
        }

        Ok(root_store)
    }

    pub(crate) async fn create_client_config(&self) -> crate::Result<rustls::ClientConfig> {
        use itertools::Itertools;

        let config = if self.mode == SslMode::VerifyCa {
            rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(std::sync::Arc::new(
                    internal::CaRootServerVerifier::new(std::sync::Arc::new(
                        self.create_root_store().await?,
                    )),
                ))
        } else if self.mode == SslMode::VerifyFull {
            self.builder_with_root_store().await?
        } else {
            rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(std::sync::Arc::new(
                    internal::NoopServerVerifier {},
                ))
        };

        Ok(if self.ssl_key.is_some() && self.ssl_cert.is_some() {
            let path = self.ssl_cert.as_ref().unwrap();
            let mut f = File::open(path).await?;
            let mut buf = vec![];
            f.read_to_end(&mut buf).await?;
            let certs = rustls_pemfile::certs(&mut std::io::BufReader::new(buf.as_slice()))
                .map(|result| match result {
                    Ok(der) => Ok(der),
                    Err(err) => Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("could not load PEM file {path:?}: {err}"),
                    )),
                })
                .try_collect()?;

            let path = self.ssl_key.as_ref().unwrap();
            let mut f = File::open(path).await?;
            let mut buf = vec![];
            f.read_to_end(&mut buf).await?;
            let key = rustls_pemfile::private_key(&mut std::io::BufReader::new(buf.as_slice()))?
                .ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("could not load key file {path:?}"),
                    )
                })?;

            config.with_client_auth_cert(certs, key).unwrap()
        } else {
            config.with_no_client_auth()
        })
    }

    async fn builder_with_root_store(
        &self,
    ) -> crate::Result<rustls::ConfigBuilder<rustls::ClientConfig, rustls::client::WantsClientCert>>
    {
        Ok(rustls::ClientConfig::builder().with_root_certificates(self.create_root_store().await?))
    }
}

#[cfg(feature = "rustls")]
mod internal {
    use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
    use rustls::crypto::aws_lc_rs::default_provider;
    use rustls::crypto::{
        WebPkiSupportedAlgorithms, verify_tls12_signature, verify_tls13_signature,
    };
    use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
    use rustls::{
        CertRevocationListError, CertificateError, DigitallySignedStruct, Error, OtherError,
        RootCertStore, SignatureScheme, pki_types,
    };
    use std::sync::Arc;
    use webpki::EndEntityCert;

    #[derive(Debug)]
    pub(super) struct CaRootServerVerifier {
        roots: Arc<RootCertStore>,
        supported: WebPkiSupportedAlgorithms,
    }

    impl CaRootServerVerifier {
        pub fn new(roots: Arc<RootCertStore>) -> Self {
            Self {
                roots,
                supported: default_provider().signature_verification_algorithms,
            }
        }
    }

    #[allow(deprecated)]
    fn pki_error(error: webpki::Error) -> Error {
        use webpki::Error::*;
        match error {
            BadDer | BadDerTime | TrailingData(_) => CertificateError::BadEncoding.into(),
            CertNotValidYet { .. } => CertificateError::NotValidYet.into(),
            CertExpired { .. } => CertificateError::Expired.into(),
            InvalidCertValidity => CertificateError::Expired.into(),
            UnknownIssuer => CertificateError::UnknownIssuer.into(),
            CertNotValidForName(_) => CertificateError::NotValidForName.into(),
            CertRevoked => CertificateError::Revoked.into(),
            UnknownRevocationStatus => CertificateError::UnknownRevocationStatus.into(),
            IssuerNotCrlSigner => CertRevocationListError::IssuerInvalidForCrl.into(),

            InvalidSignatureForPublicKey
            | UnsupportedSignatureAlgorithm
            | UnsupportedSignatureAlgorithmForPublicKey => CertificateError::BadSignature.into(),

            InvalidCrlSignatureForPublicKey
            | UnsupportedCrlSignatureAlgorithm
            | UnsupportedCrlSignatureAlgorithmForPublicKey => {
                CertRevocationListError::BadSignature.into()
            }

            _ => CertificateError::Other(OtherError(Arc::new(error))).into(),
        }
    }

    impl ServerCertVerifier for CaRootServerVerifier {
        /// Will verify the certificate is valid in the following ways:
        /// - Signed by a trusted `RootCertStore` CA
        /// - Not Expired
        /// - Valid for DNS entry
        /// - Valid revocation status (if applicable).
        ///
        /// Depending on the verifier's configuration revocation status checking may be performed for
        /// each certificate in the chain to a root CA (excluding the root itself), or only the
        /// end entity certificate. Similarly, unknown revocation status may be treated as an error
        /// or allowed based on configuration.
        fn verify_server_cert(
            &self,
            end_entity: &CertificateDer<'_>,
            intermediates: &[CertificateDer<'_>],
            _: &ServerName<'_>,
            _: &[u8],
            now: UnixTime,
        ) -> Result<ServerCertVerified, Error> {
            let cert = EndEntityCert::try_from(end_entity).map_err(pki_error)?;

            cert.verify_for_usage(
                self.supported.all,
                &self.roots.roots,
                intermediates,
                now,
                webpki::KeyUsage::server_auth(),
                None,
                None,
            )
            .map_err(pki_error)?;

            Ok(ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, Error> {
            verify_tls12_signature(message, cert, dss, &self.supported)
        }

        fn verify_tls13_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, Error> {
            verify_tls13_signature(message, cert, dss, &self.supported)
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            self.supported.supported_schemes()
        }
    }

    #[derive(Debug)]
    pub(super) struct NoopServerVerifier;

    impl ServerCertVerifier for NoopServerVerifier {
        fn verify_server_cert(
            &self,
            _: &CertificateDer<'_>,
            _: &[CertificateDer<'_>],
            _: &ServerName,
            _: &[u8],
            _: pki_types::UnixTime,
        ) -> std::result::Result<ServerCertVerified, rustls::Error> {
            Ok(ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            _: &[u8],
            _: &CertificateDer<'_>,
            _: &DigitallySignedStruct,
        ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
            Ok(HandshakeSignatureValid::assertion())
        }

        fn verify_tls13_signature(
            &self,
            _: &[u8],
            _: &CertificateDer<'_>,
            _: &DigitallySignedStruct,
        ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
            Ok(HandshakeSignatureValid::assertion())
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            vec![
                SignatureScheme::RSA_PKCS1_SHA1,
                SignatureScheme::ECDSA_SHA1_Legacy,
                SignatureScheme::RSA_PKCS1_SHA256,
                SignatureScheme::ECDSA_NISTP256_SHA256,
                SignatureScheme::RSA_PKCS1_SHA384,
                SignatureScheme::ECDSA_NISTP384_SHA384,
                SignatureScheme::RSA_PKCS1_SHA512,
                SignatureScheme::ECDSA_NISTP521_SHA512,
                SignatureScheme::RSA_PSS_SHA256,
                SignatureScheme::RSA_PSS_SHA384,
                SignatureScheme::RSA_PSS_SHA512,
                SignatureScheme::ED25519,
                SignatureScheme::ED448,
            ]
        }
    }
}

use crate::driver::postgres::keepalive::KeepaliveConfig;
use crate::error::ErrorKind;
use crate::tls::{DbalTls, PgMaybeTlsStream};
use crate::{Error, Result};
use rand::prelude::SliceRandom;
use rand::rng;
use socket2::{SockRef, TcpKeepalive};
use std::net::IpAddr;
use std::path::PathBuf;
use std::time::Duration;
use std::{cmp, io};
use tokio::net;
use tokio::net::{TcpStream, UnixStream};
use tokio_postgres::config::{Host, LoadBalanceHosts, TargetSessionAttrs};
use tokio_postgres::tls::MakeTlsConnect;
use tokio_postgres::{Client, Config, SimpleQueryMessage};

pub(crate) async fn connect(
    mut tls: DbalTls,
    config: &Config,
) -> Result<(
    Client,
    tokio_postgres::Connection<PgMaybeTlsStream, PgMaybeTlsStream>,
)> {
    let hosts = config.get_hosts();
    let hostaddrs = config.get_hostaddrs();
    let ports = config.get_ports();

    if hosts.is_empty() && hostaddrs.is_empty() {
        return Err(Error::config("both host and hostaddr are missing"));
    }

    if !hosts.is_empty() && !hostaddrs.is_empty() && hosts.len() != hostaddrs.len() {
        let msg = format!(
            "number of hosts ({}) is different from number of hostaddrs ({})",
            hosts.len(),
            hostaddrs.len(),
        );
        return Err(Error::config(&msg));
    }

    // At this point, either one of the following two scenarios could happen:
    // (1) either config.host or config.hostaddr must be empty;
    // (2) if both config.host and config.hostaddr are NOT empty; their lengths must be equal.
    let num_hosts = cmp::max(hosts.len(), hostaddrs.len());

    if ports.len() > 1 && ports.len() != num_hosts {
        return Err(Error::config("invalid number of ports"));
    }

    let mut indices = (0..num_hosts).collect::<Vec<_>>();
    if config.get_load_balance_hosts() == LoadBalanceHosts::Random {
        indices.shuffle(&mut rng());
    }

    let mut error = None;
    for i in indices {
        let host = hosts.get(i);
        let hostaddr = hostaddrs.get(i);
        let port = ports
            .get(i)
            .or_else(|| ports.first())
            .copied()
            .unwrap_or(5432);

        // The value of host is used as the hostname for TLS validation,
        let hostname = match host {
            Some(Host::Tcp(host)) => Some(host.clone()),
            // postgres doesn't support TLS over unix sockets, so the choice here doesn't matter
            #[cfg(unix)]
            Some(Host::Unix(_)) => None,
            None => None,
        };

        // Try to use the value of hostaddr to establish the TCP connection,
        // fallback to host if hostaddr is not present.
        let addr = match hostaddr {
            Some(ipaddr) => Host::Tcp(ipaddr.to_string()),
            None => host.cloned().unwrap(),
        };

        match connect_host(addr, hostname, port, &mut tls, config).await {
            Ok((client, connection)) => return Ok((client, connection)),
            Err(e) => error = Some(e),
        }
    }

    Err(error.unwrap())
}

#[derive(Clone)]
pub(crate) enum Addr {
    Tcp(IpAddr),
    #[cfg(unix)]
    Unix(PathBuf),
}

async fn connect_host(
    host: Host,
    hostname: Option<String>,
    port: u16,
    tls: &mut DbalTls,
    config: &Config,
) -> Result<(
    Client,
    tokio_postgres::Connection<PgMaybeTlsStream, PgMaybeTlsStream>,
)> {
    match host {
        Host::Tcp(host) => {
            let mut addrs = net::lookup_host((&*host, port))
                .await
                .map_err(Error::connect)?
                .collect::<Vec<_>>();

            if config.get_load_balance_hosts() == LoadBalanceHosts::Random {
                addrs.shuffle(&mut rng());
            }

            let mut last_err = None;
            for addr in addrs {
                match connect_once(Addr::Tcp(addr.ip()), hostname.as_deref(), port, tls, config)
                    .await
                {
                    Ok(stream) => return Ok(stream),
                    Err(e) => {
                        last_err = Some(e);
                        continue;
                    }
                };
            }

            Err(last_err.unwrap_or_else(|| {
                Error::connect(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "could not resolve any addresses",
                ))
            }))
        }
        #[cfg(unix)]
        Host::Unix(path) => {
            connect_once(Addr::Unix(path), hostname.as_deref(), port, tls, config).await
        }
    }
}

async fn connect_once(
    addr: Addr,
    hostname: Option<&str>,
    port: u16,
    tls: &mut DbalTls,
    config: &Config,
) -> Result<(
    Client,
    tokio_postgres::Connection<PgMaybeTlsStream, PgMaybeTlsStream>,
)> {
    let keepalive_config = KeepaliveConfig {
        idle: config.get_keepalives_idle(),
        interval: config.get_keepalives_interval(),
        retries: config.get_keepalives_retries(),
    };

    let socket = connect_socket(
        &addr,
        port,
        config.get_tcp_user_timeout().cloned(),
        if config.get_keepalives() {
            Some(&keepalive_config)
        } else {
            None
        },
    )
    .await?;

    let tls = tls
        .make_tls_connect(hostname.unwrap_or(""))
        .map_err(|e| Error::new(ErrorKind::UnknownError, e.to_string()))?;
    let (client, connection) = config.connect_raw(socket, tls).await?;

    if let TargetSessionAttrs::ReadWrite = config.get_target_session_attrs() {
        let rows = client.simple_query("SHOW transaction_read_only").await?;
        for next in rows {
            if let SimpleQueryMessage::Row(row) = next {
                if row.try_get(0)? == Some("on") {
                    return Err(Error::connect(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "database does not allow writes",
                    )));
                } else {
                    break;
                }
            }
        }
    }

    Ok((client, connection))
}

pub(crate) async fn connect_socket(
    addr: &Addr,
    port: u16,
    #[cfg_attr(not(target_os = "linux"), allow(unused_variables))] tcp_user_timeout: Option<
        Duration,
    >,
    keepalive_config: Option<&KeepaliveConfig>,
) -> Result<PgMaybeTlsStream> {
    match addr {
        Addr::Tcp(ip) => {
            let stream = TcpStream::connect((*ip, port)).await?;
            stream.set_nodelay(true).map_err(Error::connect)?;
            let sock_ref = SockRef::from(&stream);
            #[cfg(target_os = "linux")]
            {
                sock_ref
                    .set_tcp_user_timeout(tcp_user_timeout)
                    .map_err(Error::connect)?;
            }

            if let Some(keepalive_config) = keepalive_config {
                sock_ref
                    .set_tcp_keepalive(&TcpKeepalive::from(keepalive_config))
                    .map_err(Error::connect)?;
            }

            Ok(PgMaybeTlsStream::from(stream))
        }
        #[cfg(unix)]
        Addr::Unix(dir) => {
            let path = dir.join(format!(".s.PGSQL.{}", port));
            let socket = UnixStream::connect(path).await?;
            Ok(PgMaybeTlsStream::Unix(socket))
        }
    }
}

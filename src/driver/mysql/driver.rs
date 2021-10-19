use crate::driver::connection::{Connection, DriverConnection};
use crate::Result;
use mysql_async::{Conn, Opts};
use std::future::Future;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct Driver {
    pub(super) connection: Arc<Mutex<Conn>>,
}

impl DriverConnection<&str> for Driver {
    type Output = impl Future<Output = Result<Self>>;

    fn create(dsn: &str) -> Self::Output {
        let opts = Opts::from_url(dsn);

        async move {
            let connection = Conn::new(opts?).await?;

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

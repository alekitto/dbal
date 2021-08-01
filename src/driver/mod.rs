use crate::driver::connection::Connection;
use crate::driver::statement::StatementExecuteResult;
use crate::{Parameters, Result};
use url::Url;

pub mod connection;
pub mod statement;
pub mod statement_result;

pub mod sqlite;

pub enum Driver {
    Sqlite(sqlite::driver::Driver),
}

impl Driver {
    pub async fn create<'a, T>(dsn: T) -> Result<Self>
    where
        T: Into<String>,
    {
        use connection::DriverConnection;

        let dsn = dsn.into();
        if dsn.starts_with("sqlite:") {
            let driver = sqlite::driver::Driver::create(dsn)?;
            return Ok(Self::Sqlite(driver));
        }

        let url = Url::parse(dsn.as_str())?;
        let driver = match url.scheme() {
            _ => unimplemented!(),
        };

        Ok(driver)
    }

    pub fn prepare<St: Into<String>>(&self, sql: St) -> Result<impl statement::Statement + '_> {
        match self {
            Self::Sqlite(driver) => driver.prepare(sql),
        }
    }

    /// Executes an SQL statement, returning a result set as a Statement object.
    pub fn query<St: Into<String>>(
        &self,
        sql: St,
        params: Parameters,
    ) -> StatementExecuteResult<impl statement_result::StatementResult> {
        match self {
            Self::Sqlite(driver) => driver.query(sql, params),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::driver::statement::Statement;
    use crate::driver::Driver;
    use crate::params;

    #[test]
    fn can_create_sqlite_connection() {
        let connection =
            tokio_test::block_on(Driver::create("sqlite://:memory:")).expect("Must be connected");

        let statement = connection.prepare("SELECT 1").expect("Prepare failed");
        let result = tokio_test::block_on(statement.execute(params![]));
        assert_eq!(result.is_ok(), true);
    }
}

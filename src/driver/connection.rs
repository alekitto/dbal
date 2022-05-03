use super::statement::Statement;
use crate::{Async, AsyncResult, Parameters, Result};
use std::future::Future;

pub(in crate::driver) trait DriverConnection<T>: Sized {
    type Output: Future<Output = Result<Self>>;

    /// Creates a new driver connection
    fn create(params: T) -> Self::Output;
}

pub trait Connection<'conn>: Send + Sync + 'conn
where
    <Self as Connection<'conn>>::Statement: Statement<'conn>,
{
    type Statement;

    /// Retrieves the server version (if any).
    fn server_version(&self) -> Async<Option<String>>;

    /// Prepares a statement for execution and returns a Statement object.
    fn prepare<St: Into<String>>(&'conn self, sql: St) -> Result<Self::Statement>;

    /// Executes an SQL statement, returning a result set as a Statement object.
    fn query<St: Into<String>>(
        &'conn self,
        sql: St,
        params: Parameters,
    ) -> AsyncResult<
        <<Self as Connection<'conn>>::Statement as super::statement::Statement>::StatementResult,
    > {
        let statement = self.prepare(sql);
        if let Err(e) = statement {
            return Box::pin(async move { Err(e) });
        }

        let statement = statement.unwrap();
        statement.query_owned(Vec::from(params))
    }
}

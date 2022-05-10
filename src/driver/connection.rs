use super::statement::Statement;
use crate::platform::DatabasePlatform;
use crate::{Async, AsyncResult, EventDispatcher, Parameters, Result};
use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;

pub(in crate::driver) trait DriverConnection<T>: Sized {
    type Output: Future<Output = Result<Self>>;

    /// Creates a new driver connection
    fn create(params: T) -> Self::Output;
}

pub trait Connection<'conn>: Debug + Send + Sync + 'conn
where
    <Self as Connection<'conn>>::Statement: Statement<'conn>,
{
    type Statement;

    fn create_platform(
        &self,
        ev: Arc<EventDispatcher>,
    ) -> Async<Box<dyn DatabasePlatform + Send + Sync>>;

    /// Retrieves the server version (if any).
    fn server_version(&self) -> Async<Option<String>>;

    /// Prepares a statement for execution and returns a Statement object.
    fn prepare(&'conn self, sql: &str) -> Result<Self::Statement>;

    /// Executes an SQL statement, returning a result set as a Statement object.
    fn query(
        &'conn self,
        sql: &str,
        params: Parameters,
    ) -> AsyncResult<<<Self as Connection<'conn>>::Statement as Statement>::StatementResult> {
        let statement = self.prepare(sql);
        if let Err(e) = statement {
            return Box::pin(async move { Err(e) });
        }

        let statement = statement.unwrap();
        statement.query_owned(Vec::from(params))
    }
}

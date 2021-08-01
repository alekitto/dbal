use crate::driver::statement::StatementExecuteResult;
use crate::{Parameters, Result};

pub(in crate::driver) trait DriverConnection<T, S> {
    /// Creates a new driver connection
    fn create(params: T) -> Result<S>;
}

pub trait Connection<'conn>
where
    <Self as Connection<'conn>>::Statement: super::statement::Statement,
{
    type Statement;
    type StatementResult =
        <<Self as Connection<'conn>>::Statement as super::statement::Statement>::StatementResult;

    /// Prepares a statement for execution and returns a Statement object.
    fn prepare<St: Into<String>>(&'conn self, sql: St) -> Result<Self::Statement>;

    /// Executes an SQL statement, returning a result set as a Statement object.
    fn query<St: Into<String>>(
        &'conn self,
        sql: St,
        params: Parameters,
    ) -> StatementExecuteResult<Self::StatementResult>;
}

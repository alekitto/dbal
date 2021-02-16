use crate::{Parameters, Result};

pub(in crate::driver) trait DriverConnection<T, S> {
    /// Creates a new driver connection
    fn new(params: T) -> Result<S>;
}

pub trait Connection<'a, M> {
    /// Prepares a statement for execution and returns a Statement object.
    fn prepare<S: Into<String>>(&'a self, sql: S) -> Result<M>;

    /// Executes an SQL statement, returning a result set as a Statement object.
    fn query<S: Into<String>>(&'a mut self, sql: S, params: Parameters) -> Result<M>;
}

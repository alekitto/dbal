use crate::{Parameter, ParameterIndex, Parameters, Result};
use std::future::Future;
use std::pin::Pin;

pub type StatementExecuteResult<R> = Pin<Box<dyn Future<Output = Result<R>>>>;
pub trait Statement {
    type StatementResult;

    /// Binds a value to a corresponding named or positional placeholder in the SQL statement
    /// that was used to prepare the statement.
    ///
    /// * `param` Parameter identifier. For a prepared statement using named placeholders, this will
    ///           be a parameter name of the form :name. For a prepared statement using question
    ///           mark placeholders, this will be the 1-indexed position of the parameter.
    /// * `value` The value to bind to the parameter.
    fn bind_value(&self, param: ParameterIndex, value: Parameter) -> Result<()>;

    /// Executes a prepared statement
    ///
    /// * `params` A vector of values with as many elements as there are bound parameters in the
    ///            SQL statement being executed.
    fn execute(&self, params: Parameters) -> StatementExecuteResult<Self::StatementResult>
    where
        Self: Sized;

    /// Returns the number of rows affected by the last DELETE, INSERT, or UPDATE statement
    /// executed by the corresponding object.
    ///
    /// If the last SQL statement executed by the associated Statement object was a SELECT statement,
    /// some databases may return the number of rows returned by that statement. However,
    /// this behaviour is not guaranteed for all databases and should not be
    /// relied on for portable applications.
    fn row_count(&self) -> usize;
}

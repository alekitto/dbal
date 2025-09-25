use crate::driver::statement_result::StatementResult;
use crate::{AsyncResult, Parameter, ParameterIndex, Parameters, Result};
use delegate::delegate;
use std::fmt::Debug;

pub trait Statement<'conn>: Debug + Send + Sync {
    /// Binds a value to a corresponding named or positional placeholder in the SQL statement
    /// that was used to prepare the statement.
    ///
    /// * `param` Parameter identifier. For a prepared statement using named placeholders, this will
    ///   be a parameter name of the form :name. For a prepared statement using question
    ///   mark placeholders, this will be the 1-indexed position of the parameter.
    /// * `value` The value to bind to the parameter.
    fn bind_value(&self, param: ParameterIndex, value: Parameter) -> Result<()>;

    /// Executes a prepared statement and returns the resulting rows.
    ///
    /// * `params` A vector of values with as many elements as there are bound parameters in the
    ///   SQL statement being executed.
    fn query(&self, params: Parameters) -> AsyncResult<'_, StatementResult>;

    /// Executes a prepared statement and returns the resulting rows.
    /// This method consumes the statement.
    ///
    /// * `params` A vector of values with as many elements as there are bound parameters in the
    ///   SQL statement being executed.
    fn query_owned(
        self: Box<Self>,
        params: Vec<(ParameterIndex, Parameter)>,
    ) -> AsyncResult<'conn, StatementResult>;

    /// Executes a prepared statement
    ///
    /// * `params` A vector of values with as many elements as there are bound parameters in the
    ///   SQL statement being executed.
    fn execute(&self, params: Parameters) -> AsyncResult<'_, usize>;

    /// Executes a prepared statement.
    /// This method consumes the statement.
    ///
    /// * `params` A vector of values with as many elements as there are bound parameters in the
    ///   SQL statement being executed.
    fn execute_owned(
        self: Box<Self>,
        params: Vec<(ParameterIndex, Parameter)>,
    ) -> AsyncResult<'conn, usize>;

    /// Returns the number of rows affected by the last DELETE, INSERT, or UPDATE statement
    /// executed by the corresponding object.
    ///
    /// If the last SQL statement executed by the associated Statement object was a SELECT statement,
    /// some databases may return the number of rows returned by that statement. However,
    /// this behaviour is not guaranteed for all databases and should not be
    /// relied on for portable applications.
    fn row_count(&self) -> usize;
}

impl<'conn, T: Statement<'conn> + ?Sized> Statement<'conn> for Box<T> {
    delegate! {
        to (**self) {
            fn bind_value(&self, param: ParameterIndex, value: Parameter) -> Result<()>;
            fn query(&self, params: Parameters) -> AsyncResult<'_, StatementResult>;
            fn execute(&self, params: Parameters) -> AsyncResult<'_, usize>;
            fn row_count(&self) -> usize;
        }

        to (*self) {
            fn query_owned(
                self: Box<Self>,
                params: Vec<(ParameterIndex, Parameter)>,
            ) -> AsyncResult<'conn, StatementResult>;
            fn execute_owned(
                self: Box<Self>,
                params: Vec<(ParameterIndex, Parameter)>,
            ) -> AsyncResult<'conn, usize>;
        }
    }
}

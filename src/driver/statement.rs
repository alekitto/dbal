use crate::{Parameter, ParameterIndex, Result, Row, Parameters};

pub trait Statement<'s> {
    /// Binds a value to a corresponding named or positional placeholder in the SQL statement
    /// that was used to prepare the statement.
    ///
    /// * `param` Parameter identifier. For a prepared statement using named placeholders, this will
    ///           be a parameter name of the form :name. For a prepared statement using question
    ///           mark placeholders, this will be the 1-indexed position of the parameter.
    /// * `value` The value to bind to the parameter.
    fn bind_value(&mut self, param: ParameterIndex, value: Parameter) -> Result<()>;

    /// Executes a prepared statement
    ///
    /// * `params` A vector of values with as many elements as there are bound parameters in the
    ///            SQL statement being executed.
    fn execute(&mut self, params: Parameters) -> Result<()>
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

    /// Returns the *NEXT* row of the statement if any.
    /// If the iterator has been consumed fully, [None] is returned.
    fn fetch_one(&'s mut self) -> Result<Option<Row>>;

    /// Returns all the *REMAINING* rows of the statement if any.
    /// Builds and return a vector of Row objects which can be queried to get the data.
    ///
    /// If the iterator has been consumed partly, only the remaining rows are collected
    /// and returned into the vector.
    /// Consequently, if the statement has been fetched fully, an empty vector is returned.
    fn fetch_all(&'s mut self) -> Result<Vec<Row>>;

    /// Returns the number of columns in the result set
    /// If there is no result set, 0 is returned
    fn column_count(&self) -> usize;
}

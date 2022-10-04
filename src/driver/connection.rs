use super::statement::Statement;
use crate::driver::statement_result::StatementResult;
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

pub trait Connection<'conn>: Debug + Send + Sync + 'conn {
    fn create_platform(
        &self,
        ev: Arc<EventDispatcher>,
    ) -> Async<Box<dyn DatabasePlatform + Send + Sync>>;

    /// Retrieves the server version (if any).
    fn server_version(&self) -> Async<Option<String>>;

    /// Prepares a statement for execution and returns a Statement object.
    fn prepare(&'conn self, sql: &str) -> Result<Box<dyn Statement + 'conn>>;

    /// Executes an SQL statement, returning a result set as a Statement object.
    fn query(&'conn self, sql: &str, params: Parameters) -> AsyncResult<StatementResult> {
        let statement = self.prepare(sql);
        if let Err(e) = statement {
            return Box::pin(async move { Err(e) });
        }

        let statement = statement.unwrap();
        Box::new(statement).query_owned(Vec::from(params))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::driver::connection::Connection;
    use crate::driver::statement::Statement;
    use crate::platform::tests::MockPlatform;
    use crate::platform::DatabasePlatform;
    use crate::{Async, EventDispatcher};
    use std::fmt::{Debug, Formatter};
    use std::sync::Arc;

    pub struct MockConnection {}

    impl Debug for MockConnection {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("MockConnection").finish()
        }
    }

    impl<'conn> Connection<'conn> for MockConnection {
        fn create_platform(
            &self,
            ev: Arc<EventDispatcher>,
        ) -> Async<Box<dyn DatabasePlatform + Send + Sync>> {
            Box::pin(async move {
                Box::new(MockPlatform { ev }) as Box<dyn DatabasePlatform + Send + Sync>
            })
        }

        fn server_version(&self) -> Async<Option<String>> {
            Box::pin(async { None })
        }

        fn prepare(&'conn self, sql: &str) -> crate::Result<Box<dyn Statement + 'conn>> {
            todo!()
        }
    }
}

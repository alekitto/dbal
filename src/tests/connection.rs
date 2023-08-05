use crate::driver::connection::Connection as ConnectionTrait;
use crate::driver::statement::Statement;
use crate::platform::DatabasePlatform;
use crate::tests::MockPlatform;
use crate::{Async, Connection, EventDispatcher, Result};
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::sync::Arc;

pub fn get_database_dsn() -> String {
    std::env::var("DATABASE_DSN").unwrap()
}

pub fn create_connection() -> impl Future<Output = Result<Connection>> {
    Connection::create_from_dsn(&get_database_dsn(), None, None)
        .unwrap()
        .connect()
}

pub struct MockConnection {}

impl Debug for MockConnection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MockConnection").finish()
    }
}

impl<'conn> ConnectionTrait<'conn> for MockConnection {
    fn create_platform(
        &self,
        ev: Arc<EventDispatcher>,
    ) -> Async<Box<dyn DatabasePlatform + Send + Sync>> {
        Box::pin(
            async move { Box::new(MockPlatform { ev }) as Box<dyn DatabasePlatform + Send + Sync> },
        )
    }

    fn server_version(&self) -> Async<Option<String>> {
        Box::pin(async { None })
    }

    fn prepare(&'conn self, _: &str) -> crate::Result<Box<dyn Statement + 'conn>> {
        unreachable!()
    }
}

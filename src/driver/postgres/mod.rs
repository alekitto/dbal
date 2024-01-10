mod connect;
pub(in crate::driver) mod driver;
mod keepalive;
pub(in crate::driver) mod platform;
pub(in crate::driver) mod rows;
pub(in crate::driver) mod statement;

pub use driver::ConnectionOptions;
pub use platform::{
    AbstractPostgreSQLPlatform, AbstractPostgreSQLSchemaManager, PostgreSQLPlatform,
    PostgreSQLSchemaManager,
};

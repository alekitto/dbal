pub(in crate::driver) mod driver;
pub(in crate::driver) mod platform;
pub(in crate::driver) mod rows;
pub(in crate::driver) mod statement;

pub use driver::ConnectionOptions;
pub use platform::{
    AbstractMySQLPlatform, AbstractMySQLSchemaManager, MySQLPlatform, MySQLSchemaManager,
    MySQLVariant,
};

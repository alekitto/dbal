mod comparator;
mod mysql_platform;
mod schema_manager;

#[derive(Copy, Clone)]
pub enum MySQLVariant {
    MySQL5_6,
    MySQL5_7,
    MySQL8_0,
    MariaDB,
}

pub mod mariadb;
pub mod mysql;

pub use comparator::MySQLComparator;
pub use mysql_platform::{AbstractMySQLPlatform, MySQLPlatform};
pub use schema_manager::{AbstractMySQLSchemaManager, MySQLSchemaManager};

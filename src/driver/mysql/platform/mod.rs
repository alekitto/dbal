mod mysql_platform;
mod schema_manager;

#[derive(Copy, Clone)]
pub enum MySQLVariant {
    MySQL,
    MySQL80,
    MariaDB,
}

pub mod mariadb;
pub mod mysql;

pub use mysql_platform::{AbstractMySQLPlatform, MySQLPlatform};
pub use schema_manager::{AbstractMySQLSchemaManager, MySQLSchemaManager};

mod mysql_platform;

pub enum MySQLVariant {
    MySQL,
    MySQL80,
    MariaDB,
}

pub mod mariadb;
pub mod mysql;

pub use mysql_platform::AbstractMySQLPlatform;
pub(super) use mysql_platform::MySQLPlatform;

# Creed - Extensible Database Abstraction Layer Library

`Creed` is a Database Abstraction Layer for Rust for SQL databases
with built-in drivers for the most popular RDBMS (mysql, postgresql, sqlite)
and with extensibility in mind, which means that you can always use a custom
driver for a currently unsupported db engine or even for your custom
database engine.

A DSN (connection string) can be used to set all the connection options,
meaning that changing a simple environment variable is sufficient to switch the
whole DB driver.

## Connection examples

```rust
use creed::{Connection, Result};
async fn connect_database(dsn: &str) -> Result<Connection> {
    let connection = Connection::create_from_dsn(dsn, None, None)?;
    connection.connect().await
}

connect_database("postgresql://postgres@localhost:5432/database_name"); // Will connect to postgres
connect_database("mysql://root@localhost:3306/my_database"); // Will connect to mysql
```

## Schema Manager

`Creed` is built with a schema manager system which helps you to reliably
maintain a database on multiple RDBMS.
[`platform::DatabasePlatform`] and [`schema::SchemaManager`]
traits are built to help you to write platform-independent code to generate
correct SQL queries and schema DDL commands on different platforms.

## Event Dispatcher

Events are used to allow you to intercept and handle complex operations
such as executing a set of defined queries on connection or generate custom
SQL when an ALTER TABLE is issued by the schema manager.
See [`EventDispatcher`] for further information.

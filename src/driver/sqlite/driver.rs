use crate::driver::connection::{Connection as DbalConnection, DriverConnection};
use crate::driver::sqlite;
use crate::driver::sqlite::platform::SQLitePlatform;
use crate::driver::statement::Statement;
use crate::platform::DatabasePlatform;
use crate::{Async, EventDispatcher, Parameter, Result, Value};
use itertools::Itertools;
use rusqlite::functions::{Context, FunctionFlags};
use rusqlite::types::ToSqlOutput;
use rusqlite::ToSql;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::sync::Arc;
use url::Url;

pub type Udf = dyn FnMut(&Context) -> rusqlite::Result<Box<dyn ToSql>>
    + Sync
    + Send
    + std::panic::UnwindSafe
    + 'static;

pub struct ConnectionOptions {
    path: Option<String>,
    memory: bool,
    user_defined_functions: HashMap<&'static str, (isize, Box<Udf>)>,
}

impl From<&crate::ConnectionOptions> for ConnectionOptions {
    fn from(opts: &crate::ConnectionOptions) -> Self {
        Self {
            path: opts.file_path.as_ref().cloned().map(String::from),
            memory: opts
                .host
                .as_ref()
                .map(|h| h.eq(":memory:"))
                .unwrap_or(false),
            user_defined_functions: ConnectionOptions::builtin_user_defined_functions(),
        }
    }
}

impl Default for ConnectionOptions {
    fn default() -> Self {
        Self {
            path: None,
            memory: true,
            user_defined_functions: Self::builtin_user_defined_functions(),
        }
    }
}

impl ConnectionOptions {
    pub fn new<T: Into<String>>(dsn: T) -> Result<Self> {
        let dsn = dsn.into();
        if !dsn.starts_with("sqlite:") {
            return Ok(Self::new_with_path(dsn));
        }

        if dsn.starts_with("sqlite://:memory:") {
            return Ok(Self::new_from_memory());
        }

        let url = Url::parse(dsn.as_str())?;
        let path = url.path();

        let mut target = url.domain().unwrap_or("");
        if target.is_empty() {
            target = path;
        }

        Ok(Self::new_with_path(target))
    }

    pub fn new_with_path<T: Into<String>>(path: T) -> Self {
        ConnectionOptions {
            path: Some(path.into()),
            memory: false,
            ..Self::default()
        }
    }

    pub fn new_from_memory() -> Self {
        ConnectionOptions {
            path: None,
            memory: true,
            ..Self::default()
        }
    }

    fn builtin_user_defined_functions() -> HashMap<&'static str, (isize, Box<Udf>)> {
        let mut hashmap: HashMap<&str, (isize, Box<Udf>)> = HashMap::new();
        hashmap.insert(
            "sqrt",
            (
                1,
                Box::new(|context: &Context| {
                    let value = context.get::<f64>(0)?;
                    Ok(Box::new(value.sqrt()))
                }),
            ),
        );

        hashmap.insert(
            "mod",
            (
                2,
                Box::new(|context: &Context| {
                    let a = context.get::<i64>(0)?;
                    let b = context.get::<i64>(1)?;

                    Ok(Box::new(a % b))
                }),
            ),
        );

        hashmap.insert(
            "locate",
            (
                -1,
                Box::new(|context: &Context| {
                    let substr = context.get::<String>(0)?;
                    let mut str = context.get::<String>(1)?;
                    let mut offset = if context.len() > 2 {
                        context.get::<i32>(2)? as usize
                    } else {
                        0
                    };

                    // SQL's LOCATE function works on 1-based positions, so we have to make them compatible if an offset is given.
                    if offset > 0 {
                        offset -= 1;
                        str = str[offset..].to_string();
                    }

                    let pos = str.find(&substr);
                    Ok(Box::new(if let Some(p) = pos { p as i32 + 1 } else { 0 }))
                }),
            ),
        );

        hashmap
    }

    pub fn add_user_defined_function(
        &mut self,
        name: &'static str,
        num_arguments: isize,
        func: Box<Udf>,
    ) {
        self.user_defined_functions
            .insert(name, (num_arguments, func));
    }
}

pub(crate) struct ConnectionWrapper(pub(crate) rusqlite::Connection);
unsafe impl Sync for ConnectionWrapper {}

pub struct Driver {
    pub(crate) connection: ConnectionWrapper,
}

impl Debug for Driver {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Driver (SQLite)").finish_non_exhaustive()
    }
}

impl DriverConnection<ConnectionOptions> for Driver {
    type Output = impl Future<Output = Result<Self>>;

    fn create(params: ConnectionOptions) -> Self::Output {
        async move {
            let connection = if params.memory {
                rusqlite::Connection::open_in_memory()
            } else {
                rusqlite::Connection::open(params.path.unwrap())
            }?;

            for (name, (num_args, cb)) in params.user_defined_functions.into_iter() {
                connection.create_scalar_function(
                    name,
                    num_args as i32,
                    FunctionFlags::default(),
                    cb,
                )?;
            }

            Ok(Driver {
                connection: ConnectionWrapper(connection),
            })
        }
    }
}

impl<'conn> DbalConnection<'conn> for Driver {
    fn create_platform(
        &self,
        ev: Arc<EventDispatcher>,
    ) -> Async<Box<dyn DatabasePlatform + Send + Sync>> {
        Box::pin(async move {
            Box::new(SQLitePlatform::new(ev)) as Box<(dyn DatabasePlatform + Send + Sync)>
        })
    }

    fn server_version(&self) -> Async<Option<String>> {
        Box::pin(async move { None })
    }

    fn prepare(&'conn self, sql: &str) -> Result<Box<dyn Statement + 'conn>> {
        Ok(Box::new(sqlite::statement::Statement::new(self, sql)?))
    }
}

impl ToSql for Parameter {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>, rusqlite::Error> {
        self.value.to_sql()
    }
}

impl ToSql for Value {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>, rusqlite::Error> {
        Ok(match self {
            Value::NULL => ToSqlOutput::from(rusqlite::types::Null {}),
            Value::Int(value) => ToSqlOutput::from(*value),
            Value::UInt(value) => ToSqlOutput::from(*value as i64),
            Value::String(value) => ToSqlOutput::from(value.clone()),
            Value::Bytes(value) => ToSqlOutput::from(value.clone()),
            Value::Float(value) => ToSqlOutput::from(*value),
            Value::Boolean(value) => ToSqlOutput::from(*value),
            Value::Array(value) => {
                let vec = value.iter().map(ToString::to_string).join(",");
                ToSqlOutput::from(vec)
            }
            Value::DateTime(value) => ToSqlOutput::Owned(rusqlite::types::Value::Text(
                value.clone().format("%+").to_string(),
            )),
            Value::Json(value) => {
                ToSqlOutput::Owned(rusqlite::types::Value::Text(value.to_string()))
            }
            Value::Uuid(value) => ToSqlOutput::from(value.to_string()),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::driver::connection::Connection;
    use crate::driver::sqlite::driver::{Driver, DriverConnection};
    use crate::driver::sqlite::ConnectionOptions;
    use crate::driver::statement::Statement;
    use crate::params;
    use crate::{Result, Row, Value};
    use std::fs::remove_file;

    #[test]
    fn can_connect() {
        let result = tokio_test::block_on(Driver::create(ConnectionOptions::new_from_memory()));
        assert_eq!(true, result.is_ok());

        let mut file = std::env::temp_dir();
        file.push("test_temp_db.sqlite");

        let result = tokio_test::block_on(Driver::create(ConnectionOptions::new_with_path(
            file.to_str().unwrap(),
        )));
        assert_eq!(true, result.is_ok());

        #[allow(unused_must_use)]
        {
            remove_file(file.to_str().unwrap());
        }
    }

    #[test]
    fn can_prepare_statements() {
        let connection = tokio_test::block_on(Driver::create(ConnectionOptions::new_from_memory()))
            .expect("Must be connected");

        let statement = connection.prepare("SELECT 1");
        assert_eq!(statement.is_ok(), true);
        let statement = connection.prepare("NOT_A_COMMAND 1");
        assert_eq!(statement.is_ok(), false);
    }

    #[test]
    fn can_execute_statements() {
        let connection = tokio_test::block_on(Driver::create(ConnectionOptions::new_from_memory()))
            .expect("Must be connected");

        let statement = connection.prepare("SELECT 1").expect("Prepare failed");
        let result = tokio_test::block_on(statement.execute(params![]));
        assert_eq!(result.is_ok(), true);
    }

    #[tokio::test]
    async fn can_fetch_statements() -> Result<()> {
        let connection = Driver::create(ConnectionOptions::new_from_memory())
            .await
            .expect("Must be connected");

        let statement = connection.prepare("SELECT 1").expect("Prepare failed");
        let result = statement
            .query(params![])
            .await
            .expect("Execution succeeds");

        let rows = result.fetch_all().await?;

        assert_eq!(rows.len(), 1);
        assert_eq!(
            &Row::new(vec!["1".to_string()], vec![Value::Int(1)]),
            rows.get(0).unwrap(),
        );

        // Re-execute
        let mut result = statement
            .query(params![])
            .await
            .expect("Execution succeeds");

        let row = result.fetch_one().await?;
        assert_eq!(
            Row::new(vec!["1".to_string()], vec![Value::Int(1)]),
            row.unwrap(),
        );

        let row = result.fetch_one().await?;
        assert_eq!(row.is_none(), true);

        Ok(())
    }

    #[tokio::test]
    async fn builtin_udf_should_be_added() -> Result<()> {
        let connection = Driver::create(ConnectionOptions::new_from_memory())
            .await
            .expect("Must be connected");

        let statement = connection
            .query("SELECT sqrt(2)", params![])
            .await
            .expect("Query must succeed");
        let rows = statement.fetch_all().await?;
        assert_eq!(
            rows.get(0).unwrap(),
            &Row::new(
                vec!["sqrt(2)".to_string()],
                vec![Value::Float(std::f64::consts::SQRT_2)]
            )
        );

        let statement = connection
            .query("SELECT mod(17, 3)", params![])
            .await
            .expect("Query must succeed");
        let rows = statement.fetch_all().await?;
        assert_eq!(
            rows.get(0).unwrap(),
            &Row::new(vec!["mod(17, 3)".to_string()], vec![Value::Int(2)])
        );

        let statement = connection
            .query(
                "SELECT LOCATE('3', 'W3Schools.com') AS MatchPosition",
                params![],
            )
            .await
            .expect("Query must succeed");
        let rows = statement.fetch_all().await?;
        assert_eq!(
            rows.get(0).unwrap(),
            &Row::new(vec!["MatchPosition".to_string()], vec![Value::Int(2)])
        );

        let statement = connection
            .query(
                "SELECT LOCATE('o', 'W3Schools.com', 3) AS MatchPosition",
                params![],
            )
            .await
            .expect("Query must succeed");
        let rows = statement.fetch_all().await?;
        assert_eq!(
            rows.get(0).unwrap(),
            &Row::new(vec!["MatchPosition".to_string()], vec![Value::Int(4)])
        );

        let statement = connection
            .query(
                "SELECT LOCATE('3', 'W3Schools.com', 3) AS MatchPosition",
                params![],
            )
            .await
            .expect("Query must succeed");
        let rows = statement.fetch_all().await?;
        assert_eq!(
            rows.get(0).unwrap(),
            &Row::new(vec!["MatchPosition".to_string()], vec![Value::Int(0)])
        );

        Ok(())
    }
}

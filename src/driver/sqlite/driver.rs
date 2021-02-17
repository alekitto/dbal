use crate::driver::connection::{Connection as DbalConnection, DriverConnection};
use crate::driver::sqlite;
use crate::driver::statement::Statement;
use crate::{Parameter, Parameters, Result, Value};
use rusqlite::functions::{Context, FunctionFlags};
use rusqlite::types::ToSqlOutput;
use rusqlite::ToSql;
use std::collections::HashMap;
use url::Url;

type Udf = dyn FnMut(&Context) -> rusqlite::Result<Box<dyn ToSql>>
    + Send
    + std::panic::UnwindSafe
    + 'static;

pub struct ConnectionOptions {
    path: Option<String>,
    memory: bool,
    user_defined_functions: HashMap<&'static str, (isize, Box<Udf>)>,
}

impl ConnectionOptions {
    fn new<T: Into<String>>(dsn: T) -> Result<Self> {
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
        if target == "" {
            target = path;
        }

        Ok(Self::new_with_path(target))
    }

    fn new_with_path<T: Into<String>>(path: T) -> Self {
        ConnectionOptions {
            path: Some(path.into()),
            memory: false,
            ..Self::default()
        }
    }

    fn new_from_memory() -> Self {
        ConnectionOptions {
            path: None,
            memory: true,
            ..Self::default()
        }
    }

    fn default() -> Self {
        ConnectionOptions {
            path: None,
            memory: true,
            user_defined_functions: Self::builtin_user_defined_functions(),
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

                    // SQL's LOCATE function works on 1-based positions, while PHP's strpos works on 0-based positions.
                    // So we have to make them compatible if an offset is given.
                    if offset > 0 {
                        offset -= 1;
                        str = str[offset..].to_string();
                    }

                    let pos = str.find(&substr);
                    Ok(Box::new(if pos.is_some() {
                        pos.unwrap() as i32 + 1
                    } else {
                        0
                    }))
                }),
            ),
        );

        hashmap
    }

    pub fn add_user_defined_function(&mut self, name: &'static str, num_arguments: isize, func: Box<Udf>) -> () {
        self.user_defined_functions.insert(name, (num_arguments, func));
    }
}

pub struct Driver {
    pub(in crate::driver::sqlite) connection: rusqlite::Connection,
}

impl DriverConnection<ConnectionOptions, Driver> for Driver {
    fn new(params: ConnectionOptions) -> Result<Driver> {
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

        Ok(Driver { connection })
    }
}

impl<T> DriverConnection<T, Driver> for Driver
where
    T: Into<String>,
{
    fn new(params: T) -> Result<Driver> {
        Self::new(ConnectionOptions::new(params)?)
    }
}

impl<'a> DbalConnection<'a, sqlite::statement::Statement<'a>> for Driver {
    fn prepare<S: Into<String>>(&'a self, sql: S) -> Result<sqlite::statement::Statement<'a>> {
        sqlite::statement::Statement::new(self, sql.into().as_str())
    }

    fn query<S: Into<String>>(
        &'a self,
        sql: S,
        params: Parameters,
    ) -> Result<sqlite::statement::Statement<'a>> {
        let mut statement = self.prepare(sql)?;
        statement.execute(params)?;

        Ok(statement)
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
            Value::DateTime(value) => ToSqlOutput::Owned(rusqlite::types::Value::Text(
                value.clone().format("%+").to_string(),
            )),
            Value::Json(value) => {
                ToSqlOutput::Owned(rusqlite::types::Value::Text(value.to_string()))
            }
            Value::Uuid(value) => ToSqlOutput::from(value.clone()),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::driver::connection::Connection;
    use crate::driver::sqlite::driver::{Driver, DriverConnection};
    use crate::driver::statement::Statement;
    use std::fs::remove_file;
    use crate::{Row, Value};
    use crate::params;

    #[test]
    fn can_connect() {
        let result = Driver::new("sqlite://:memory:");
        assert_eq!(true, result.is_ok());

        let mut file = std::env::temp_dir();
        file.push("test_temp_db.sqlite");

        let result = Driver::new(format!("sqlite://{}", file.to_str().unwrap()));
        assert_eq!(true, result.is_ok());

        #[allow(unused_must_use)]
        {
            remove_file(file.to_str().unwrap());
        }
    }

    #[test]
    fn can_prepare_statements() {
        let connection = Driver::new("sqlite://:memory:").expect("Must be connected");

        let statement = connection.prepare("SELECT 1");
        assert_eq!(statement.is_ok(), true);
        let statement = connection.prepare("NOT_A_COMMAND 1");
        assert_eq!(statement.is_ok(), false);
    }

    #[test]
    fn can_execute_statements() {
        let connection = Driver::new("sqlite://:memory:").expect("Must be connected");

        let mut statement = connection.prepare("SELECT 1").expect("Prepare failed");
        let result = statement.execute(params![]);
        assert_eq!(result.is_ok(), false);
    }

    #[test]
    fn builtin_udf_should_be_added() {
        let connection = &mut Driver::new("sqlite://:memory:").expect("Must be connected");

        let mut statement = connection.query("SELECT sqrt(2)", params![]).expect("Query must succeed");
        let rows = statement.fetch_all().expect("Fetch must succeed");
        assert_eq!(
            *rows.get(0).unwrap(),
            Row::new(vec!["sqrt(2)".to_string()], vec![Value::Float(std::f64::consts::SQRT_2)])
        );

        let mut statement = connection.query("SELECT mod(17, 3)", params![]).expect("Query must succeed");
        let rows = statement.fetch_all().expect("Fetch must succeed");
        assert_eq!(
            *rows.get(0).unwrap(),
            Row::new(vec!["mod(17, 3)".to_string()], vec![Value::Int(2)])
        );

        let mut statement = connection.query("SELECT LOCATE('3', 'W3Schools.com') AS MatchPosition", params![]).expect("Query must succeed");
        let rows = statement.fetch_all().expect("Fetch must succeed");
        assert_eq!(
            *rows.get(0).unwrap(),
            Row::new(vec!["MatchPosition".to_string()], vec![Value::Int(2)])
        );

        let mut statement = connection.query("SELECT LOCATE('o', 'W3Schools.com', 3) AS MatchPosition", params![]).expect("Query must succeed");
        let rows = statement.fetch_all().expect("Fetch must succeed");
        assert_eq!(
            *rows.get(0).unwrap(),
            Row::new(vec!["MatchPosition".to_string()], vec![Value::Int(4)])
        );

        let mut statement = connection.query("SELECT LOCATE('3', 'W3Schools.com', 3) AS MatchPosition", params![]).expect("Query must succeed");
        let rows = statement.fetch_all().expect("Fetch must succeed");
        assert_eq!(
            *rows.get(0).unwrap(),
            Row::new(vec!["MatchPosition".to_string()], vec![Value::Int(0)])
        );
    }
}

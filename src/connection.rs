use crate::driver::statement::Statement;
use crate::driver::statement_result::StatementResult;
use crate::driver::Driver;
use crate::event::ConnectionEvent;
use crate::parameter::{IntoParameters, NO_PARAMS};
use crate::platform::DatabasePlatform;
use crate::r#type::IntoType;
use crate::schema::SchemaManager;
use crate::util::PlatformBox;
use crate::{
    params, Configuration, ConnectionOptions, Error, EventDispatcher, Parameters, Result, Row,
    TypedValueMap, Value,
};
use itertools::Itertools;
use log::debug;
use std::io::Read;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// The main database connection struct.
///
/// `Connection` is the main entry point for `Creed`.
/// It serves as a central point of control for the database connection,
/// handling configuration options, the database platform object,
/// the event manager and, obviously, the real database connection driver.
///
/// Connection creation is a synchronous operation as it DOES NOT
/// immediately connect to the database, but initializes the environment to
/// do so in the `connect` method.
/// Please note that `connect` currently consumes the connection returning
/// a (future) result which contains Self in case everything's ok or an
/// non-recoverable error.
/// On error the connection is gone and must be re-initialized before
/// retrying a connection.
#[derive(Debug)]
pub struct Connection {
    connection_options: ConnectionOptions,
    configuration: Arc<Configuration>,
    driver: Option<Arc<Driver>>,
    platform: Option<PlatformBox>,
    event_manager: Arc<EventDispatcher>,
    transaction_nesting_level: AtomicUsize,
}

impl Connection {
    /// Creates a connection with the given connection options.
    ///
    /// If the `event_manager` parameter is `None`, a new [`EventDispatcher`] object
    /// is created and used. You can access the event manager through the
    /// [`Connection::get_event_manager`] method.
    pub fn create(
        connection_options: ConnectionOptions,
        configuration: Option<Configuration>,
        event_manager: Option<EventDispatcher>,
    ) -> Self {
        let connection_options = Self::add_database_suffix(connection_options);
        let platform = connection_options.platform.clone();
        let event_manager = platform
            .as_ref()
            .map(|platform| platform.get_event_manager())
            .unwrap_or_else(|| Arc::new(event_manager.unwrap_or_default()));

        Self {
            connection_options,
            configuration: Arc::new(configuration.unwrap_or_default()),
            driver: None,
            platform,
            event_manager,
            transaction_nesting_level: AtomicUsize::default(),
        }
    }

    /// Creates a new connection object parsing a DSN string.
    /// The matching connection driver will be selected based on the DSN.
    pub fn create_from_dsn(
        dsn: &str,
        configuration: Option<Configuration>,
        event_manager: Option<EventDispatcher>,
    ) -> Result<Self> {
        Ok(Self::create(
            ConnectionOptions::try_from(dsn)?,
            configuration,
            event_manager,
        ))
    }

    /// Creates a new [`Connection`] object with an already established connection
    pub async fn create_with_connection(
        connection: Box<dyn for<'a> crate::driver::connection::Connection<'a>>,
        configuration: Option<Configuration>,
        event_manager: Option<EventDispatcher>,
    ) -> Result<Self> {
        let event_manager = Arc::new(event_manager.unwrap_or_default());

        let platform = Arc::new(connection.create_platform(event_manager.clone()).await);
        let driver = Arc::new(Driver::create_with_connection(connection));

        Ok(Self {
            connection_options: ConnectionOptions::default(),
            configuration: Arc::new(configuration.unwrap_or_default()),
            platform: Some(platform),
            driver: Some(driver),
            event_manager,
            transaction_nesting_level: AtomicUsize::default(),
        })
    }

    /// Whether the connection is active.
    pub fn is_connected(&self) -> bool {
        self.driver.is_some()
    }

    /// Gets the options this connections has been generated from.
    pub fn get_params(&self) -> &ConnectionOptions {
        &self.connection_options
    }

    /// Returns a pointer to the connection configuration for this connection.
    pub fn get_configuration(&self) -> Arc<Configuration> {
        self.configuration.clone()
    }

    /// Returns a pointer to the event manager for this connection.
    pub fn get_event_manager(&self) -> Arc<EventDispatcher> {
        self.event_manager.clone()
    }

    /// Creates a new schema manager.
    ///
    /// # Errors
    ///
    /// The connection must be connected to the server for this method to succeed.
    /// Otherwise will return a "Not Connected" Error.
    pub fn create_schema_manager(&self) -> Result<Box<dyn SchemaManager + '_>> {
        self.get_platform()
            .map(|platform| platform.create_schema_manager(self))
    }

    /// Returns the database platform.
    ///
    /// # Errors
    ///
    /// The connection must be connected to the server for this method to succeed.
    /// Otherwise will return a "Not Connected" Error.
    pub fn get_platform(&self) -> Result<PlatformBox> {
        self.platform.clone().ok_or_else(Error::not_connected)
    }

    /// Gets the name of the currently selected database.
    ///
    /// The name of the database or `None` if a database is not selected.
    /// The platforms which don't support the concept of a database (e.g. embedded databases)
    /// MUST always return a string as an indicator of an implicitly selected database.
    pub async fn get_database(&self) -> Option<String> {
        if let Some(platform) = self.platform.clone() {
            let query =
                platform.get_dummy_select_sql(Some(&platform.get_current_database_expression()));
            self.query(query, params!())
                .await
                .ok()?
                .fetch_one()
                .await
                .ok()?
                .and_then(|row| row.get(0).cloned().ok())
                .and_then(|v| match v {
                    Value::String(res) => Some(res),
                    Value::Bytes(v) => {
                        let mut s = String::new();
                        v.as_slice().read_to_string(&mut s).ok().map(|_| s)
                    }
                    _ => None,
                })
        } else {
            None
        }
    }

    /// Initiate the connection to the SQL server.
    ///
    /// # Events
    ///
    /// A [`ConnectionEvent`](ConnectionEvent) (async) event is fired and dispatched
    /// through the event manager.
    ///
    /// # Errors
    ///
    /// An error is returned:
    /// - if an unknown driver is requested (database engine not supported)
    /// - if a not compiled driver is requested (excluded via feature flags)
    /// - if the connection fails
    ///
    /// # Notes
    ///
    /// This method consumes the connection object, returning an Error if the connection
    /// cannot be established.
    /// In case of success, the self object is returned with the established driver
    /// connection and the initialized platform object.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use creed::Connection;
    ///
    /// async fn make_connection() -> Result<Connection, creed::Error> {
    ///     let connection = Connection::create_from_dsn(&std::env::var("DATABASE_DSN")?, None, None)?
    ///         .connect()
    ///         .await?;
    ///
    ///     Ok(connection)
    /// }
    /// ```
    pub async fn connect(mut self) -> Result<Self> {
        if self.driver.is_some() {
            return Ok(self);
        }

        let driver = Arc::new(Driver::create(&self.connection_options).await?);
        if self.platform.is_none() {
            let platform = Arc::new(driver.create_platform(self.event_manager.clone()).await);
            let _ = self.platform.insert(platform);
        }

        let _ = self.driver.insert(driver);
        let this = Arc::new(self);

        {
            this.event_manager
                .dispatch_async(ConnectionEvent::new(this.clone()))
                .await?;
        }

        Ok(Arc::try_unwrap(this).unwrap())
    }

    /// Prepares a SQL statement, returning a Statement object.
    ///
    /// Note that this method DOES NOT send the statement to the server, thus
    /// CANNOT guarantee that the SQL query is valid nor well-formed.
    /// As a result, this method WON'T return an error on invalid SQL syntax, but a
    /// subsequent call to `query` or `execute` methods on the returned Statement
    /// object will.
    pub fn prepare<St: Into<String>>(&self, sql: St) -> Result<Box<dyn Statement<'_> + '_>> {
        let driver = self.driver.as_ref().ok_or_else(Error::not_connected)?;
        driver.prepare(sql)
    }

    /// Executes an SQL statement, returning a result set as a StatementResult object.
    pub async fn query<St: Into<String>, P: IntoParameters>(
        &self,
        sql: St,
        params: P,
    ) -> Result<StatementResult> {
        let driver = self.driver.as_ref().ok_or_else(Error::not_connected)?;
        let platform = self.platform.as_ref().ok_or_else(Error::not_connected)?;
        driver.query(sql, params.into_parameters(platform)?).await
    }

    /// Executes an SQL statement with the given parameters and returns the number of affected rows.
    /// Could be used for:
    /// - DML statements: INSERT, UPDATE, DELETE, etc.
    /// - DDL statements: CREATE, DROP, ALTER, etc.
    /// - DCL statements: GRANT, REVOKE, etc.
    /// - Session control statements: ALTER SESSION, SET, DECLARE, etc.
    /// - Other statements that don't yield a row set.
    pub async fn execute_statement<St: Into<String>, P: IntoParameters>(
        &self,
        sql: St,
        params: P,
    ) -> Result<usize> {
        let driver = self.driver.as_ref().ok_or_else(Error::not_connected)?;
        let platform = self.platform.as_ref().ok_or_else(Error::not_connected)?;

        let stmt = driver.prepare(sql)?;
        stmt.execute(params.into_parameters(platform)?).await
    }

    /// Inserts a record into the given table.
    pub async fn insert(&self, table: &str, values: TypedValueMap<'_>) -> Result<usize> {
        if values.is_empty() {
            self.execute_statement(format!("INSERT INTO {} () VALUES ()", table), NO_PARAMS)
                .await
        } else {
            let platform = self.platform.as_ref().ok_or_else(Error::not_connected)?;

            let set = vec!["?"; values.len()].join(", ");
            let columns = values
                .keys()
                .map(|k| platform.quote_identifier(k))
                .join(", ");

            self.execute_statement(
                format!(
                    "INSERT INTO {} ({}) VALUES ({})",
                    platform.quote_identifier(table),
                    columns,
                    set
                ),
                values.into_values(),
            )
            .await
        }
    }

    /// Executes an SQL DELETE statement on a table.
    /// Table expression and columns are not escaped and are not safe for user-input.
    pub async fn delete(&self, table: &str, criteria: TypedValueMap<'_>) -> Result<usize> {
        if criteria.is_empty() {
            return Err(Error::empty_criteria());
        }

        let platform = self.platform.as_ref().ok_or_else(Error::not_connected)?;
        let columns = criteria
            .keys()
            .map(|k| format!("{} = ?", platform.quote_identifier(k)))
            .join(" AND ");

        self.execute_statement(
            format!("DELETE FROM {} WHERE {}", table, columns),
            criteria.into_values(),
        )
        .await
    }

    /// Executes an SQL statement, returning a result set as a vector of Row objects.
    pub async fn fetch_all<St: Into<String>>(
        &self,
        sql: St,
        params: Parameters<'static>,
    ) -> Result<Vec<Row>> {
        let statement_result = self.query(sql, params).await?;
        statement_result.fetch_all().await
    }

    /// Converts a value from database scalar format into runtime type format,
    /// according to the conversion rules specified by the mapping type.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use chrono::DateTime;
    /// # use creed::{Error, Result, Value, r#type, Connection};
    ///
    /// # fn convert(connection: &Connection) -> Result<()> {
    /// let v = connection.convert_value(&Value::String("2022-05-30T00:08:10+02".into()), r#type::DATETIMETZ)?;
    /// let res = DateTime::parse_from_rfc2822("Mon, 30 May 2022 00:08:10 +02:00")?;
    /// assert_eq!(v, Value::DateTime(res.into()));
    /// # Ok::<(), Error>(())
    /// # }
    /// ```
    pub fn convert_value<T: IntoType>(&self, value: &Value, column_type: T) -> Result<Value> {
        if let Some(platform) = self.platform.as_ref().cloned() {
            let t = column_type.into_type()?;
            t.convert_to_value(value, platform.as_ref().as_ref())
        } else {
            Err(Error::not_connected())
        }
    }

    /// Converts a value from runtime type format into database scalar format,
    /// according to the conversion rules specified by the mapping type.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use creed::{Result, Value, r#type, Connection, Error};
    /// # use uuid::Uuid;
    ///
    /// # fn convert(connection: &Connection) -> Result<()> {
    /// let v = connection.convert_database_value(Value::Uuid(Uuid::parse_str("978549b1-2b2b-42b4-91c4-355980ac1bb4")?), r#type::GUID)?;
    /// assert_eq!(v, Value::String("978549b1-2b2b-42b4-91c4-355980ac1bb4".into()));
    /// # Ok::<(), Error>(())
    /// # }
    /// ```
    pub fn convert_database_value<T: IntoType>(
        &self,
        value: Value,
        column_type: T,
    ) -> Result<Value> {
        if let Some(platform) = self.platform.as_ref().cloned() {
            let t = column_type.into_type()?;
            t.convert_to_database_value(value, platform.as_ref().as_ref())
        } else {
            Err(Error::not_connected())
        }
    }

    pub async fn begin_transaction(&self) -> Result<()> {
        let driver = self.driver.as_ref().ok_or_else(Error::not_connected)?;
        let old_level = self
            .transaction_nesting_level
            .fetch_add(1, Ordering::SeqCst);
        if old_level == 0 {
            debug!(target: "creed::sql", r#""START TRANSACTION""#);
            driver.begin_transaction().await?;
        } else {
            debug!(target: "creed::sql", r#""SAVEPOINT""#);
            self.create_savepoint(format!(
                "CREED_SAVEPOINT_{}",
                self.transaction_nesting_level.load(Ordering::SeqCst)
            ))
            .await?;
        }

        // todo: dispatch event

        Ok(())
    }

    pub async fn commit(&self) -> Result<()> {
        let transaction_nesting_level = self.transaction_nesting_level.load(Ordering::SeqCst);
        if transaction_nesting_level == 0 {
            return Err(Error::no_active_transaction());
        }

        let driver = self.driver.as_ref().ok_or_else(Error::not_connected)?;
        if transaction_nesting_level == 1 {
            debug!(target: "creed::sql", r#""COMMIT""#);
            driver.commit().await?;
        } else {
            self.release_savepoint(format!("CREED_SAVEPOINT_{}", transaction_nesting_level))
                .await?;
        }

        self.transaction_nesting_level
            .fetch_sub(1, Ordering::SeqCst);

        // todo: dispatch event

        Ok(())
    }

    /// Cancels any database changes done during the current transaction.
    pub async fn roll_back(&self) -> Result<()> {
        let transaction_nesting_level = self.transaction_nesting_level.load(Ordering::SeqCst);
        if transaction_nesting_level == 0 {
            return Err(Error::no_active_transaction());
        }

        let driver = self.driver.as_ref().ok_or_else(Error::not_connected)?;
        if transaction_nesting_level == 1 {
            debug!(target: "creed::sql", r#""ROLLBACK""#);
            driver.roll_back().await?;
        } else {
            self.rollback_savepoint(format!("CREED_SAVEPOINT_{}", transaction_nesting_level))
                .await?;
        }

        self.transaction_nesting_level
            .fetch_sub(1, Ordering::SeqCst);

        // todo: dispatch event

        Ok(())
    }

    /// Creates a new savepoint.
    pub async fn create_savepoint(&self, savepoint: impl AsRef<str>) -> Result<()> {
        let platform = self.get_platform()?;
        self.execute_statement(platform.create_save_point(savepoint.as_ref()), NO_PARAMS)
            .await?;

        Ok(())
    }

    /// Releases the given savepoint.
    pub async fn release_savepoint(&self, savepoint: impl AsRef<str>) -> Result<()> {
        let platform = self.get_platform()?;
        if platform.supports_release_savepoints() {
            debug!(target: "creed::sql", r#""RELEASE SAVEPOINT""#);
        }

        self.execute_statement(platform.release_save_point(savepoint.as_ref()), NO_PARAMS)
            .await?;

        Ok(())
    }

    /// Rolls back to the given savepoint.
    pub async fn rollback_savepoint(&self, savepoint: impl AsRef<str>) -> Result<()> {
        let platform = self.get_platform()?;
        self.execute_statement(platform.rollback_save_point(savepoint.as_ref()), NO_PARAMS)
            .await?;

        Ok(())
    }

    pub async fn server_version(&self) -> Result<String> {
        let Some(ref driver) = self.driver else {
            return Err(Error::not_connected());
        };
        Ok(driver.server_version().await)
    }

    fn add_database_suffix(connection_options: ConnectionOptions) -> ConnectionOptions {
        let mut options = connection_options.clone();
        if let Some(db_suffix) = connection_options.database_name_suffix {
            let db_name = &options.database_name;
            let db_name = format!(
                "{}{}",
                db_name
                    .as_ref()
                    .cloned()
                    .unwrap_or_else(|| "app".to_string()),
                db_suffix
            );

            options = options.with_database_name(Some(db_name));

            // TODO: primary/replica
        }

        options
    }
}

#[cfg(test)]
mod tests {
    use crate::event::ConnectionEvent;
    use crate::rows::ColumnIndex;
    use crate::tests::get_database_dsn;
    use crate::{params, r#type, Connection, EventDispatcher, Result, Row, Value};
    use lazy_static::lazy_static;
    use serial_test::serial;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    async fn is_debuggable() {
        #[cfg(test)]
        env_logger::init();

        let connection = Connection::create_from_dsn(&get_database_dsn(), None, None).unwrap();
        assert!(format!("{:?}", connection).starts_with("Connection {"));
    }

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    #[serial]
    async fn can_create_connection() {
        let connection = Connection::create_from_dsn(&get_database_dsn(), None, None).unwrap();
        assert!(!connection.is_connected());

        let connection = connection.connect().await.expect("Connection failed");
        assert!(connection.is_connected());

        {
            let statement = connection.prepare("SELECT 1").expect("Prepare failed");
            let result = statement.execute(params![]).await;
            assert!(result.is_ok());
        }

        let connection = connection.connect().await;
        assert!(connection.is_ok());
    }

    static CALLED: AtomicBool = AtomicBool::new(false);
    lazy_static! {
        static ref M_RESULT: Mutex<Option<Vec<Row>>> = Mutex::default();
    }

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    #[serial]
    async fn can_create_connection_with_event_dispatcher() {
        let events = EventDispatcher::new();
        events
            .add_async_listener(|ev: &mut ConnectionEvent| {
                Box::pin(async move {
                    let result = ev
                        .connection
                        .as_ref()
                        .query("SELECT 1", params![])
                        .await
                        .unwrap();
                    let rows = result.fetch_all().await?;

                    CALLED.store(true, Ordering::SeqCst);
                    let _ = M_RESULT.lock().unwrap().insert(rows);

                    Ok(())
                })
            })
            .await;

        let connection =
            Connection::create_from_dsn(&get_database_dsn(), None, Some(events)).unwrap();
        assert!(!connection.is_connected());

        let connection = connection.connect().await.expect("Connection failed");
        assert!(connection.is_connected());
        assert!(CALLED.load(Ordering::SeqCst));

        let rows = M_RESULT.lock().unwrap().clone().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows.get(0).unwrap().get(ColumnIndex::Position(0)).unwrap(),
            &Value::Int(1)
        );

        let statement = connection.prepare("SELECT 1").expect("Prepare failed");
        let result = statement.execute(params![]).await;
        assert!(result.is_ok());
    }

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    #[serial]
    async fn can_convert_type_to_runtime() -> Result<()> {
        let connection = Connection::create_from_dsn(&get_database_dsn(), None, None).unwrap();
        let connection = connection.connect().await.expect("Connection failed");
        assert!(connection.is_connected());

        let mut result = connection
            .query("SELECT 1", params![])
            .await
            .expect("Query failed");
        let row = result.fetch_one().await?.expect("One row is expected");

        let value = row.get(0).expect("At least one column is expected");
        let v_int = connection
            .convert_value(value, crate::r#type::INTEGER)
            .expect("Failed integer conversion");
        let v_string = connection
            .convert_value(value, crate::r#type::STRING)
            .expect("Failed string conversion");

        assert_eq!(v_int, Value::Int(1));
        assert_eq!(v_string, Value::String("1".to_string()));

        Ok(())
    }

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    #[serial]
    async fn can_retrieve_database_name() {
        let connection = Connection::create_from_dsn(&get_database_dsn(), None, None).unwrap();
        assert!(!connection.is_connected());

        let connection = connection.connect().await.expect("Connection failed");
        let current_database = connection.get_database().await;
        let current_database = current_database.expect("Failed to retrieve current database");
        #[cfg(feature = "sqlite")]
        assert_eq!("main", &current_database);
        #[cfg(not(feature = "sqlite"))]
        assert_eq!("dbal", &current_database);
    }

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    #[serial]
    async fn can_fetch_results() {
        let connection = Connection::create_from_dsn(&get_database_dsn(), None, None).unwrap();
        assert!(!connection.is_connected());

        let connection = connection.connect().await.expect("Connection failed");
        let platform = connection
            .get_platform()
            .expect("Failed to create platform");

        let result = connection
            .fetch_all(platform.get_dummy_select_sql(None), params!())
            .await;
        assert!(result.is_ok());

        let rows = result.unwrap();
        assert_eq!(1, rows.len());
    }

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    #[serial]
    async fn can_convert_values() {
        let connection = Connection::create_from_dsn(&get_database_dsn(), None, None).unwrap();
        assert!(!connection.is_connected());

        let connection = connection.connect().await.expect("Connection failed");
        let result = connection.convert_value(&Value::from("{ \"test\": true }"), r#type::JSON);
        assert!(result.is_ok());

        let value = result.unwrap();
        match value {
            Value::Json(v) => {
                assert_eq!(v, serde_json::json!({ "test": true }));
                Ok(())
            }
            _ => Err("Expected json"),
        }
        .unwrap();

        let result = connection.convert_database_value(
            Value::Json(serde_json::json!({ "test": true })),
            r#type::JSON,
        );
        assert!(result.is_ok());

        let value = result.unwrap();
        match value {
            Value::String(v) => {
                assert_eq!(&v, r#"{"test":true}"#);
                Ok(())
            }
            _ => Err("Expected string"),
        }
        .unwrap();
    }
}

use crate::driver::statement::Statement;
use crate::driver::statement_result::StatementResult;
use crate::driver::Driver;
use crate::event::ConnectionEvent;
use crate::platform::DatabasePlatform;
use crate::r#type::{IntoType, Type};
use crate::{ConnectionOptions, Error, EventDispatcher, Parameters, Result, Value};
use std::sync::Arc;

#[derive(Debug)]
pub struct Connection {
    connection_options: ConnectionOptions,
    driver: Option<Arc<Driver>>,
    platform: Option<Arc<Box<dyn DatabasePlatform + Send + Sync>>>,
    event_manager: Arc<EventDispatcher>,
}

impl Connection {
    pub fn create(
        connection_options: ConnectionOptions,
        event_manager: Option<EventDispatcher>,
    ) -> Self {
        let connection_options = Self::add_database_suffix(connection_options);
        let event_manager = Arc::new(event_manager.unwrap_or_else(|| EventDispatcher::default()));

        Self {
            connection_options,
            driver: None,
            platform: None,
            event_manager,
        }
    }

    pub fn create_from_dsn(dsn: &str, event_manager: Option<EventDispatcher>) -> Result<Self> {
        Ok(Self::create(
            ConnectionOptions::try_from(dsn)?,
            event_manager,
        ))
    }

    pub fn is_connected(&self) -> bool {
        self.driver.is_some()
    }

    pub async fn connect(mut self) -> Result<Self> {
        if self.driver.is_some() {
            return Ok(self);
        }

        let driver = Arc::new(Driver::create(&self.connection_options).await?);
        let platform = Arc::new(driver.create_platform(self.event_manager.clone()).await);

        let _ = self.driver.insert(driver);
        let _ = self.platform.insert(platform);

        let this = Arc::new(self);

        {
            let mut event = ConnectionEvent::new(this.clone());
            this.event_manager.dispatch_async(&mut event).await;
        }

        Ok(Arc::try_unwrap(this).unwrap())
    }

    pub fn prepare<St: Into<String>>(&self, sql: St) -> Result<Box<dyn Statement<'_> + '_>> {
        let driver = self.driver.as_ref().ok_or_else(Error::not_connected)?;
        driver.prepare(sql)
    }

    /// Executes an SQL statement, returning a result set as a Statement object.
    pub async fn query<St: Into<String>>(
        &self,
        sql: St,
        params: Parameters<'_>,
    ) -> Result<Box<dyn StatementResult>> {
        let driver = self.driver.as_ref().ok_or_else(Error::not_connected)?;
        driver.query(sql, params).await
    }

    pub fn convert_value<T: IntoType>(&self, value: Option<&str>, column_type: T) -> Result<Value> {
        if let Some(platform) = self.platform.as_ref().cloned() {
            let t = column_type.into_type()?;
            t.convert_to_value(value, platform.as_ref().as_ref())
        } else {
            Err(Error::not_connected())
        }
    }

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

    fn add_database_suffix(connection_options: ConnectionOptions) -> ConnectionOptions {
        let mut options = connection_options.clone();
        if let Some(db_suffix) = connection_options.database_name_suffix {
            let db_name = &options.database_name;
            let db_name = format!(
                "{}{}",
                db_name.as_ref().cloned().unwrap_or("app".to_string()),
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
    use crate::{params, Connection, EventDispatcher, Row, Value};
    use lazy_static::lazy_static;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    async fn can_create_connection() {
        let connection =
            Connection::create_from_dsn(&std::env::var("DATABASE_DSN").unwrap(), None).unwrap();
        assert_eq!(connection.is_connected(), false);

        let connection = connection.connect().await.expect("Connection failed");
        assert_eq!(connection.is_connected(), true);

        let statement = connection.prepare("SELECT 1").expect("Prepare failed");
        let result = statement.execute(params![]).await;
        assert_eq!(result.is_ok(), true);
    }

    static CALLED: AtomicBool = AtomicBool::new(false);
    lazy_static! {
        static ref M_RESULT: Mutex<Option<Vec<Row>>> = Mutex::default();
    }

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
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
                    let rows = result.fetch_all();

                    CALLED.store(true, Ordering::SeqCst);
                    let _ = M_RESULT.lock().unwrap().insert(rows.unwrap().clone());
                })
            })
            .await;

        let connection =
            Connection::create_from_dsn(&std::env::var("DATABASE_DSN").unwrap(), Some(events))
                .unwrap();
        assert_eq!(connection.is_connected(), false);

        let connection = connection.connect().await.expect("Connection failed");
        assert_eq!(connection.is_connected(), true);
        assert_eq!(CALLED.load(Ordering::SeqCst), true);

        let rows = M_RESULT.lock().unwrap().clone().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows.get(0).unwrap().get(ColumnIndex::Position(0)).unwrap(),
            &Value::Int(1)
        );

        let statement = connection.prepare("SELECT 1").expect("Prepare failed");
        let result = statement.execute(params![]).await;
        assert_eq!(result.is_ok(), true);
    }
}

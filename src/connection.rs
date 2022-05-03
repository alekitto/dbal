use crate::driver::{Driver, DriverStatement, DriverStatementResult};
use crate::{ConnectionEvent, Error, EventDispatcher, Parameters, Result};
use std::sync::Arc;

pub struct Connection {
    dsn: String,
    driver: Option<Arc<Driver>>,
    event_manager: Option<EventDispatcher>,
}

impl Connection {
    pub fn create(dsn: &str, event_manager: Option<EventDispatcher>) -> Self {
        Self {
            dsn: dsn.to_string(),
            driver: None,
            event_manager,
        }
    }

    pub fn is_connected(&self) -> bool {
        self.driver.is_some()
    }

    pub async fn connect(mut self) -> Result<Self> {
        if self.driver.is_some() {
            return Ok(self);
        }

        let driver = Arc::new(Driver::create(&self.dsn).await?);
        if let Some(ref ev) = self.event_manager {
            let mut event = ConnectionEvent::new(driver.clone());
            ev.dispatch(&mut event).await;
        }

        let _ = self.driver.insert(driver);

        Ok(self)
    }

    pub fn prepare<St: Into<String>>(&self, sql: St) -> Result<DriverStatement<'_>> {
        let driver = self.driver.as_ref().ok_or_else(Error::not_connected)?;
        driver.prepare(sql)
    }

    /// Executes an SQL statement, returning a result set as a Statement object.
    pub async fn query<St: Into<String>>(
        &self,
        sql: St,
        params: Parameters<'_>,
    ) -> Result<DriverStatementResult> {
        let driver = self.driver.as_ref().ok_or_else(Error::not_connected)?;
        driver.query(sql, params).await
    }
}

#[cfg(test)]
mod tests {
    use crate::rows::ColumnIndex;
    use crate::{params, Connection, ConnectionEvent, EventDispatcher, Row, Value};
    use lazy_static::lazy_static;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    async fn can_create_connection() {
        let connection = Connection::create(&std::env::var("DATABASE_DSN").unwrap(), None);
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
        events.add_listener(|ev: &mut ConnectionEvent| {
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
        });

        let connection = Connection::create(&std::env::var("DATABASE_DSN").unwrap(), Some(events));
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

use crate::migrate::Direction;
use crate::schema::Schema;
use crate::Error;

pub struct ExecutionResult {
    pub sql: Vec<String>,
    pub version: i64,
    pub direction: Direction,
    pub executed_at: chrono::DateTime<chrono::Utc>,
    pub execution_time: i64,
    pub skipped: bool,
    pub error: Option<Error>,
    pub to_schema: Option<Schema>,
}

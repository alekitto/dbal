use crate::migrate::execution_result::ExecutionResult;
use crate::migrate::{Direction, Migration};

pub(super) struct MigrationPlan {
    pub version: i64,
    pub migration: Migration,
    pub direction: Direction,
    pub execution_result: Option<ExecutionResult>,
}

impl MigrationPlan {
    pub fn new(migration: Migration, direction: Direction) -> Self {
        Self {
            version: migration.version,
            migration,
            direction,
            execution_result: None,
        }
    }
}

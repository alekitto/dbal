pub struct ExecutedMigration {
    pub version: i64,
    pub executed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub execution_time: Option<u64>,
}

pub struct ExecutedMigrationList {
    pub(super) items: Vec<ExecutedMigration>,
}

impl ExecutedMigrationList {
    pub fn first(&self) -> Option<&ExecutedMigration> {
        self.items.first()
    }

    pub fn last(&self) -> Option<&ExecutedMigration> {
        self.items.last()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn has_migration(&self, version: i64) -> bool {
        self.items.iter().any(|v| v.version == version)
    }

    pub fn get_migration(&self, version: i64) -> Option<&ExecutedMigration> {
        self.items.iter().find(|v| v.version == version)
    }
}

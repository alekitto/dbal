use crate::event::event::PlatformBox;
use crate::platform::DatabasePlatform;
use crate::schema::TableDiff;
use crate::Event;
use std::any::TypeId;
use std::sync::atomic::{AtomicBool, Ordering};

/// Event Arguments used when SQL queries for renaming table columns are generated inside DatabasePlatform.
pub struct SchemaAlterTableEvent<'platform, 'table: 'platform> {
    prevent_default_flag: AtomicBool,
    table_diff: &'table TableDiff<'table>,
    platform: PlatformBox<'platform>,
    pub(crate) sql: Vec<String>,
}

impl Event for SchemaAlterTableEvent<'_, '_> {
    fn is_async() -> bool {
        false
    }

    fn event_type() -> TypeId {
        TypeId::of::<SchemaAlterTableEvent>()
    }
}

impl<'platform, 'table: 'platform> SchemaAlterTableEvent<'platform, 'table> {
    pub(crate) fn new(
        table_diff: &'table TableDiff,
        platform: &'platform (dyn DatabasePlatform + Sync),
    ) -> Self {
        Self {
            prevent_default_flag: AtomicBool::new(false),
            table_diff,
            platform: Box::new(platform),
            sql: vec![],
        }
    }

    pub fn prevent_default(&self) {
        self.prevent_default_flag.store(true, Ordering::SeqCst);
    }

    pub fn is_default_prevented(&self) -> bool {
        self.prevent_default_flag.load(Ordering::SeqCst)
    }

    pub fn get_table_diff(&self) -> &TableDiff {
        &self.table_diff
    }

    pub fn get_platform(&self) -> &PlatformBox<'platform> {
        &self.platform
    }

    pub fn add_sql(&mut self, sql: &mut Vec<String>) {
        self.sql.append(sql)
    }

    pub fn get_sql(&self) -> Vec<String> {
        self.sql.clone()
    }
}

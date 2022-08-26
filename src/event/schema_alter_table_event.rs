use crate::schema::TableDiff;
use crate::util::PlatformBox;
use crate::Event;
use std::any::TypeId;
use std::sync::atomic::{AtomicBool, Ordering};

/// Event Arguments used when SQL queries for renaming table columns are generated inside DatabasePlatform.
pub struct SchemaAlterTableEvent<'table> {
    prevent_default_flag: AtomicBool,
    table_diff: &'table TableDiff<'table>,
    platform: PlatformBox,
    pub(crate) sql: Vec<String>,
}

impl Event for SchemaAlterTableEvent<'_> {
    fn is_async() -> bool {
        false
    }

    fn event_type() -> TypeId {
        TypeId::of::<SchemaAlterTableEvent>()
    }
}

impl<'table> SchemaAlterTableEvent<'table> {
    pub(crate) fn new(table_diff: &'table TableDiff, platform: PlatformBox) -> Self {
        Self {
            prevent_default_flag: AtomicBool::new(false),
            table_diff,
            platform,
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
        self.table_diff
    }

    pub fn get_platform(&self) -> PlatformBox {
        self.platform.clone()
    }

    pub fn add_sql(&mut self, sql: &mut Vec<String>) {
        self.sql.append(sql)
    }

    pub fn get_sql(&self) -> Vec<String> {
        self.sql.clone()
    }
}

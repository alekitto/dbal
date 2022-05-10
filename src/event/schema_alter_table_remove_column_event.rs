use crate::event::event::PlatformBox;
use crate::platform::DatabasePlatform;
use crate::schema::{Column, TableDiff};
use crate::Event;
use std::any::TypeId;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct SchemaAlterTableRemoveColumnEvent<'platform, 'table: 'platform, 'column: 'table> {
    prevent_default_flag: AtomicBool,
    table_diff: &'table TableDiff<'table>,
    column: &'column Column,
    platform: PlatformBox<'platform>,
    pub(crate) sql: Vec<String>,
}

impl Event for SchemaAlterTableRemoveColumnEvent<'_, '_, '_> {
    fn is_async() -> bool {
        false
    }

    fn event_type() -> TypeId {
        TypeId::of::<SchemaAlterTableRemoveColumnEvent>()
    }
}

impl<'platform, 'table: 'platform, 'column: 'table>
    SchemaAlterTableRemoveColumnEvent<'platform, 'table, 'column>
{
    pub(crate) fn new(
        column: &'column Column,
        table_diff: &'table TableDiff,
        platform: &'platform (dyn DatabasePlatform + Sync),
    ) -> Self {
        Self {
            prevent_default_flag: AtomicBool::new(false),
            table_diff,
            column,
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

    pub fn get_column(&self) -> &Column {
        &self.column
    }

    pub fn get_platform(&self) -> &PlatformBox<'platform> {
        &self.platform
    }

    pub fn get_sql(&self) -> Vec<String> {
        self.sql.clone()
    }
}

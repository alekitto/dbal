use crate::schema::{Column, TableDiff};
use crate::util::PlatformBox;
use crate::Event;
use std::any::TypeId;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct SchemaAlterTableAddColumnEvent<'table, 'column: 'table> {
    prevent_default_flag: AtomicBool,
    table_diff: &'table TableDiff<'table>,
    column: &'column Column,
    platform: PlatformBox,
    pub(crate) sql: Vec<String>,
}

impl Event for SchemaAlterTableAddColumnEvent<'_, '_> {
    fn is_async() -> bool {
        false
    }

    fn event_type() -> TypeId {
        TypeId::of::<SchemaAlterTableAddColumnEvent>()
    }
}

impl<'table, 'column: 'table> SchemaAlterTableAddColumnEvent<'table, 'column> {
    pub(crate) fn new(
        column: &'column Column,
        table_diff: &'table TableDiff,
        platform: PlatformBox,
    ) -> Self {
        Self {
            prevent_default_flag: AtomicBool::new(false),
            table_diff,
            column,
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

    pub fn get_column(&self) -> &Column {
        self.column
    }

    pub fn get_platform(&self) -> PlatformBox {
        self.platform.clone()
    }

    pub fn get_sql(&self) -> Vec<String> {
        self.sql.clone()
    }
}

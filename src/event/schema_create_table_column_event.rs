use crate::Event;
use crate::schema::{Column, Table};
use crate::util::PlatformBox;
use std::any::TypeId;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct SchemaCreateTableColumnEvent<'table, 'column: 'table> {
    prevent_default_flag: AtomicBool,
    table: &'table Table,
    column: &'column Column,
    platform: PlatformBox,
    pub(crate) sql: Vec<String>,
}

impl Event for SchemaCreateTableColumnEvent<'_, '_> {
    fn is_async() -> bool {
        false
    }

    fn event_type() -> TypeId {
        TypeId::of::<SchemaCreateTableColumnEvent>()
    }
}

impl<'table, 'column: 'table> SchemaCreateTableColumnEvent<'table, 'column> {
    pub(crate) fn new(
        table: &'table Table,
        column: &'column Column,
        platform: PlatformBox,
    ) -> Self {
        Self {
            prevent_default_flag: AtomicBool::new(false),
            table,
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

    pub fn get_table(&self) -> &Table {
        self.table
    }

    pub fn get_column(&self) -> &Column {
        self.column
    }

    pub fn get_platform(&self) -> PlatformBox {
        self.platform.clone()
    }

    pub(crate) fn get_sql(&self) -> Vec<String> {
        self.sql.clone()
    }
}

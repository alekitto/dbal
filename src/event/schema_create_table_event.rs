use crate::schema::Table;
use crate::util::PlatformBox;
use crate::Event;
use std::any::TypeId;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct SchemaCreateTableEvent<'table> {
    prevent_default_flag: AtomicBool,
    table: &'table Table,
    platform: PlatformBox,
    pub(crate) sql: Vec<String>,
}

impl Event for SchemaCreateTableEvent<'_> {
    fn is_async() -> bool {
        false
    }

    fn event_type() -> TypeId {
        TypeId::of::<SchemaCreateTableEvent>()
    }
}

impl<'table> SchemaCreateTableEvent<'table> {
    pub(crate) fn new(table: &'table Table, platform: PlatformBox) -> Self {
        Self {
            prevent_default_flag: AtomicBool::new(false),
            table,
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

    pub fn get_platform(&self) -> PlatformBox {
        self.platform.clone()
    }

    pub(crate) fn get_sql(&self) -> Vec<String> {
        self.sql.clone()
    }
}

use crate::event::event::PlatformBox;
use crate::platform::DatabasePlatform;
use crate::Event;
use std::any::TypeId;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct SchemaDropTableEvent<'a> {
    prevent_default_flag: AtomicBool,
    table: String,
    platform: PlatformBox<'a>,
    pub(crate) sql: Option<String>,
}

impl Event for SchemaDropTableEvent<'_> {
    fn is_async() -> bool {
        false
    }

    fn event_type() -> TypeId {
        TypeId::of::<SchemaDropTableEvent>()
    }
}

impl<'a> SchemaDropTableEvent<'a> {
    pub(crate) fn new(table: String, platform: &'a (dyn DatabasePlatform + Sync)) -> Self {
        Self {
            prevent_default_flag: AtomicBool::new(false),
            table,
            platform: Box::new(platform),
            sql: None,
        }
    }

    pub fn prevent_default(&self) {
        self.prevent_default_flag.store(true, Ordering::SeqCst);
    }

    pub fn is_default_prevented(&self) -> bool {
        self.prevent_default_flag.load(Ordering::SeqCst)
    }

    pub fn get_table(&self) -> &str {
        &self.table
    }

    pub fn get_platform(&self) -> &PlatformBox<'a> {
        &self.platform
    }
}

use crate::schema::{Index, IndexOptions};
use crate::util::PlatformBox;
use crate::Event;
use std::any::TypeId;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct SchemaIndexDefinitionEvent<'a> {
    prevent_default_flag: AtomicBool,
    index: Option<Index>,
    table_index: &'a IndexOptions,
    table: &'a str,
    platform: PlatformBox,
    pub(crate) sql: Vec<String>,
}

impl Event for SchemaIndexDefinitionEvent<'_> {
    fn is_async() -> bool {
        false
    }

    fn event_type() -> TypeId {
        TypeId::of::<SchemaIndexDefinitionEvent>()
    }
}

impl<'a> SchemaIndexDefinitionEvent<'a> {
    pub(crate) fn new(
        table_index: &'a IndexOptions,
        table: &'a str,
        platform: PlatformBox,
    ) -> Self {
        Self {
            prevent_default_flag: AtomicBool::new(false),
            index: None,
            table_index,
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

    pub fn index(self) -> Option<Index> {
        self.index
    }

    pub fn get_index(&self) -> &Option<Index> {
        &self.index
    }

    pub fn set_index(&mut self, index: Option<Index>) {
        self.index = index;
    }

    pub fn get_table_index(&self) -> &'a IndexOptions {
        self.table_index
    }

    pub fn get_table(&self) -> &'a str {
        self.table
    }

    pub fn get_platform(&self) -> PlatformBox {
        self.platform.clone()
    }

    pub fn get_sql(&self) -> Vec<String> {
        self.sql.clone()
    }
}

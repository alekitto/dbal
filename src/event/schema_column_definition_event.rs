use crate::schema::Column;
use crate::util::PlatformBox;
use crate::{Event, Row};
use std::any::TypeId;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct SchemaColumnDefinitionEvent<'a> {
    prevent_default_flag: AtomicBool,
    column: Option<Column>,
    table_column: &'a Row,
    table: &'a str,
    database: &'a str,
    platform: PlatformBox,
    pub(crate) sql: Vec<String>,
}

impl Event for SchemaColumnDefinitionEvent<'_> {
    fn is_async() -> bool {
        false
    }

    fn event_type() -> TypeId {
        TypeId::of::<SchemaColumnDefinitionEvent>()
    }
}

impl<'a> SchemaColumnDefinitionEvent<'a> {
    pub(crate) fn new(
        table_column: &'a Row,
        table: &'a str,
        database: &'a str,
        platform: PlatformBox,
    ) -> Self {
        Self {
            prevent_default_flag: AtomicBool::new(false),
            column: None,
            table_column,
            table,
            database,
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

    pub fn column(self) -> Option<Column> {
        self.column
    }

    pub fn get_column(&self) -> &Option<Column> {
        &self.column
    }

    pub fn set_column(&mut self, column: Option<Column>) {
        self.column = column;
    }

    pub fn get_table_column(&self) -> &'a Row {
        self.table_column
    }

    pub fn get_table(&self) -> &'a str {
        self.table
    }

    pub fn get_database(&self) -> &'a str {
        self.database
    }

    pub fn get_platform(&self) -> PlatformBox {
        self.platform.clone()
    }

    pub fn get_sql(&self) -> Vec<String> {
        self.sql.clone()
    }
}

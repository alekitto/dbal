use crate::schema::{Column, TableDiff};
use crate::util::PlatformBox;
use crate::Event;
use std::any::TypeId;
use std::sync::atomic::{AtomicBool, Ordering};

/// Event Arguments used when SQL queries for renaming table columns are generated inside DatabasePlatform.
pub struct SchemaAlterTableRenameColumnEvent<'table, 'column: 'table> {
    prevent_default_flag: AtomicBool,
    table_diff: &'table TableDiff<'table>,
    column: &'column Column,
    old_column_name: String,
    platform: PlatformBox,
    pub(crate) sql: Vec<String>,
}

impl Event for SchemaAlterTableRenameColumnEvent<'_, '_> {
    fn is_async() -> bool {
        false
    }

    fn event_type() -> TypeId {
        TypeId::of::<SchemaAlterTableRenameColumnEvent>()
    }
}

impl<'table, 'column: 'table> SchemaAlterTableRenameColumnEvent<'table, 'column> {
    pub(crate) fn new(
        old_column_name: &str,
        column: &'column Column,
        table_diff: &'table TableDiff,
        platform: PlatformBox,
    ) -> Self {
        Self {
            prevent_default_flag: AtomicBool::new(false),
            old_column_name: old_column_name.to_string(),
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

    pub fn get_old_column_name(&self) -> &str {
        &self.old_column_name
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

    pub fn add_sql(&mut self, sql: &mut Vec<String>) {
        self.sql.append(sql)
    }

    pub fn get_sql(&self) -> Vec<String> {
        self.sql.clone()
    }
}

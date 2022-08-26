use crate::platform::DatabasePlatform;
use crate::schema::ColumnData;
use crate::{platform_debug, EventDispatcher};
use std::any::TypeId;
use std::sync::Arc;

platform_debug!(SQLitePlatform);
pub(super) struct SQLitePlatform {
    ev: Arc<EventDispatcher>,
}

impl SQLitePlatform {
    pub fn new(ev: Arc<EventDispatcher>) -> Self {
        Self { ev }
    }
}

impl DatabasePlatform for SQLitePlatform {
    fn get_event_manager(&self) -> Arc<EventDispatcher> {
        self.ev.clone()
    }

    fn _initialize_type_mappings(&self) {
        todo!()
    }

    fn _add_type_mapping(&self, db_type: &str, type_id: TypeId) {
        todo!()
    }

    fn get_boolean_type_declaration_sql(&self, column: &ColumnData) -> crate::Result<String> {
        todo!()
    }

    fn get_integer_type_declaration_sql(&self, column: &ColumnData) -> crate::Result<String> {
        todo!()
    }

    fn get_bigint_type_declaration_sql(&self, column: &ColumnData) -> crate::Result<String> {
        todo!()
    }

    fn get_smallint_type_declaration_sql(&self, column: &ColumnData) -> crate::Result<String> {
        todo!()
    }

    fn get_clob_type_declaration_sql(&self, column: &ColumnData) -> crate::Result<String> {
        todo!()
    }

    fn get_blob_type_declaration_sql(&self, column: &ColumnData) -> crate::Result<String> {
        todo!()
    }

    fn get_name(&self) -> String {
        todo!()
    }

    fn get_type_mapping(&self, db_type: &str) -> crate::Result<TypeId> {
        todo!()
    }

    fn get_current_database_expression(&self) -> String {
        todo!()
    }

    fn create_reserved_keywords_list(&self) -> crate::platform::KeywordList {
        todo!()
    }
}

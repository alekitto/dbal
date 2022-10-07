use crate::platform::{DatabasePlatform, KeywordList, Keywords};
use crate::schema::{ColumnData, SchemaManager};
use crate::tests::schema_manager::MockSchemaManager;
use crate::{Connection, EventDispatcher, Result};
use std::any::TypeId;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

pub struct MockKeywords {}
impl Keywords for MockKeywords {
    fn get_name(&self) -> &'static str {
        "Mock"
    }

    fn get_keywords(&self) -> &[&'static str] {
        &["TABLE"]
    }
}

static MOCK_KEYWORDS: MockKeywords = MockKeywords {};

pub struct MockPlatform {
    pub ev: Arc<EventDispatcher>,
}

impl Debug for MockPlatform {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MockPlatform {{}}")
    }
}

impl DatabasePlatform for MockPlatform {
    fn get_event_manager(&self) -> Arc<EventDispatcher> {
        self.ev.clone()
    }

    fn as_dyn(&self) -> &dyn DatabasePlatform {
        self
    }

    fn _initialize_type_mappings(&self) {
        todo!()
    }

    fn _add_type_mapping(&self, db_type: &str, type_id: TypeId) {
        todo!()
    }

    fn get_boolean_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        todo!()
    }

    fn get_integer_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        todo!()
    }

    fn get_bigint_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        todo!()
    }

    fn get_smallint_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        todo!()
    }

    fn get_clob_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        todo!()
    }

    fn get_blob_type_declaration_sql(&self, column: &ColumnData) -> Result<String> {
        todo!()
    }

    fn get_name(&self) -> String {
        todo!()
    }

    fn get_type_mapping(&self, db_type: &str) -> Result<TypeId> {
        todo!()
    }

    fn get_current_database_expression(&self) -> String {
        todo!()
    }

    fn create_reserved_keywords_list(&self) -> KeywordList {
        KeywordList::new(&MOCK_KEYWORDS)
    }

    fn create_schema_manager<'a>(&self, connection: &'a Connection) -> Box<dyn SchemaManager + 'a> {
        Box::new(MockSchemaManager::new(connection))
    }
}

pub macro common_platform_tests($ex:expr) {
    #[test]
    pub fn get_unknown_mapping_type() {
        let platform = $ex;
        let t = platform.get_type_mapping("foobar");
        assert_eq!(t.is_err(), true);
    }

    #[test]
    pub fn register_mapping_type() {
        let platform = $ex;
        platform
            .register_type_mapping("foo", TypeId::of::<$crate::r#type::IntegerType>())
            .expect("Failed to register type mapping");
        assert_eq!(
            platform.get_type_mapping("foo").unwrap(),
            TypeId::of::<$crate::r#type::IntegerType>()
        );
    }

    #[test]
    pub fn register_unknown_mapping_type() {
        use std::any::Any;
        let platform = $ex;
        let result = platform.register_type_mapping("foo", platform.type_id());
        assert_eq!(result.is_err(), true);
    }
}

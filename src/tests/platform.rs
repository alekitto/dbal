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

    #[test]
    pub fn generates_bit_and_comparison_expression_sql() {
        let platform = $ex;
        let sql = platform.get_bit_and_comparison_expression(&2, &4).unwrap();
        assert_eq!(sql, "(2 & 4)");
    }

    #[test]
    pub fn generates_bit_or_comparison_expression_sql() {
        let platform = $ex;
        let sql = platform.get_bit_or_comparison_expression(&2, &4).unwrap();
        assert_eq!(sql, "(2 | 4)");
    }

    #[test]
    pub fn get_default_value_declaration_sql() {
        let platform = $ex;
        let mut column = $crate::schema::Column::new("foo", $crate::r#type::STRING).unwrap();
        column.set_default("non_timestamp".into());

        // non-timestamp value will get single quotes
        assert_eq!(
            platform
                .get_default_value_declaration_sql(&column.generate_column_data(&platform))
                .unwrap(),
            " DEFAULT 'non_timestamp'"
        );
    }

    #[test]
    pub fn get_default_value_declaration_sql_date_time() {
        let platform = $ex;

        // timestamps on datetime types should not be quoted
        for t in [$crate::r#type::DATETIME, $crate::r#type::DATETIMETZ] {
            let mut column = $crate::schema::Column::new("foo", t).unwrap();
            column.set_default(platform.get_current_timestamp_sql().into());

            assert_eq!(
                platform
                    .get_default_value_declaration_sql(&column.generate_column_data(&platform))
                    .unwrap(),
                format!(" DEFAULT {}", platform.get_current_timestamp_sql())
            );
        }
    }

    #[test]
    pub fn get_default_value_declaration_sqlfor_integer_types() {
        let platform = $ex;

        for t in [$crate::r#type::BIGINT, $crate::r#type::INTEGER] {
            let mut column = $crate::schema::Column::new("foo", t).unwrap();
            column.set_default(1.into());

            assert_eq!(
                platform
                    .get_default_value_declaration_sql(&column.generate_column_data(&platform))
                    .unwrap(),
                " DEFAULT 1"
            );
        }
    }

    #[test]
    pub fn test_get_default_value_declaration_sql_for_date_type() {
        let platform = $ex;

        let current_date_sql = platform.get_current_date_sql();
        let mut column = $crate::schema::Column::new("foo", $crate::r#type::DATE).unwrap();
        column.set_default(current_date_sql.into());

        assert_eq!(
            platform
                .get_default_value_declaration_sql(&column.generate_column_data(&platform))
                .unwrap(),
            format!(" DEFAULT {}", current_date_sql)
        );
    }

    #[test]
    pub fn keyword_list() {
        let platform = $ex;

        let keyword_list = platform.create_reserved_keywords_list();
        assert_eq!(keyword_list.is_keyword("table"), true);
    }
}

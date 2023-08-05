use crate::platform::DatabasePlatform;
use crate::r#type::Type;
use crate::schema::ColumnData;
use crate::Result;
use crate::Value;

pub struct BooleanType {}

impl Type for BooleanType {
    fn default() -> Box<dyn Type + Sync + Send> {
        Box::new(BooleanType {})
    }

    fn convert_to_database_value(
        &self,
        value: Value,
        platform: &dyn DatabasePlatform,
    ) -> Result<Value> {
        platform.convert_booleans_to_database_value(value)
    }

    fn convert_to_value(&self, value: &Value, platform: &dyn DatabasePlatform) -> Result<Value> {
        match value {
            Value::NULL | Value::Boolean(_) => Ok(value.clone()),
            _ => Ok(platform.convert_from_boolean(value)),
        }
    }

    fn get_name(&self) -> &'static str {
        super::BOOLEAN
    }

    fn requires_sql_comment_hint(&self, platform: &dyn DatabasePlatform) -> bool {
        platform.get_name() != "DB2"
    }

    fn get_sql_declaration(
        &self,
        column: &ColumnData,
        platform: &dyn DatabasePlatform,
    ) -> Result<String> {
        platform.get_boolean_type_declaration_sql(column)
    }

    fn convert_to_default_value(
        &self,
        value: &Value,
        platform: &dyn DatabasePlatform,
    ) -> Result<String> {
        platform
            .convert_boolean(value.clone())
            .map(|v| v.to_string())
    }
}

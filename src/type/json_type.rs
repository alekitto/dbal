use crate::platform::DatabasePlatform;
use crate::r#type::Type;
use crate::schema::ColumnData;
use crate::{Error, Result, Value};

pub struct JsonType {}

impl Type for JsonType {
    fn default() -> Box<dyn Type + Sync + Send> {
        Box::new(JsonType {})
    }

    fn convert_to_value(&self, value: &Value, _: &dyn DatabasePlatform) -> Result<Value> {
        match value {
            Value::NULL | Value::Json(_) => Ok(value.clone()),
            Value::String(value) => Ok(Value::Json(serde_json::from_str(&value)?)),
            _ => Err(Error::conversion_failed_invalid_type(
                &value,
                self.get_name(),
                &["NULL", "JSON String"],
            )),
        }
    }

    fn get_name(&self) -> &'static str {
        super::JSON
    }

    fn get_sql_declaration(
        &self,
        column: &ColumnData,
        platform: &dyn DatabasePlatform,
    ) -> Result<String> {
        platform.get_json_type_declaration_sql(column)
    }
}

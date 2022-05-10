use crate::platform::DatabasePlatform;
use crate::r#type::Type;
use crate::schema::ColumnData;
use crate::{Error, Result, Value};

pub struct JsonType {}

impl Type for JsonType {
    fn default() -> Box<dyn Type + Sync + Send> {
        Box::new(JsonType {})
    }

    fn convert_to_value(&self, value: Option<&str>, _: &dyn DatabasePlatform) -> Result<Value> {
        if let Some(value) = value {
            if value.is_empty() {
                Ok(Value::NULL)
            } else {
                if let Ok(result) = serde_json::from_str(value) {
                    Ok(Value::Json(result))
                } else {
                    Err(Error::conversion_failed_invalid_type(
                        &Value::String(value.to_string()),
                        self.get_name(),
                        &["NULL", "JSON"],
                    ))
                }
            }
        } else {
            Ok(Value::NULL)
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

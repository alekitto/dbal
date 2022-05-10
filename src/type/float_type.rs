use crate::platform::DatabasePlatform;
use crate::r#type::Type;
use crate::schema::ColumnData;
use crate::{Error, ParameterType, Result, Value};

pub struct FloatType {}

impl Type for FloatType {
    fn default() -> Box<dyn Type + Sync + Send> {
        Box::new(FloatType {})
    }

    fn convert_to_value(&self, value: Option<&str>, _: &dyn DatabasePlatform) -> Result<Value> {
        if let Some(value) = value {
            if value.is_empty() {
                Ok(Value::NULL)
            } else {
                if let Ok(result) = value.parse() {
                    Ok(Value::Float(result))
                } else {
                    Err(Error::conversion_failed_invalid_type(
                        &Value::String(value.to_string()),
                        self.get_name(),
                        &["NULL", "Float"],
                    ))
                }
            }
        } else {
            Ok(Value::NULL)
        }
    }

    fn get_name(&self) -> &'static str {
        super::FLOAT
    }

    fn get_sql_declaration(
        &self,
        column: &ColumnData,
        platform: &dyn DatabasePlatform,
    ) -> Result<String> {
        platform.get_float_declaration_sql(column)
    }

    fn get_binding_type(&self) -> ParameterType {
        ParameterType::Float
    }
}

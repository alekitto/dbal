use crate::platform::DatabasePlatform;
use crate::r#type::Type;
use crate::schema::ColumnData;
use crate::{Error, ParameterType, Result, Value};

pub struct FloatType {}

impl Type for FloatType {
    fn default() -> Box<dyn Type + Sync + Send> {
        Box::new(FloatType {})
    }

    fn convert_to_value(&self, value: &Value, _: &dyn DatabasePlatform) -> Result<Value> {
        match value {
            Value::NULL | Value::Float(_) => Ok(value.clone()),
            Value::String(ref str) => {
                if let Ok(result) = str.parse() {
                    Ok(Value::Float(result))
                } else {
                    Err(Error::conversion_failed_invalid_type(
                        &Value::String(value.to_string()),
                        self.get_name(),
                        &["NULL", "Float"],
                    ))
                }
            }
            _ => Err(Error::conversion_failed_invalid_type(
                value,
                self.get_name(),
                &["NULL", "Numeric string", "Float"],
            )),
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

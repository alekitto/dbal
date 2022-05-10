use crate::platform::DatabasePlatform;
use crate::r#type::Type;
use crate::schema::ColumnData;
use crate::{Error, ParameterType, Result, Value};

pub struct IntegerType {}

impl Type for IntegerType {
    fn default() -> Box<dyn Type + Sync + Send> {
        Box::new(IntegerType {})
    }

    fn convert_to_value(&self, value: Option<&str>, _: &dyn DatabasePlatform) -> Result<Value> {
        if let Some(value) = value {
            if value.is_empty() {
                Ok(Value::NULL)
            } else {
                if let Ok(result) = value.parse() {
                    Ok(Value::Int(result))
                } else {
                    Err(Error::conversion_failed_invalid_type(
                        &Value::String(value.to_string()),
                        self.get_name(),
                        &["NULL", "Integer"],
                    ))
                }
            }
        } else {
            Ok(Value::NULL)
        }
    }

    fn get_name(&self) -> &'static str {
        super::INTEGER
    }

    fn get_sql_declaration(
        &self,
        column: &ColumnData,
        platform: &dyn DatabasePlatform,
    ) -> Result<String> {
        platform.get_integer_type_declaration_sql(column)
    }

    fn get_binding_type(&self) -> ParameterType {
        ParameterType::Integer
    }
}

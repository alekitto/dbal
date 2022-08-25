use crate::platform::DatabasePlatform;
use crate::r#type::Type;
use crate::schema::ColumnData;
use crate::{Error, ParameterType, Result, Value};

pub struct IntegerType {}

impl Type for IntegerType {
    fn default() -> Box<dyn Type + Sync + Send> {
        Box::new(IntegerType {})
    }

    fn convert_to_value(&self, value: &Value, _: &dyn DatabasePlatform) -> Result<Value> {
        match value {
            Value::NULL | Value::Int(_) | Value::UInt(_) => Ok(value.clone()),
            Value::String(str) => Ok(Value::Int(str.parse()?)),
            _ => Err(Error::conversion_failed_invalid_type(
                &value,
                self.get_name(),
                &["NULL", "Integer"],
            )),
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

use crate::platform::DatabasePlatform;
use crate::r#type::Type;
use crate::schema::ColumnData;
use crate::{Error, Value};
use crate::{ParameterType, Result};

pub struct DecimalType {}

impl Type for DecimalType {
    fn default() -> Box<dyn Type + Sync + Send> {
        Box::new(DecimalType {})
    }

    fn convert_to_database_value(&self, value: Value, _: &dyn DatabasePlatform) -> Result<Value> {
        match value {
            Value::NULL => Ok(value),
            Value::String(_) => Ok(value),
            Value::Int(i) => Ok(Value::String(i.to_string())),
            Value::UInt(u) => Ok(Value::String(u.to_string())),
            Value::Float(f) => Ok(Value::String(f.to_string())),
            _ => Err(Error::conversion_failed_invalid_type(
                &value,
                self.get_name(),
                &["NULL", "String"],
            )),
        }
    }

    fn get_name(&self) -> &'static str {
        super::DECIMAL
    }

    fn get_sql_declaration(
        &self,
        column: &ColumnData,
        platform: &dyn DatabasePlatform,
    ) -> Result<String> {
        platform.get_binary_type_declaration_sql(column)
    }

    fn get_binding_type(&self) -> ParameterType {
        ParameterType::Binary
    }
}

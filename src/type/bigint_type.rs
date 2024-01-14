use crate::platform::DatabasePlatform;
use crate::r#type::Type;
use crate::schema::ColumnData;
use crate::{Error, Value};
use crate::{ParameterType, Result};

pub struct BigintType {}

impl Type for BigintType {
    fn default() -> Box<dyn Type + Sync + Send> {
        Box::new(BigintType {})
    }

    fn convert_to_value(&self, value: &Value, _: &dyn DatabasePlatform) -> Result<Value> {
        match value {
            Value::NULL | Value::Int(_) | Value::UInt(_) => Ok(value.clone()),
            Value::String(str) => Ok(Value::Int(str.parse()?)),
            _ => Err(Error::conversion_failed_invalid_type(
                value,
                self.get_name(),
                &["NULL", "String", "Integer"],
            )),
        }
    }

    fn convert_to_database_value(&self, value: Value, _: &dyn DatabasePlatform) -> Result<Value> {
        match value {
            Value::NULL | Value::Int(_) | Value::UInt(_) => Ok(value),
            _ => Err(Error::conversion_failed_invalid_type(
                &value,
                self.get_name(),
                &["NULL", "Integer"],
            )),
        }
    }

    fn get_name(&self) -> &'static str {
        super::BIGINT
    }

    fn get_sql_declaration(
        &self,
        column: &ColumnData,
        platform: &dyn DatabasePlatform,
    ) -> Result<String> {
        platform.get_bigint_type_declaration_sql(column)
    }

    fn get_binding_type(&self) -> ParameterType {
        ParameterType::Integer
    }

    fn convert_to_default_value(&self, value: &Value, _: &dyn DatabasePlatform) -> Result<String> {
        match value {
            Value::NULL => Ok(0.to_string()),
            Value::Int(n) => Ok(n.to_string()),
            Value::UInt(n) => Ok(n.to_string()),
            Value::String(s) => Ok(s.to_string()),
            _ => Err(Error::conversion_failed_invalid_type(
                value,
                self.get_name(),
                &["NULL", "String", "Integer"],
            )),
        }
    }
}

use crate::platform::DatabasePlatform;
use crate::r#type::Type;
use crate::schema::ColumnData;
use crate::Result;
use crate::{Error, Value};
use std::io::Read;

pub struct StringType {}

impl Type for StringType {
    fn default() -> Box<dyn Type + Sync + Send> {
        Box::new(StringType {})
    }

    fn convert_to_value(&self, value: &Value, _: &dyn DatabasePlatform) -> Result<Value> {
        match value {
            Value::NULL | Value::String(_) => Ok(value.clone()),
            Value::Int(v) => Ok(Value::String(v.to_string())),
            Value::UInt(v) => Ok(Value::String(v.to_string())),
            Value::Float(v) => Ok(Value::String(v.to_string())),
            Value::Uuid(v) => Ok(Value::String(v.to_string())),
            Value::Bytes(v) => {
                let mut s = String::new();
                v.as_slice()
                    .read_to_string(&mut s)
                    .map(|_| Value::String(s))
                    .map_err(|e| e.into())
            }
            _ => Err(Error::conversion_failed_invalid_type(
                value,
                self.get_name(),
                &["NULL", "String"],
            )),
        }
    }

    fn convert_to_database_value(&self, value: Value, _: &dyn DatabasePlatform) -> Result<Value> {
        match value {
            Value::NULL | Value::String(_) => Ok(value),
            _ => Err(Error::conversion_failed_invalid_type(
                &value,
                self.get_name(),
                &["NULL", "String"],
            )),
        }
    }

    fn get_name(&self) -> &'static str {
        super::STRING
    }

    fn get_sql_declaration(
        &self,
        column: &ColumnData,
        platform: &dyn DatabasePlatform,
    ) -> Result<String> {
        platform.get_string_type_declaration_sql(column)
    }
}

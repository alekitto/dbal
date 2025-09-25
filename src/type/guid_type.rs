use crate::Result;
use crate::platform::DatabasePlatform;
use crate::schema::ColumnData;
use crate::r#type::Type;
use crate::{Error, Value};

pub struct GuidType {}

impl Type for GuidType {
    fn default() -> Box<dyn Type + Sync + Send> {
        Box::new(GuidType {})
    }

    fn convert_to_value(&self, value: &Value, _: &dyn DatabasePlatform) -> Result<Value> {
        match value {
            Value::NULL | Value::String(_) => Ok(value.clone()),
            Value::Int(v) => Ok(Value::String(v.to_string())),
            Value::UInt(v) => Ok(Value::String(v.to_string())),
            Value::Float(v) => Ok(Value::String(v.to_string())),
            Value::Uuid(v) => Ok(Value::String(v.to_string())),
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
        super::GUID
    }

    fn requires_sql_comment_hint(&self, platform: &dyn DatabasePlatform) -> bool {
        !platform.has_native_guid_type()
    }

    fn get_sql_declaration(
        &self,
        column: &ColumnData,
        platform: &dyn DatabasePlatform,
    ) -> Result<String> {
        platform.get_guid_type_declaration_sql(column)
    }
}

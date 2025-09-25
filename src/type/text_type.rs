use crate::Result;
use crate::platform::DatabasePlatform;
use crate::schema::ColumnData;
use crate::r#type::Type;
use crate::{Error, Value};

pub struct TextType {}

impl Type for TextType {
    fn default() -> Box<dyn Type + Sync + Send> {
        Box::new(TextType {})
    }

    fn convert_to_value(&self, value: &Value, _: &dyn DatabasePlatform) -> Result<Value> {
        match value {
            Value::NULL | Value::String(_) => Ok(value.clone()),
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
        super::TEXT
    }

    fn get_sql_declaration(
        &self,
        column: &ColumnData,
        platform: &dyn DatabasePlatform,
    ) -> Result<String> {
        platform.get_clob_type_declaration_sql(column)
    }
}

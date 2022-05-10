use crate::platform::DatabasePlatform;
use crate::r#type::Type;
use crate::schema::ColumnData;
use crate::Result;
use crate::{Error, Value};

pub struct StringType {}

impl Type for StringType {
    fn default() -> Box<dyn Type + Sync + Send> {
        Box::new(StringType {})
    }

    fn convert_to_database_value(&self, value: Value, _: &dyn DatabasePlatform) -> Result<Value> {
        match value {
            Value::NULL => Ok(value),
            Value::String(_) => Ok(value),
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

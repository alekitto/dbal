use crate::platform::DatabasePlatform;
use crate::r#type::Type;
use crate::schema::ColumnData;
use crate::Result;
use crate::{Error, Value};
use chrono::DateTime;

pub struct TimeType {}

impl Type for TimeType {
    fn default() -> Box<dyn Type + Sync + Send> {
        Box::new(TimeType {})
    }

    fn convert_to_database_value(
        &self,
        value: Value,
        platform: &dyn DatabasePlatform,
    ) -> Result<Value> {
        match value {
            Value::NULL => Ok(value),
            Value::DateTime(dt) => Ok(Value::String(
                dt.format(platform.get_time_format_string()).to_string(),
            )),
            _ => Err(Error::conversion_failed_invalid_type(
                &value,
                self.get_name(),
                &["NULL", "DateTime"],
            )),
        }
    }

    fn convert_to_value(
        &self,
        value: Option<&str>,
        platform: &dyn DatabasePlatform,
    ) -> Result<Value> {
        if let Some(value) = value {
            if value.is_empty() {
                Ok(Value::NULL)
            } else {
                let dt = DateTime::parse_from_str(value, platform.get_time_format_string())?;
                Ok(Value::DateTime(DateTime::from(dt)))
            }
        } else {
            Ok(Value::NULL)
        }
    }

    fn get_name(&self) -> &'static str {
        super::TIME
    }

    fn get_sql_declaration(
        &self,
        column: &ColumnData,
        platform: &dyn DatabasePlatform,
    ) -> Result<String> {
        platform.get_time_type_declaration_sql(column)
    }
}

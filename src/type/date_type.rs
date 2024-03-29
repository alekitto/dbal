use crate::platform::DatabasePlatform;
use crate::r#type::Type;
use crate::schema::ColumnData;
use crate::Result;
use crate::{Error, Value};
use chrono::DateTime;

pub struct DateType {}

impl Type for DateType {
    fn default() -> Box<dyn Type + Sync + Send> {
        Box::new(DateType {})
    }

    fn convert_to_database_value(
        &self,
        value: Value,
        platform: &dyn DatabasePlatform,
    ) -> Result<Value> {
        match value {
            Value::NULL => Ok(value),
            Value::DateTime(dt) => Ok(Value::String(
                dt.format(platform.get_date_format_string()).to_string(),
            )),
            _ => Err(Error::conversion_failed_invalid_type(
                &value,
                self.get_name(),
                &["NULL", "DateTime"],
            )),
        }
    }

    fn convert_to_value(&self, value: &Value, platform: &dyn DatabasePlatform) -> Result<Value> {
        match value {
            Value::NULL | Value::DateTime(_) => Ok(value.clone()),
            Value::String(str) => {
                let dt = DateTime::parse_from_str(str, platform.get_date_format_string())?;
                Ok(Value::DateTime(dt.into()))
            }
            _ => Err(Error::conversion_failed_invalid_type(
                value,
                self.get_name(),
                &["NULL", "DateTime", "String"],
            )),
        }
    }

    fn get_name(&self) -> &'static str {
        super::DATE
    }

    fn get_sql_declaration(
        &self,
        column: &ColumnData,
        platform: &dyn DatabasePlatform,
    ) -> Result<String> {
        platform.get_date_type_declaration_sql(column)
    }

    fn convert_to_default_value(
        &self,
        value: &Value,
        platform: &dyn DatabasePlatform,
    ) -> Result<String> {
        match value {
            Value::NULL => Ok("''".to_string()),
            Value::String(s) => {
                if s == platform.get_current_date_sql() {
                    Ok(platform.get_current_date_sql().to_string())
                } else {
                    Ok(platform.quote_string_literal(s))
                }
            }
            Value::DateTime(dt) => Ok(platform
                .quote_string_literal(&dt.format(platform.get_date_format_string()).to_string())),
            _ => Err(Error::conversion_failed_invalid_type(
                value,
                self.get_name(),
                &["NULL", "String", "DateTime"],
            )),
        }
    }
}

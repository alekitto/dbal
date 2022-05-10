use crate::platform::DatabasePlatform;
use crate::r#type::Type;
use crate::schema::ColumnData;
use crate::{Error, Result, Value};
use chrono::DateTime;

pub struct DateTimeTzType {}

impl Type for DateTimeTzType {
    fn default() -> Box<dyn Type + Sync + Send> {
        Box::new(DateTimeTzType {})
    }

    fn convert_to_database_value(
        &self,
        value: Value,
        platform: &dyn DatabasePlatform,
    ) -> Result<Value> {
        match &value {
            Value::NULL => Ok(Value::NULL),
            Value::DateTime(dt) => Ok(Value::String(
                dt.format(platform.get_date_time_tz_format_string())
                    .to_string(),
            )),
            _ => Err(Error::conversion_failed_invalid_type(
                &value,
                self.get_name(),
                &["NULL", "DateTime<Tz>"],
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
                if let Ok(dt) =
                    DateTime::parse_from_str(value, platform.get_date_time_tz_format_string())
                {
                    Ok(Value::DateTime(dt.into()))
                } else {
                    Err(Error::conversion_failed_invalid_type(
                        &Value::String(value.to_string()),
                        self.get_name(),
                        &["NULL", "DateTime<Tz>"],
                    ))
                }
            }
        } else {
            Ok(Value::NULL)
        }
    }

    fn get_name(&self) -> &'static str {
        super::DATETIMETZ
    }

    fn requires_sql_comment_hint(&self, _: &dyn DatabasePlatform) -> bool {
        true
    }

    fn get_sql_declaration(
        &self,
        column: &ColumnData,
        platform: &dyn DatabasePlatform,
    ) -> Result<String> {
        platform.get_date_time_tz_type_declaration_sql(column)
    }
}

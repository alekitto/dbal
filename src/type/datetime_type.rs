use crate::error::ErrorKind;
use crate::platform::DatabasePlatform;
use crate::schema::ColumnData;
use crate::r#type::Type;
use crate::{Error, Result, Value};
use chrono::{DateTime, Local, NaiveDateTime};

pub struct DateTimeType {}

impl Type for DateTimeType {
    fn default() -> Box<dyn Type + Sync + Send> {
        Box::new(DateTimeType {})
    }

    fn convert_to_database_value(
        &self,
        value: Value,
        platform: &dyn DatabasePlatform,
    ) -> Result<Value> {
        match &value {
            Value::NULL => Ok(Value::NULL),
            Value::DateTime(dt) => Ok(Value::String(
                dt.format(platform.get_date_time_format_string())
                    .to_string(),
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
            Value::String(value) => {
                if value.is_empty() {
                    Ok(Value::NULL)
                } else {
                    NaiveDateTime::parse_from_str(value, platform.get_date_time_format_string())
                        .map_err(|e| {
                            Error::new(
                                ErrorKind::ConversionFailed,
                                format!(r#"conversion failed for value "{}": {}"#, value, e),
                            )
                        })
                        .and_then(|ndt| {
                            ndt.and_local_timezone(Local).earliest().ok_or_else(|| {
                                Error::new(ErrorKind::ConversionFailed, "conversion failed")
                            })
                        })
                        .map(Value::DateTime)
                }
            }
            _ => Err(Error::conversion_failed_invalid_type(
                value,
                self.get_name(),
                &["NULL", "DateTime", "String"],
            )),
        }
    }

    fn get_name(&self) -> &'static str {
        super::DATETIME
    }

    fn requires_sql_comment_hint(&self, _: &dyn DatabasePlatform) -> bool {
        true
    }

    fn get_sql_declaration(
        &self,
        column: &ColumnData,
        platform: &dyn DatabasePlatform,
    ) -> Result<String> {
        platform.get_date_time_type_declaration_sql(column)
    }

    fn convert_to_default_value(
        &self,
        value: &Value,
        platform: &dyn DatabasePlatform,
    ) -> Result<String> {
        match value {
            Value::NULL => Ok("''".to_string()),
            Value::String(s) => {
                if s == platform.get_current_timestamp_sql() {
                    Ok(platform.get_current_timestamp_sql().to_string())
                } else {
                    let dt = DateTime::parse_from_str(s, platform.get_date_time_format_string())?;

                    Ok(platform.quote_string_literal(
                        &dt.format(platform.get_date_time_format_string())
                            .to_string(),
                    ))
                }
            }
            Value::DateTime(dt) => Ok(platform.quote_string_literal(
                &dt.format(platform.get_date_time_format_string())
                    .to_string(),
            )),
            _ => Err(Error::conversion_failed_invalid_type(
                value,
                self.get_name(),
                &["NULL", "String", "Bytes"],
            )),
        }
    }
}

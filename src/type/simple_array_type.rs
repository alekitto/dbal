use crate::Value;
use crate::platform::DatabasePlatform;
use crate::schema::ColumnData;
use crate::r#type::Type;
use crate::{Error, Result};
use itertools::Itertools;

/// Array Type which can be used for simple values.
/// Only use this type if you are sure that your values cannot contain a ",".
pub struct SimpleArrayType {}

impl Type for SimpleArrayType {
    fn default() -> Box<dyn Type + Sync + Send> {
        Box::new(SimpleArrayType {})
    }

    fn convert_to_value(&self, value: &Value, _: &dyn DatabasePlatform) -> Result<Value> {
        match value {
            Value::NULL => Ok(value.clone()),
            Value::Array(vec) => {
                if vec.iter().all(|e| matches!(e, Value::String(_))) {
                    Ok(value.clone())
                } else {
                    Err(Error::conversion_failed_invalid_type(
                        value,
                        self.get_name(),
                        &["NULL", "Array-of-strings", "String"],
                    ))
                }
            }
            Value::String(value) => Ok(Value::Array(
                value
                    .split(',')
                    .map(ToString::to_string)
                    .map(Value::from)
                    .collect(),
            )),
            _ => Err(Error::conversion_failed_invalid_type(
                value,
                self.get_name(),
                &["NULL", "String"],
            )),
        }
    }

    fn convert_to_database_value(&self, value: Value, _: &dyn DatabasePlatform) -> Result<Value> {
        match value {
            Value::Array(ref vec) => {
                if vec.iter().all(|e| matches!(e, Value::String(_))) {
                    Ok(Value::String(
                        vec.iter()
                            .map(|el| {
                                if let Value::String(el) = el {
                                    el
                                } else {
                                    unreachable!()
                                }
                            })
                            .join(","),
                    ))
                } else {
                    Err(Error::conversion_failed_invalid_type(
                        &value,
                        self.get_name(),
                        &["NULL", "Array-of-strings"],
                    ))
                }
            }
            _ => Ok(Value::NULL),
        }
    }

    fn get_name(&self) -> &'static str {
        super::SIMPLE_ARRAY
    }

    fn get_sql_declaration(
        &self,
        column: &ColumnData,
        platform: &dyn DatabasePlatform,
    ) -> Result<String> {
        platform.get_clob_type_declaration_sql(column)
    }

    fn requires_sql_comment_hint(&self, _: &dyn DatabasePlatform) -> bool {
        true
    }
}

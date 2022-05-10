use crate::platform::DatabasePlatform;
use crate::r#type::Type;
use crate::schema::ColumnData;
use crate::{Error, Value};
use crate::{ParameterType, Result};

/// Array Type which can be used for simple values.
/// Only use this type if you are sure that your values cannot contain a ",".
pub struct SimpleArrayType {}

impl Type for SimpleArrayType {
    fn default() -> Box<dyn Type + Sync + Send> {
        Box::new(SimpleArrayType {})
    }

    fn convert_to_value(&self, value: Option<&str>, _: &dyn DatabasePlatform) -> Result<Value> {
        Ok(if let Some(value) = value {
            if value.is_empty() {
                Value::NULL
            } else {
                Value::VecString(
                    value
                        .to_string()
                        .split(",")
                        .map(ToString::to_string)
                        .collect(),
                )
            }
        } else {
            Value::NULL
        })
    }

    fn convert_to_database_value(
        &self,
        value: Value,
        platform: &dyn DatabasePlatform,
    ) -> Result<Value> {
        Ok(match value {
            Value::VecString(vec) => Value::String(vec.join(",")),
            _ => Value::NULL,
        })
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
}

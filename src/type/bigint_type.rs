use crate::platform::DatabasePlatform;
use crate::r#type::Type;
use crate::schema::ColumnData;
use crate::{Error, Value};
use crate::{ParameterType, Result};

pub struct BigintType {}

impl Type for BigintType {
    fn default() -> Box<dyn Type + Sync + Send> {
        Box::new(BigintType {})
    }

    fn convert_to_value(&self, value: Option<&str>, _: &dyn DatabasePlatform) -> Result<Value> {
        Ok(if let Some(value) = value {
            if value.is_empty() {
                Value::NULL
            } else {
                Value::String(value.to_string())
            }
        } else {
            Value::NULL
        })
    }

    fn get_name(&self) -> &'static str {
        super::BIGINT
    }

    fn get_sql_declaration(
        &self,
        column: &ColumnData,
        platform: &dyn DatabasePlatform,
    ) -> Result<String> {
        platform.get_bigint_type_declaration_sql(column)
    }
}

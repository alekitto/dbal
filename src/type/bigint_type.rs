use crate::platform::DatabasePlatform;
use crate::r#type::Type;
use crate::schema::ColumnData;
use crate::Result;
use crate::{Error, Value};

pub struct BigintType {}

impl Type for BigintType {
    fn default() -> Box<dyn Type + Sync + Send> {
        Box::new(BigintType {})
    }

    fn convert_to_value(&self, value: &Value, _: &dyn DatabasePlatform) -> Result<Value> {
        match value {
            Value::NULL | Value::String(_) => Ok(value.clone()),
            Value::Int(val) => Ok(Value::String(val.to_string())),
            _ => Err(Error::conversion_failed_invalid_type(
                &value,
                "Bigint",
                &["NULL", "String", "Integer"],
            )),
        }
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

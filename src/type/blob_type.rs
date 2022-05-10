use crate::platform::DatabasePlatform;
use crate::r#type::Type;
use crate::schema::ColumnData;
use crate::{Error, Value};
use crate::{ParameterType, Result};

pub struct BlobType {}

impl Type for BlobType {
    fn default() -> Box<dyn Type + Sync + Send> {
        Box::new(BlobType {})
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
        super::BLOB
    }

    fn get_sql_declaration(
        &self,
        column: &ColumnData,
        platform: &dyn DatabasePlatform,
    ) -> Result<String> {
        platform.get_blob_type_declaration_sql(column)
    }

    fn get_binding_type(&self) -> ParameterType {
        ParameterType::LargeObject
    }
}

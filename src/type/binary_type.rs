use crate::platform::DatabasePlatform;
use crate::r#type::Type;
use crate::schema::ColumnData;
use crate::Value;
use crate::{ParameterType, Result};

pub struct BinaryType {}

impl Type for BinaryType {
    fn default() -> Box<dyn Type + Sync + Send> {
        Box::new(BinaryType {})
    }

    fn convert_to_value(
        &self,
        value: Option<&str>,
        platform: &dyn DatabasePlatform,
    ) -> Result<Value> {
        todo!()
    }

    fn get_name(&self) -> &'static str {
        super::BINARY
    }

    fn get_sql_declaration(
        &self,
        column: &ColumnData,
        platform: &dyn DatabasePlatform,
    ) -> Result<String> {
        platform.get_binary_type_declaration_sql(column)
    }

    fn get_binding_type(&self) -> ParameterType {
        ParameterType::Binary
    }
}

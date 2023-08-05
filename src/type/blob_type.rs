use crate::platform::DatabasePlatform;
use crate::r#type::Type;
use crate::schema::ColumnData;
use crate::{Error, Value};
use crate::{ParameterType, Result};
use lazy_static::lazy_static;
use regex::Regex;

lazy_static! {
    static ref BINARY_REGEX: Regex =
        Regex::new("^(?:[xX]'([a-fA-F0-9]*)'|\\\\x([a-fA-F0-9]+))$").unwrap();
}

pub struct BlobType {}
impl Type for BlobType {
    fn default() -> Box<dyn Type + Sync + Send> {
        Box::new(BlobType {})
    }

    fn convert_to_database_value(&self, value: Value, _: &dyn DatabasePlatform) -> Result<Value> {
        match value {
            Value::NULL => Ok(value),
            Value::String(_) => Ok(value),
            Value::Bytes(b) => Ok(Value::String(format!("x'{}'", hex::encode(b)))),
            _ => Err(Error::conversion_failed_invalid_type(
                &value,
                self.get_name(),
                &["NULL", "String", "Bytes"],
            )),
        }
    }

    fn convert_to_value(&self, value: &Value, _: &dyn DatabasePlatform) -> Result<Value> {
        match value {
            Value::NULL => Ok(Value::NULL),
            Value::String(s) => {
                if let Some(m) = BINARY_REGEX.captures(s) {
                    Ok(m.get(1)
                        .or_else(|| m.get(2))
                        .map_or(Ok(vec![]), |v| hex::decode(v.as_str()))?
                        .into())
                } else {
                    Ok(Value::String(s.clone()))
                }
            }
            Value::Bytes(_) => Ok(value.clone()),
            _ => Err(Error::conversion_failed_invalid_type(
                value,
                self.get_name(),
                &["NULL", "String", "Bytes"],
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

    fn convert_to_default_value(
        &self,
        value: &Value,
        platform: &dyn DatabasePlatform,
    ) -> Result<String> {
        match value {
            Value::NULL => Ok("''".to_string()),
            Value::String(s) => Ok(platform.quote_string_literal(s)),
            Value::Bytes(b) => Ok(format!("x'{}'", hex::encode(b))),
            _ => Err(Error::conversion_failed_invalid_type(
                value,
                self.get_name(),
                &["NULL", "String", "Bytes"],
            )),
        }
    }
}

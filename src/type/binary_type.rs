use crate::platform::DatabasePlatform;
use crate::r#type::Type;
use crate::schema::ColumnData;
use crate::{Error, Value};
use crate::{ParameterType, Result};
use lazy_static::lazy_static;
use regex::Regex;

lazy_static! {
    static ref BINARY_REGEX: Regex = Regex::new("^[0\\\\]?[xX]([a-fA-F0-9]+)$").unwrap();
}

pub struct BinaryType {}
impl Type for BinaryType {
    fn default() -> Box<dyn Type + Sync + Send> {
        Box::new(BinaryType {})
    }

    fn convert_to_value(&self, value: &Value, _: &dyn DatabasePlatform) -> Result<Value> {
        match value {
            Value::NULL => Ok(Value::NULL),
            Value::String(s) => {
                if let Some(m) = BINARY_REGEX.captures(s) {
                    Ok(m.get(1)
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

    fn convert_to_default_value(
        &self,
        value: &Value,
        platform: &dyn DatabasePlatform,
    ) -> Result<String> {
        match value {
            Value::NULL => Ok("''".to_string()),
            Value::String(s) => Ok(platform.quote_string_literal(s)),
            Value::Bytes(b) => {
                let encoded = hex::encode(b);
                if platform.get_name() == "postgresql" {
                    Ok(format!("'\\x{}'::bytea", encoded))
                } else {
                    Ok(format!("x'{}'", encoded))
                }
            }
            _ => Err(Error::conversion_failed_invalid_type(
                value,
                self.get_name(),
                &["NULL", "String", "Bytes"],
            )),
        }
    }
}

use chrono::{DateTime, TimeZone, Local};
use std::cmp::Ordering;

#[derive(Debug, Clone)]
pub enum Value {
    NULL,
    Int(i64),
    UInt(u64),
    String(String),
    Bytes(Vec<u8>),
    Float(f64),
    Boolean(bool),

    /// date-time
    DateTime(chrono::DateTime<Local>),

    /// json
    Json(serde_json::Value),

    /// uuid
    Uuid(uuid::Uuid),
}

impl Value {
    fn is_null(&self) -> bool {
        match self {
            Value::NULL => true,
            _ => false
        }
    }

    fn is_string_eq(&self, str: &String) -> bool {
        match self {
            Value::String(cur) => cur == str,
            _ => false,
        }
    }

    fn is_int_eq(&self, val: &i64) -> bool {
        match self {
            Value::Int(cur) => cur == val,
            _ => false,
        }
    }

    fn is_uint_eq(&self, val: &u64) -> bool {
        match self {
            Value::UInt(cur) => cur == val,
            _ => false,
        }
    }

    fn is_float_eq(&self, val: &f64) -> bool {
        match self {
            Value::Float(cur) => cur == val,
            _ => false,
        }
    }

    fn is_bytes_eq(&self, val: &Vec<u8>) -> bool {
        match self {
            Value::Bytes(cur) => cur.cmp(val) == Ordering::Equal,
            _ => false,
        }
    }

    fn is_boolean_eq(&self, val: &bool) -> bool {
        match self {
            Value::Boolean(cur) => cur == val,
            _ => false,
        }
    }

    fn is_datetime_eq(&self, val: &chrono::DateTime<Local>) -> bool {
        match self {
            Value::DateTime(cur) => cur == val,
            _ => false,
        }
    }

    fn is_json_eq(&self, val: &serde_json::Value) -> bool {
        match self {
            Value::Json(cur) => cur == val,
            _ => false,
        }
    }

    fn is_uuid_eq(&self, val: &uuid::Uuid) -> bool {
        match self {
            Value::Uuid(cur) => cur == val,
            _ => false,
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match self {
            Value::NULL => other.is_null(),
            Value::Int(value) => other.is_int_eq(value),
            Value::UInt(value) => other.is_uint_eq(value),
            Value::String(value) => other.is_string_eq(value),
            Value::Bytes(value) => other.is_bytes_eq(value),
            Value::Float(value) => other.is_float_eq(value),
            Value::Boolean(value) => other.is_boolean_eq(value),
            Value::DateTime(value) => other.is_datetime_eq(value),
            Value::Json(value) => other.is_json_eq(value),
            Value::Uuid(value) => other.is_uuid_eq(value),
        }
    }

    fn ne(&self, other: &Self) -> bool {
        ! self.eq(other)
    }
}

impl From<i8> for Value {
    #[inline]
    fn from(value: i8) -> Self {
        Value::Int(value as i64)
    }
}

impl From<&i8> for Value {
    #[inline]
    fn from(value: &i8) -> Self {
        Value::Int(*value as i64)
    }
}

impl From<i16> for Value {
    #[inline]
    fn from(value: i16) -> Self {
        Value::Int(value as i64)
    }
}

impl From<&i16> for Value {
    #[inline]
    fn from(value: &i16) -> Self {
        Value::Int(*value as i64)
    }
}

impl From<i32> for Value {
    #[inline]
    fn from(value: i32) -> Self {
        Value::Int(value as i64)
    }
}

impl From<&i32> for Value {
    #[inline]
    fn from(value: &i32) -> Self {
        Value::Int(*value as i64)
    }
}

impl From<i64> for Value {
    #[inline]
    fn from(value: i64) -> Self {
        Value::Int(value)
    }
}

impl From<&i64> for Value {
    #[inline]
    fn from(value: &i64) -> Self {
        Value::Int(*value)
    }
}

impl From<u8> for Value {
    #[inline]
    fn from(value: u8) -> Self {
        Value::UInt(value as u64)
    }
}

impl From<&u8> for Value {
    #[inline]
    fn from(value: &u8) -> Self {
        Value::UInt(*value as u64)
    }
}

impl From<u16> for Value {
    #[inline]
    fn from(value: u16) -> Self {
        Value::UInt(value as u64)
    }
}

impl From<&u16> for Value {
    #[inline]
    fn from(value: &u16) -> Self {
        Value::UInt(*value as u64)
    }
}

impl From<u32> for Value {
    #[inline]
    fn from(value: u32) -> Self {
        Value::UInt(value as u64)
    }
}

impl From<&u32> for Value {
    #[inline]
    fn from(value: &u32) -> Self {
        Value::UInt(*value as u64)
    }
}

impl From<u64> for Value {
    #[inline]
    fn from(value: u64) -> Self {
        Value::UInt(value)
    }
}

impl From<&u64> for Value {
    #[inline]
    fn from(value: &u64) -> Self {
        Value::UInt(*value)
    }
}

impl From<String> for Value {
    #[inline]
    fn from(value: String) -> Self {
        Value::String(value)
    }
}

impl From<&String> for Value {
    #[inline]
    fn from(value: &String) -> Self {
        Value::String(value.into())
    }
}

impl From<&str> for Value {
    #[inline]
    fn from(value: &str) -> Self {
        Value::String(value.into())
    }
}

impl From<f32> for Value {
    #[inline]
    fn from(value: f32) -> Self {
        Value::Float(value as f64)
    }
}

impl From<&f32> for Value {
    #[inline]
    fn from(value: &f32) -> Self {
        Value::Float(*value as f64)
    }
}

impl From<f64> for Value {
    #[inline]
    fn from(value: f64) -> Self {
        Value::Float(value)
    }
}

impl From<&f64> for Value {
    #[inline]
    fn from(value: &f64) -> Self {
        Value::Float(*value)
    }
}

impl From<bool> for Value {
    #[inline]
    fn from(value: bool) -> Self {
        Value::Boolean(value)
    }
}

impl From<&bool> for Value {
    #[inline]
    fn from(value: &bool) -> Self {
        Value::Boolean(*value)
    }
}

impl From<Vec<u8>> for Value {
    #[inline]
    fn from(value: Vec<u8>) -> Self {
        Value::Bytes(value)
    }
}

impl From<&Vec<u8>> for Value {
    #[inline]
    fn from(value: &Vec<u8>) -> Self {
        Value::Bytes(value.clone())
    }
}

impl<Tz: TimeZone> From<DateTime<Tz>> for Value {
    #[inline]
    fn from(value: DateTime<Tz>) -> Self {
        Value::DateTime(Local.from_utc_datetime(&value.naive_utc()))
    }
}

impl<Tz: TimeZone> From<&DateTime<Tz>> for Value {
    #[inline]
    fn from(value: &DateTime<Tz>) -> Self {
        let value = value.clone();
        Value::DateTime(Local.from_utc_datetime(&value.naive_utc()))
    }
}

impl From<serde_json::Value> for Value {
    #[inline]
    fn from(value: serde_json::Value) -> Self {
        Value::Json(value)
    }
}

impl From<uuid::Uuid> for Value {
    #[inline]
    fn from(value: uuid::Uuid) -> Self {
        Value::Uuid(value)
    }
}

impl From<&uuid::Uuid> for Value {
    #[inline]
    fn from(value: &uuid::Uuid) -> Self {
        Value::Uuid(value.clone())
    }
}

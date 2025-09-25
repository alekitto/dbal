use crate::parameter::{IntoParameter, IntoParameters};
use crate::platform::DatabasePlatform;
use crate::private::Sealed;
use crate::r#type::{IntoType, TypePtr};
use crate::{Error, Parameter, ParameterIndex, ParameterType, Parameters, Result as CreedResult};
use chrono::{DateTime, Local, TimeZone, Utc};
use itertools::Itertools;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::hash_map::{IntoIter, IntoValues, Keys, Values};
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Default)]
pub enum Value {
    #[default]
    NULL,
    Int(i64),
    UInt(u64),
    String(String),
    Bytes(Vec<u8>),
    Float(f64),
    Boolean(bool),

    Array(Vec<Value>),

    /// date-time
    DateTime(DateTime<Local>),

    /// json
    Json(serde_json::Value),

    /// uuid
    Uuid(uuid::Uuid),
}

impl Default for &Value {
    fn default() -> Self {
        &Value::NULL
    }
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::NULL)
    }

    fn is_string_eq(&self, str: &str) -> bool {
        match self {
            Value::String(cur) => cur == str,
            _ => false,
        }
    }

    fn is_int_eq(&self, val: i64) -> bool {
        match *self {
            Value::Int(cur) => cur == val,
            _ => false,
        }
    }

    fn is_uint_eq(&self, val: u64) -> bool {
        match *self {
            Value::UInt(cur) => cur == val,
            _ => false,
        }
    }

    fn is_float_eq(&self, val: f64) -> bool {
        match *self {
            Value::Float(cur) => cur == val,
            _ => false,
        }
    }

    fn is_bytes_eq(&self, val: &[u8]) -> bool {
        match self {
            Value::Bytes(cur) => cur.as_slice().cmp(val) == Ordering::Equal,
            _ => false,
        }
    }

    fn is_boolean_eq(&self, val: bool) -> bool {
        match *self {
            Value::Boolean(cur) => cur == val,
            _ => false,
        }
    }

    fn is_datetime_eq<Tz: TimeZone>(&self, val: &DateTime<Tz>) -> bool {
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

    pub fn try_into_vec(self) -> CreedResult<Vec<Value>> {
        match self {
            Value::Array(v) => Ok(v),
            _ => Err(Error::type_mismatch()),
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            Value::NULL => "NULL".to_string(),
            Value::Int(value) => value.to_string(),
            Value::UInt(value) => value.to_string(),
            Value::String(value) => value.clone(),
            Value::Bytes(value) => format!("Bytes (len: {}) <{:02X?}>", value.len(), value),
            Value::Float(value) => value.to_string(),
            Value::Boolean(value) => (if *value { "true" } else { "false" }).to_string(),
            Value::DateTime(value) => value.to_string(),
            Value::Array(value) => format!("Array (len: {}) {:?}", value.len(), value),
            Value::Json(value) => value.to_string(),
            Value::Uuid(value) => value.to_string(),
        };

        write!(f, "{}", str)
    }
}

impl Eq for Value {}
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match self {
            Value::NULL => other.is_null(),
            Value::Int(value) => other.is_int_eq(*value),
            Value::UInt(value) => other.is_uint_eq(*value),
            Value::String(value) => other.is_string_eq(value),
            Value::Bytes(value) => other.is_bytes_eq(value),
            Value::Float(value) => other.is_float_eq(*value),
            Value::Boolean(value) => other.is_boolean_eq(*value),
            Value::DateTime(value) => other.is_datetime_eq(value),
            Value::Json(value) => other.is_json_eq(value),
            Value::Uuid(value) => other.is_uuid_eq(value),
            Value::Array(value) => {
                if let Value::Array(other) = other {
                    value.eq(other)
                } else {
                    false
                }
            }
        }
    }
}

macro from_to_value($variant:ident,$source:ty) {
    impl From<$source> for Value {
        #[inline]
        fn from(value: $source) -> Self {
            Value::$variant(value)
        }
    }

    impl From<Option<$source>> for Value {
        #[inline]
        fn from(value: Option<$source>) -> Self {
            match value {
                None => Value::NULL,
                Some(value) => Value::$variant(value),
            }
        }
    }

    impl TryFrom<Value> for $source {
        type Error = Error;

        #[inline]
        fn try_from(value: Value) -> Result<Self, Self::Error> {
            match value {
                Value::$variant(value) => Ok(value),
                _ => Err(Error::type_mismatch()),
            }
        }
    }
}

macro from_to_value_deref($variant:ident,$source:ty) {
    from_to_value!($variant, $source);

    impl From<&$source> for Value {
        #[inline]
        fn from(value: &$source) -> Self {
            Value::$variant(*value)
        }
    }
}

macro from_to_value_clone($variant:ident,$source:ty) {
    from_to_value!($variant, $source);

    impl From<&$source> for Value {
        #[inline]
        fn from(value: &$source) -> Self {
            Value::$variant(value.clone())
        }
    }
}

// int/uint implementations
macro from_traits_int_impl {
    ($variant:ident,$target:ty,$($source:ty),*) => {$(
        impl From<$source> for Value {
            #[inline]
            fn from(value: $source) -> Self {
                Value::$variant(value as $target)
            }
        }

        impl From<&$source> for Value {
            #[inline]
            fn from(value: &$source) -> Self {
                Value::$variant(*value as $target)
            }
        }

        impl TryFrom<Value> for $source {
            type Error = Error;

            #[inline]
            fn try_from(value: Value) -> Result<Self, Self::Error> {
                match value {
                    Value::Int(value) => Ok(<$source>::try_from(value)?),
                    Value::UInt(value) => Ok(<$source>::try_from(value)?),
                    _ => Err(Error::type_mismatch())
                }
            }
        }

        impl TryFrom<&Value> for $source {
            type Error = Error;

            #[inline]
            fn try_from(value: &Value) -> Result<Self, Self::Error> {
                match value {
                    Value::Int(value) => Ok(<$source>::try_from(*value)?),
                    Value::UInt(value) => Ok(<$source>::try_from(*value)?),
                    _ => Err(Error::type_mismatch())
                }
            }
        }
    )*}
}

macro from_traits_clone_impl {
    ($variant:ident,$($source:ty),*) => {$(
        from_to_value_clone!($variant, $source);

        impl TryFrom<&Value> for $source {
            type Error = Error;

            #[inline]
            fn try_from(value: &Value) -> Result<Self, Self::Error> {
                match value {
                    Value::$variant(value) => Ok(value.clone()),
                    _ => Err(Error::type_mismatch())
                }
            }
        }
    )*}
}

macro from_traits_deref_impl {
    ($variant:ident,$($source:ty),*) => {$(
        from_to_value_deref!($variant, $source);

        impl TryFrom<&Value> for $source {
            type Error = Error;

            #[inline]
            fn try_from(value: &Value) -> Result<Self, Self::Error> {
                match value {
                    Value::$variant(value) => Ok(*value),
                    _ => Err(Error::type_mismatch())
                }
            }
        }
    )*}
}

from_traits_int_impl!(Int, i64, i8, i16, i32, i64, isize);
from_traits_int_impl!(UInt, u64, u8, u16, u32, u64, usize);

from_traits_clone_impl!(String, String);
from_traits_clone_impl!(Json, serde_json::Value);

from_traits_deref_impl!(Uuid, uuid::Uuid);

from_to_value_deref!(Boolean, bool);
impl From<&Value> for bool {
    fn from(item: &Value) -> Self {
        match item {
            Value::NULL => false,
            Value::Int(i) => *i != 0,
            Value::UInt(u) => *u != 0,
            Value::String(s) => !s.is_empty(),
            Value::Float(f) => *f != 0.0,
            Value::Boolean(cur) => *cur,
            Value::Json(j) => match j {
                serde_json::Value::Null => false,
                serde_json::Value::Bool(b) => *b,
                serde_json::Value::Number(n) => {
                    let n = n.clone();
                    n != 0_i64.into() && n != serde_json::Number::from_f64(0.0).unwrap()
                }
                serde_json::Value::String(s) => !s.is_empty(),
                serde_json::Value::Array(a) => !a.is_empty(),
                serde_json::Value::Object(_) => true,
            },
            Value::Bytes(_) | Value::DateTime(_) | Value::Uuid(_) => true,
            Value::Array(vec) => !vec.is_empty(),
        }
    }
}

impl<V: Into<Value> + 'static> From<Vec<V>> for Value {
    fn from(value: Vec<V>) -> Self {
        use std::any::TypeId;
        if TypeId::of::<V>() == TypeId::of::<u8>() {
            Value::Bytes(unsafe { std::mem::transmute::<Vec<V>, Vec<u8>>(value) })
        } else {
            Value::Array(value.into_iter().map(|v| v.into()).collect::<Vec<_>>())
        }
    }
}

impl From<&[u8]> for Value {
    #[inline]
    fn from(value: &[u8]) -> Self {
        Value::Bytes(Vec::from(value))
    }
}

impl<const N: usize> From<&[u8; N]> for Value {
    #[inline]
    fn from(value: &[u8; N]) -> Self {
        Value::Bytes(Vec::from(value.as_slice()))
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

impl TryFrom<Value> for DateTime<Local> {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::DateTime(dt) => Ok(dt),
            _ => Err(Error::type_mismatch()),
        }
    }
}

impl TryFrom<Value> for DateTime<Utc> {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::DateTime(dt) => Ok(dt.into()),
            _ => Err(Error::type_mismatch()),
        }
    }
}

impl<I: AsRef<str> + From<String>> From<Value> for Option<I> {
    fn from(value: Value) -> Self {
        match value {
            Value::NULL => None,
            _ => Some(I::from(value.to_string())),
        }
    }
}

impl<I: AsRef<str> + From<String>> From<&Value> for Option<I> {
    fn from(value: &Value) -> Self {
        match value {
            Value::NULL => None,
            _ => Some(I::from(value.to_string())),
        }
    }
}

impl IntoParameter for Value {
    fn into_parameter(self, _: &dyn DatabasePlatform) -> CreedResult<Parameter> {
        Ok(match self {
            Value::NULL => Parameter::new(self, ParameterType::Null),
            Value::UInt(_) | Value::Int(_) => Parameter::new(self, ParameterType::Integer),
            Value::Float(_) => Parameter::new(self, ParameterType::Float),
            Value::Bytes(_) => Parameter::new(self, ParameterType::Binary),
            Value::Boolean(_) => Parameter::new(self, ParameterType::Boolean),
            _ => Parameter::new(self, ParameterType::String),
        })
    }
}

impl IntoParameter for TypedValue {
    fn into_parameter(self, platform: &dyn DatabasePlatform) -> CreedResult<Parameter> {
        Ok(if let Some(ty) = self.r#type {
            let ty = ty.into_type()?;
            Parameter::new(
                ty.convert_to_database_value(self.value, platform)?,
                ty.get_binding_type(),
            )
        } else {
            match self.value {
                Value::NULL => Parameter::new(self.value, ParameterType::Null),
                Value::UInt(_) | Value::Int(_) => {
                    Parameter::new(self.value, ParameterType::Integer)
                }
                Value::Float(_) => Parameter::new(self.value, ParameterType::Float),
                Value::Bytes(_) => Parameter::new(self.value, ParameterType::Binary),
                Value::Boolean(_) => Parameter::new(self.value, ParameterType::Boolean),
                _ => Parameter::new(self.value, ParameterType::String),
            }
        })
    }
}

pub struct UntypedValueMap<'s>(pub HashMap<&'s str, Value>);
pub struct TypedValueMap<'s>(pub HashMap<&'s str, TypedValue>);

impl Sealed for UntypedValueMap<'_> {}
impl Sealed for TypedValueMap<'_> {}

impl<'s> From<UntypedValueMap<'s>> for HashMap<&'s str, Value> {
    fn from(value: UntypedValueMap<'s>) -> Self {
        value.0
    }
}

impl<'s> From<TypedValueMap<'s>> for HashMap<&'s str, TypedValue> {
    fn from(value: TypedValueMap<'s>) -> Self {
        value.0
    }
}

#[allow(private_bounds)]
pub trait ValueMap<'s>: Into<HashMap<&'s str, Self::Item>> + Sealed {
    type Item;

    /// Gets the map as std HashMap reference
    fn as_map(&self) -> &HashMap<&'s str, Self::Item>;

    /// Returns true if the map has no element.
    fn is_empty(&self) -> bool {
        self.as_map().is_empty()
    }

    /// Returns the size of the map.
    fn len(&self) -> usize {
        self.as_map().len()
    }

    /// Returns an iterator visiting all keys in the map.
    fn keys(&self) -> Keys<'_, &'s str, Self::Item> {
        self.as_map().keys()
    }

    /// Returns an iterator visiting all values in the map.
    fn values(&self) -> Values<'_, &'s str, Self::Item> {
        self.as_map().values()
    }

    /// Creates a consuming iterator visiting all the values is the map.
    /// The map cannot be used after this call.
    fn into_values(self) -> IntoValues<&'s str, Self::Item> {
        self.into().into_values()
    }

    /// Creates a consuming iterator visiting all the key-value pairs in the map.
    /// The map cannot be used after this call.
    fn into_iter(self) -> IntoIter<&'s str, Self::Item> {
        self.into().into_iter()
    }
}

impl<'s> ValueMap<'s> for UntypedValueMap<'s> {
    type Item = Value;

    fn as_map(&self) -> &HashMap<&'s str, Self::Item> {
        &self.0
    }
}

impl<'s> ValueMap<'s> for TypedValueMap<'s> {
    type Item = TypedValue;

    fn as_map(&self) -> &HashMap<&'s str, Self::Item> {
        &self.0
    }
}

impl<'s, VM> IntoParameters for VM
where
    VM: ValueMap<'s>,
    VM::Item: IntoParameter,
{
    fn into_parameters(self, platform: &dyn DatabasePlatform) -> CreedResult<Parameters<'static>> {
        let values: Vec<_> = self
            .into_iter()
            .map(
                |(name, typed)| -> CreedResult<(ParameterIndex, Parameter)> {
                    Ok((
                        ParameterIndex::Named(name.to_string()),
                        typed.into_parameter(platform)?,
                    ))
                },
            )
            .try_collect()?;

        Ok(Parameters::Vec(values))
    }
}

impl<IP: IntoParameter> IntoParameters for IntoValues<&'_ str, IP> {
    fn into_parameters(self, platform: &dyn DatabasePlatform) -> CreedResult<Parameters<'static>> {
        let v: Vec<_> = self
            .map(|typed| typed.into_parameter(platform))
            .try_collect()?;

        Ok(Parameters::from(v))
    }
}

#[derive(Clone)]
pub struct TypedValue {
    pub value: Value,
    pub r#type: Option<TypePtr>,
}

impl From<TypedValue> for Value {
    fn from(value: TypedValue) -> Self {
        value.value
    }
}

impl<'a> From<TypedValueMap<'a>> for UntypedValueMap<'a> {
    fn from(value: TypedValueMap<'a>) -> Self {
        let mut map = HashMap::with_capacity(value.len());
        for (k, v) in value.0.into_iter() {
            map.insert(k, v.value);
        }

        Self(map)
    }
}

#[cfg(test)]
mod tests {
    use crate::Value;
    use chrono::DateTime;

    #[test]
    fn is_null_should_work() {
        let value = Value::NULL;
        assert!(value.is_null());

        let value = Value::String("".to_string());
        assert!(!value.is_null());
        let value = Value::Int(0);
        assert!(!value.is_null());
        let value = Value::Float(0.0);
        assert!(!value.is_null());
    }

    #[test]
    fn is_string_eq_should_work() {
        let value = Value::from("");
        assert!(value.is_string_eq(""));
        let value = Value::String("creed test".to_string());
        assert!(value.is_string_eq("creed test"));
        assert!(!value.is_string_eq("creed te"));
        assert!(!value.is_string_eq("test"));

        let value = Value::NULL;
        assert!(!value.is_string_eq(""));
        let value = Value::Int(0);
        assert!(!value.is_string_eq("0"));
        let value = Value::Float(0.0);
        assert!(!value.is_string_eq("0.0"));
    }

    #[test]
    fn is_int_eq_should_work() {
        let value = Value::Int(0);
        assert!(value.is_int_eq(0));
        let value = Value::Int(42);
        assert!(value.is_int_eq(42));
        assert!(!value.is_uint_eq(42));
        assert!(!value.is_int_eq(0));

        let value = Value::NULL;
        assert!(!value.is_int_eq(0));
        let value = Value::String("0".to_string());
        assert!(!value.is_int_eq(0));
        let value = Value::Float(0.0);
        assert!(!value.is_int_eq(0));
    }

    #[test]
    fn is_uint_eq_should_work() {
        let value = Value::UInt(0);
        assert!(value.is_uint_eq(0));
        let value = Value::UInt(42);
        assert!(value.is_uint_eq(42));
        assert!(!value.is_int_eq(42));
        assert!(!value.is_uint_eq(0));

        let value = Value::NULL;
        assert!(!value.is_uint_eq(0));
        let value = Value::String("0".to_string());
        assert!(!value.is_uint_eq(0));
        let value = Value::Float(0.0);
        assert!(!value.is_uint_eq(0));
    }

    #[test]
    fn is_float_eq_should_work() {
        let value = Value::Float(0.0);
        assert!(value.is_float_eq(0.0));
        let value = Value::Float(42.0);
        assert!(value.is_float_eq(42.0));
        assert!(!value.is_float_eq(0.0));

        let value = Value::NULL;
        assert!(!value.is_float_eq(0.0));
        let value = Value::String("0.0".to_string());
        assert!(!value.is_float_eq(0.0));
        let value = Value::UInt(0);
        assert!(!value.is_float_eq(0.0));
    }

    #[test]
    fn is_bytes_eq_should_work() {
        let value = Value::from(&[]);
        assert!(value.is_bytes_eq(&[]));
        let value = Value::from(&[0_u8, 42_u8]);
        assert!(value.is_bytes_eq(&[0, 42]));
        assert!(!value.is_bytes_eq(&[]));

        let value = Value::NULL;
        assert!(!value.is_bytes_eq(&[]));
        let value = Value::String("0.0".to_string());
        assert!(!value.is_bytes_eq(&[]));
        let value = Value::UInt(0);
        assert!(!value.is_bytes_eq(&[]));
    }

    #[test]
    fn is_boolean_eq_should_work() {
        let value = Value::Boolean(false);
        assert!(value.is_boolean_eq(false));
        let value = Value::Boolean(true);
        assert!(value.is_boolean_eq(true));
        assert!(!value.is_boolean_eq(false));

        let value = Value::NULL;
        assert!(!value.is_boolean_eq(false));
        let value = Value::String("".to_string());
        assert!(!value.is_boolean_eq(false));
        let value = Value::UInt(0);
        assert!(!value.is_boolean_eq(false));
    }

    #[test]
    fn is_datetime_eq_should_work() {
        let date_ref = DateTime::parse_from_rfc3339("2022-08-19T05:00:00Z").unwrap();
        let date_tz = date_ref.with_timezone(&chrono_tz::Europe::Rome);

        let value = Value::from(&date_ref);
        assert!(value.is_datetime_eq(&date_ref));
        assert!(value.is_datetime_eq(&date_tz));

        let value = Value::from(date_tz);
        assert!(value.is_datetime_eq(&date_ref));
        assert!(value.is_datetime_eq(&date_tz));
        assert!(
            !value.is_datetime_eq(&DateTime::parse_from_rfc3339("2020-01-01T05:00:00Z").unwrap())
        );
    }
}

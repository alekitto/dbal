use crate::error::Error;
use crate::parameter_type::ParameterType;
use crate::platform::DatabasePlatform;
use crate::Value;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum ParameterIndex {
    Positional(usize),
    Named(String),
}

impl From<String> for ParameterIndex {
    fn from(value: String) -> Self {
        ParameterIndex::Named(value)
    }
}

impl From<&str> for ParameterIndex {
    fn from(value: &str) -> Self {
        ParameterIndex::Named(String::from(value))
    }
}

impl From<i32> for ParameterIndex {
    fn from(value: i32) -> Self {
        ParameterIndex::Positional(value as usize)
    }
}

impl From<i64> for ParameterIndex {
    fn from(value: i64) -> Self {
        ParameterIndex::Positional(value as usize)
    }
}

impl From<usize> for ParameterIndex {
    fn from(value: usize) -> Self {
        ParameterIndex::Positional(value)
    }
}

#[derive(Clone, Debug)]
pub struct Parameter {
    pub(crate) value: Value,
    pub(crate) value_type: ParameterType,
}

impl Parameter {
    pub fn new(value: Value, param_type: ParameterType) -> Self {
        Parameter {
            value,
            value_type: param_type,
        }
    }
}

impl From<Value> for Parameter {
    fn from(value: Value) -> Self {
        match value {
            Value::NULL => Parameter::new(value, ParameterType::Null),
            Value::UInt(_) | Value::Int(_) => Parameter::new(value, ParameterType::Integer),
            Value::Float(_) => Parameter::new(value, ParameterType::Float),
            Value::Bytes(_) => Parameter::new(value, ParameterType::Binary),
            Value::Boolean(_) => Parameter::new(value, ParameterType::Boolean),
            _ => Parameter::new(value, ParameterType::String),
        }
    }
}

#[derive(Debug)]
pub enum Parameters<'a> {
    Vec(Vec<(ParameterIndex, Parameter)>),
    Array(&'a [(ParameterIndex, Parameter)]),
}

impl<'a> Parameters<'a> {
    pub fn is_empty(&self) -> bool {
        match self {
            Parameters::Vec(vec) => vec.is_empty(),
            Parameters::Array(arr) => arr.is_empty(),
        }
    }
}

impl From<Parameters<'_>> for Vec<(ParameterIndex, Parameter)> {
    fn from(value: Parameters) -> Self {
        match value {
            Parameters::Vec(v) => v,
            Parameters::Array(v) => v.to_vec(),
        }
    }
}

impl From<i64> for Parameter {
    fn from(value: i64) -> Self {
        Parameter::new(Value::from(value), ParameterType::Integer)
    }
}

impl TryFrom<Parameter> for i64 {
    type Error = Error;

    fn try_from(value: Parameter) -> Result<Self, Self::Error> {
        match value.value {
            Value::Int(i) => Ok(i),
            Value::UInt(i) => i64::try_from(i).map_err(|e| e.into()),
            Value::Boolean(b) => Ok(i64::from(b)),
            _ => Err(Error::type_mismatch()),
        }
    }
}

impl TryFrom<Parameter> for u64 {
    type Error = Error;

    fn try_from(value: Parameter) -> Result<Self, Self::Error> {
        match value.value {
            Value::Int(i) => u64::try_from(i).map_err(|e| e.into()),
            Value::UInt(i) => Ok(i),
            _ => Err(Error::type_mismatch()),
        }
    }
}

impl TryFrom<Parameter> for f64 {
    type Error = Error;

    fn try_from(value: Parameter) -> Result<Self, Self::Error> {
        match value.value {
            Value::Float(i) => Ok(i),
            _ => Err(Error::type_mismatch()),
        }
    }
}

impl TryFrom<Parameter> for Vec<u8> {
    type Error = Error;

    fn try_from(value: Parameter) -> Result<Self, Self::Error> {
        match value.value {
            Value::Int(i) => Ok(i.to_be_bytes().to_vec()),
            Value::UInt(i) => Ok(i.to_be_bytes().to_vec()),
            Value::String(s) => Ok(s.into_bytes()),
            Value::Float(f) => Ok(f.to_be_bytes().to_vec()),
            Value::Bytes(v) => Ok(v),
            Value::Boolean(b) => Ok(if b { vec![1_u8] } else { vec![0_u8] }),
            Value::DateTime(dt) => Ok(dt.to_rfc3339().into_bytes()),
            Value::Json(json) => Ok(json.to_string().into_bytes()),
            Value::Uuid(uuid) => Ok(uuid.to_string().into_bytes()),
            _ => Err(Error::type_mismatch()),
        }
    }
}

impl From<Vec<Parameter>> for Parameters<'_> {
    fn from(value: Vec<Parameter>) -> Self {
        let mut v = vec![];
        for (idx, value) in value.into_iter().enumerate() {
            v.push((ParameterIndex::Positional(idx), value));
        }

        Parameters::Vec(v)
    }
}

impl<T: Into<Value>> From<Vec<T>> for Parameters<'_> {
    fn from(value: Vec<T>) -> Self {
        let mut v = vec![];
        for (idx, value) in value.into_iter().enumerate() {
            v.push((ParameterIndex::Positional(idx), value.into().into()));
        }

        Parameters::Vec(v)
    }
}

pub trait IntoParameters {
    /// Convert this object into a Parameters object.
    fn into_parameters(self, platform: &dyn DatabasePlatform)
        -> crate::Result<Parameters<'static>>;
}

impl IntoParameters for Parameters<'static> {
    fn into_parameters(self, _: &dyn DatabasePlatform) -> crate::Result<Parameters<'static>> {
        Ok(self)
    }
}

pub const NO_PARAMS: Parameters = Parameters::Array(&[]);
pub macro params {
    [] => {
        $crate::parameter::NO_PARAMS
    },

    [$idx:expr=>$value:expr] => {
        $crate::Parameters::Vec([ ($crate::ParameterIndex::from($idx),$crate::Parameter::from($value)) ].to_vec())
    },

    [$($idx:expr=>$value:expr,)*] => {
        $crate::Parameters::Vec([ $(($crate::ParameterIndex::from($idx),$crate::Parameter::from($value)),)* ].to_vec())
    }
}

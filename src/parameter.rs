use crate::parameter_type::ParameterType;
use crate::Value;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum ParameterIndex {
    Positional(usize),
    Named(String),
}

impl From<String> for ParameterIndex {
    fn from(value: String) -> Self {
        ParameterIndex::Named(value.clone())
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

#[derive(Debug)]
pub enum Parameters<'a> {
    Vec(Vec<(ParameterIndex, Parameter)>),
    Array(&'a [(ParameterIndex, Parameter)]),
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

pub const NO_PARAMS: Parameters = crate::Parameters::Array(&[]);

#[macro_export]
macro_rules! params {
    [] => {
        $crate::parameter::NO_PARAMS
    };
    [($idx:expr=>$value:expr)] => {
        $crate::Parameters::Array(&[ ($crate::ParameterIndex::from($idx),$crate::Parameter::from($value)) ])
    };
    [$(($idx:expr=>$value:expr),)*] => {
        $crate::Parameters::Array(&[ $(($crate::ParameterIndex::from($idx),$crate::Parameter::from($value)),)* ])
    };
}

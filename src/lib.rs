#![feature(associated_type_defaults)]

pub mod driver;
pub mod error;
mod parameter;
pub mod parameter_type;
mod result;
mod rows;
mod value;

pub use parameter::Parameter;
pub use parameter::ParameterIndex;
pub use parameter::Parameters;
pub use result::Result;
pub use rows::{Row, Rows};
pub use value::Value;

pub fn xxx() {
    params![
        (1 => 15),
        (2 => 12),
    ];
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}

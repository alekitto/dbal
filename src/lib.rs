#![feature(async_closure)]
#![feature(backtrace)]
#![feature(decl_macro)]
#![feature(type_alias_impl_trait)]

mod connection;
pub mod driver;
pub mod error;
mod event;
mod parameter;
mod parameter_type;
mod result;
mod rows;
mod value;

pub use connection::Connection;
pub use error::Error;
pub use event::*;
pub use parameter::Parameter;
pub use parameter::ParameterIndex;
pub use parameter::Parameters;
pub use parameter_type::ParameterType;
pub use result::{Async, AsyncResult, Result};
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

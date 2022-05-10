#![feature(decl_macro)]
#![feature(type_alias_impl_trait)]

mod connection;
mod connection_options;
mod event;
mod parameter;
mod parameter_type;
mod result;
mod rows;
mod transaction_isolation_level;
mod value;

pub(crate) mod private;

pub mod driver;
pub mod error;
pub mod platform;
pub mod schema;
pub mod r#type;

pub use connection::Connection;
pub use connection_options::ConnectionOptions;
pub use error::Error;
pub use event::*;
pub use parameter::Parameter;
pub use parameter::ParameterIndex;
pub use parameter::Parameters;
pub use parameter_type::ParameterType;
pub use result::{Async, AsyncResult, Result};
pub use rows::{Row, Rows};
pub use transaction_isolation_level::TransactionIsolationLevel;
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

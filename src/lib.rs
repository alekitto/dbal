#![doc = include_str!("../README.md")]
#![feature(decl_macro)]
#![feature(type_alias_impl_trait)]
#![feature(is_some_and)]

extern crate creed_derive;
extern crate self as creed;

mod configuration;
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
pub(crate) mod util;

pub mod driver;
pub mod error;
pub mod platform;
pub mod schema;
pub mod sync;
pub mod r#type;

pub use configuration::Configuration;
pub use connection::Connection;
pub use connection_options::ConnectionOptions;
pub use error::Error;
pub use event::*;
pub use parameter::params;
pub use parameter::Parameter;
pub use parameter::ParameterIndex;
pub use parameter::Parameters;
pub use parameter_type::ParameterType;
pub use result::{Async, AsyncResult, Result};
pub use rows::{Row, Rows};
pub use transaction_isolation_level::TransactionIsolationLevel;
pub use value::Value;

#[cfg(test)]
pub mod tests;

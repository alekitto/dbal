use crate::driver::Driver;
use std::sync::Arc;

pub trait Event: Send + Sized {}

pub struct ConnectionEvent {
    pub connection: Arc<Driver>,
}

impl ConnectionEvent {
    pub fn new(connection: Arc<Driver>) -> Self {
        Self { connection }
    }
}

impl Event for ConnectionEvent {}

use crate::Connection;
use std::sync::Arc;

pub trait Event: Send + Sized {}

pub struct ConnectionEvent {
    pub connection: Arc<Connection>,
}

impl ConnectionEvent {
    pub fn new(connection: Arc<Connection>) -> Self {
        Self { connection }
    }
}

impl Event for ConnectionEvent {}

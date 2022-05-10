use crate::{Connection, Event};
use std::any::TypeId;
use std::sync::Arc;

pub struct ConnectionEvent {
    pub connection: Arc<Connection>,
}

impl ConnectionEvent {
    pub fn new(connection: Arc<Connection>) -> Self {
        Self { connection }
    }
}

impl Event for ConnectionEvent {
    fn is_async() -> bool {
        true
    }

    fn event_type() -> TypeId {
        TypeId::of::<ConnectionEvent>()
    }
}

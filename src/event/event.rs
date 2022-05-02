use crate::driver::connection::Connection;

pub trait Event: Send + Sized + 'static {
}

pub struct ConnectionEvent<C: Connection<'static>> {
    pub connection: &'static C,
}

impl<C: Connection<'static>> Event for ConnectionEvent<C> {
}

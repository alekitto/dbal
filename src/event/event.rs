use std::any::TypeId;

pub trait Event: Send {
    fn is_async() -> bool
    where
        Self: Sized;
    fn event_type() -> TypeId
    where
        Self: Sized;
}

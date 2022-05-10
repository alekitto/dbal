use crate::platform::DatabasePlatform;
use std::any::TypeId;

pub(crate) type PlatformBox<'a> = Box<&'a (dyn DatabasePlatform + Sync)>;

pub trait Event: Send {
    fn is_async() -> bool
    where
        Self: Sized;
    fn event_type() -> TypeId
    where
        Self: Sized;
}

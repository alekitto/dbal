/// A marker for restricting a method on a public trait to internal use only.
pub(crate) enum Internal {}

pub trait InternalMarker {}
impl InternalMarker for Internal {}

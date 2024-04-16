/// A marker for restricting a method on a public trait to internal use only.
#[allow(dead_code)]
pub(crate) enum Internal {}

pub(crate) trait Sealed {}
impl Sealed for Internal {}

use super::Connection;
use crate::Async;

pub trait ServerInfoAwareConnection<'conn>: Connection<'conn>
where
    <Self as Connection<'conn>>::Statement: super::statement::Statement<'conn>,
{
    fn server_version(&self) -> Async<Option<String>>;
}

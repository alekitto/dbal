pub trait ConnectionFactory
{
    type Connection: crate::driver::connection::Connection;
}

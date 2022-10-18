use std::future::Future;

pub type Mutex<T> = ::tokio::sync::Mutex<T>;
pub type JoinHandle<T> = ::tokio::task::JoinHandle<T>;

pub fn spawn<T>(future: T) -> JoinHandle<T::Output>
where
    T: Future + Send + 'static,
    T::Output: Send + 'static,
{
    tokio::spawn(future)
}

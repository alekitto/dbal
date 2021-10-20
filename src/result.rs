use crate::error::Error;
use std::future::Future;
use std::pin::Pin;

pub type Result<T> = std::result::Result<T, Error>;
pub type AsyncResult<'a, R> = Pin<Box<dyn 'a + Future<Output = Result<R>>>>;

pub type Async<'a, R> = Pin<Box<dyn 'a + Future<Output = R>>>;

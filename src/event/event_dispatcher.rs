use crate::sync::Mutex;
use crate::{AsyncResult, Event, Result};
use std::any::*;
use std::default::Default;
use std::fmt::{Debug, Formatter};

pub type AsyncHandlerFn = dyn (FnMut(&mut dyn Event) -> AsyncResult<()>) + Send;
pub type SyncHandlerFn = dyn (FnMut(&mut dyn Event) -> Result<()>) + Send;

struct AsyncListener {
    event: TypeId,
    handler: Box<AsyncHandlerFn>,
}

struct SyncListener {
    event: TypeId,
    handler: Box<SyncHandlerFn>,
}

#[derive(Default)]
pub struct EventDispatcher {
    sync_listeners: std::sync::Mutex<Vec<SyncListener>>,
    async_listeners: Mutex<Vec<AsyncListener>>,
}

impl Debug for EventDispatcher {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventDispatcher").finish_non_exhaustive()
    }
}

impl EventDispatcher {
    pub fn new() -> Self {
        Self {
            sync_listeners: std::sync::Mutex::new(vec![]),
            async_listeners: Mutex::new(vec![]),
        }
    }

    pub fn add_listener<Ev>(&self, mut action: impl FnMut(&mut Ev) -> Result<()> + Send + 'static)
    where
        Ev: Event + 'static,
    {
        self.sync_listeners.lock().unwrap().push(SyncListener {
            event: Ev::event_type(),
            handler: Box::new(move |ev: &mut dyn Event| {
                (action)(unsafe { &mut *(ev as *mut dyn Event as *mut Ev) })
            }),
        });
    }

    pub async fn add_async_listener<Ev>(
        &self,
        mut action: impl FnMut(&mut Ev) -> AsyncResult<()> + 'static + Send,
    ) where
        Ev: Event + 'static,
    {
        debug_assert!(Ev::is_async());

        self.async_listeners.lock().await.push(AsyncListener {
            event: Ev::event_type(),
            handler: Box::new(move |ev: &mut dyn Event| {
                Box::pin((action)(unsafe { &mut *(ev as *mut dyn Event as *mut Ev) }))
            }),
        });
    }

    pub fn dispatch_sync<Ev>(&self, mut ev: Ev) -> Result<Ev>
    where
        Ev: Event,
    {
        for l in self.sync_listeners.lock().unwrap().iter_mut() {
            if Ev::event_type() == l.event {
                (l.handler)(&mut ev)?;
            }
        }

        Ok(ev)
    }

    pub async fn dispatch_async<Ev>(&self, ev: Ev) -> Result<Ev>
    where
        Ev: Event,
    {
        let mut ev = self.dispatch_sync(ev)?;
        for l in self.async_listeners.lock().await.iter_mut() {
            if Ev::event_type() == l.event {
                let promise = (l.handler)(&mut ev);
                promise.await?;
            }
        }

        Ok(ev)
    }
}

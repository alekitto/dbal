use crate::Event;
use futures::future::BoxFuture;
use std::any::*;
use std::default::Default;
use std::fmt::{Debug, Formatter};
use tokio::sync::Mutex;

struct AsyncListener {
    event: TypeId,
    handler: Box<dyn (FnMut(&mut dyn Event) -> BoxFuture<()>) + Send>,
}

struct SyncListener {
    event: TypeId,
    handler: Box<dyn (FnMut(&mut dyn Event) -> ()) + Send>,
}

#[derive(Default)]
pub struct EventDispatcher {
    sync_listeners: std::sync::Mutex<Vec<SyncListener>>,
    async_listeners: Mutex<Vec<AsyncListener>>,
}

impl Debug for EventDispatcher {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt("EventDispatcher {}", f)
    }
}

impl EventDispatcher {
    pub fn new() -> Self {
        Self {
            sync_listeners: std::sync::Mutex::new(vec![]),
            async_listeners: Mutex::new(vec![]),
        }
    }

    pub fn add_listener<Ev>(&self, mut action: impl FnMut(&mut Ev) -> () + Send + 'static)
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
        mut action: impl FnMut(&mut Ev) -> BoxFuture<()> + 'static + Send,
    ) where
        Ev: Event + 'static,
    {
        if !Ev::is_async() {
            panic!("Trying to add an async listener to a sync event. Aborting...");
        }

        self.async_listeners.lock().await.push(AsyncListener {
            event: Ev::event_type(),
            handler: Box::new(move |ev: &mut dyn Event| {
                Box::pin((action)(unsafe { &mut *(ev as *mut dyn Event as *mut Ev) }))
            }),
        });
    }

    pub fn dispatch_sync<Ev>(&self, ev: &mut Ev)
    where
        Ev: Event,
    {
        for l in self.sync_listeners.lock().unwrap().iter_mut() {
            if Ev::event_type() == l.event {
                (l.handler)(ev);
            }
        }
    }

    pub async fn dispatch_async<Ev>(&self, ev: &mut Ev)
    where
        Ev: Event,
    {
        self.dispatch_sync(ev);
        for l in self.async_listeners.lock().await.iter_mut() {
            if Ev::event_type() == l.event {
                let promise = (l.handler)(ev);
                promise.await;
            }
        }
    }
}

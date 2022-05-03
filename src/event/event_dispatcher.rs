use crate::Event;
use futures::executor::block_on;
use futures::future::BoxFuture;
use std::any::*;
use std::default::Default;
use std::fmt::{Debug, Formatter};
use tokio::sync::Mutex;

struct Listener {
    event: TypeId,
    handler: Box<dyn (FnMut(&mut dyn Any) -> BoxFuture<()>) + Send>,
}

#[derive(Default)]
pub struct EventDispatcher {
    subs: Mutex<Vec<Listener>>,
}

impl Debug for EventDispatcher {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt("EventDispatcher {}", f)
    }
}

impl EventDispatcher {
    pub fn new() -> Self {
        Self {
            subs: Mutex::new(vec![]),
        }
    }

    pub fn add_listener<Ev>(
        &self,
        mut action: impl FnMut(&mut Ev) -> BoxFuture<()> + 'static + Send,
    ) where
        Ev: Event + 'static,
    {
        block_on(async {
            self.subs.lock().await.push(Listener {
                event: TypeId::of::<Ev>(),
                handler: Box::new(move |ev: &mut dyn Any| {
                    Box::pin((action)(ev.downcast_mut().expect("Wrong Event!")))
                }),
            });
        });
    }

    pub async fn dispatch<Ev>(&self, ev: &mut Ev)
    where
        Ev: Event + 'static,
    {
        for l in self.subs.lock().await.iter_mut() {
            if TypeId::of::<Ev>() == l.event {
                (l.handler)(ev).await;
            }
        }
    }
}

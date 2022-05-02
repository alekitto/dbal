use std::any::*;
use std::default::Default;
use std::future::Future;
use std::sync::Mutex;
use futures::future::BoxFuture;
use crate::Event;

struct Listener {
    event: TypeId,
    handler: Box<dyn FnMut(&mut dyn Any) -> BoxFuture<()>>,
}

#[derive(Default)]
pub struct EventDispatcher {
    subs: Mutex<Vec<Listener>>,
}

impl EventDispatcher {
    pub fn new() -> Self {
        Self {
            subs: Mutex::new(vec![]),
        }
    }

    pub fn add_listener<Ev, Fut>(&self, mut action: impl FnMut(&mut Ev) -> Fut + Send + 'static)
    where
        Ev: Event,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.subs.lock().unwrap().push(Listener {
            event: TypeId::of::<Ev>(),
            handler: Box::new(move |ev: &mut dyn Any| {
                Box::pin((action)(ev.downcast_mut().expect("Wrong Event!")))
            }),
        });
    }

    pub async fn dispatch<Ev>(&self, ev: &mut Ev)
    where
        Ev: Event
    {
        for l in self.subs.lock().unwrap().iter_mut() {
            if TypeId::of::<Ev>() == l.event {
                (l.handler)(ev).await;
            }
        }
    }
}

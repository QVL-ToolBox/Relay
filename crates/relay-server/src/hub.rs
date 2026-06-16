//! Shared broker state tying `relay-core`'s pure [`Router`] to the live delivery
//! channels.
//!
//! `relay-core` stays I/O-free: it only answers "which clients match this topic,
//! and at which granted QoS?". The hub owns, per connected client, an unbounded
//! MPSC sender of [`Delivery`] items that the client's writer task turns into
//! PUBLISH packets on its socket. Publishing = ask the router for the matching
//! [`ClientId`]s + granted QoS, then push a [`Delivery`] (at the effective QoS
//! `min(publish, granted)`) onto each one's sender.
//!
//! Why [`Delivery`] and not a ready-made `Packet`: with QoS 1 each subscriber
//! gets its *own* packet identifier, assigned by that connection's writer from
//! its own per-connection counter. The hub therefore ships the raw message
//! (topic + payload + effective QoS) and lets the receiving connection stamp the
//! packet id.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use relay_core::{ClientId, QoS, Router, TopicFilter};
use tokio::sync::mpsc;
use tracing::debug;

/// A message to deliver to one subscriber, before the per-connection packet id
/// is stamped. `qos` is already the *effective* QoS — `min(publish, granted)`.
#[derive(Debug, Clone)]
pub struct Delivery {
    pub topic: String,
    pub payload: Bytes,
    pub qos: QoS,
    pub retain: bool,
}

/// Cloneable handle to the shared broker state.
#[derive(Clone)]
pub struct Hub {
    inner: Arc<Inner>,
}

struct Inner {
    next_id: AtomicU64,
    router: Mutex<Router>,
    clients: Mutex<HashMap<ClientId, mpsc::UnboundedSender<Delivery>>>,
}

impl Hub {
    pub fn new() -> Self {
        Hub {
            inner: Arc::new(Inner {
                next_id: AtomicU64::new(1),
                router: Mutex::new(Router::new()),
                clients: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Register a new connection: assign a [`ClientId`] and return the receiver
    /// that the connection's writer drains to the socket.
    pub fn register(&self) -> (ClientId, mpsc::UnboundedReceiver<Delivery>) {
        let id = ClientId(self.inner.next_id.fetch_add(1, Ordering::Relaxed));
        let (tx, rx) = mpsc::unbounded_channel();
        self.inner.clients.lock().unwrap().insert(id, tx);
        (id, rx)
    }

    /// Tear a connection down on disconnect/error.
    pub fn deregister(&self, id: ClientId) {
        self.inner.clients.lock().unwrap().remove(&id);
        self.inner.router.lock().unwrap().remove_client(id);
    }

    /// Register a normal (fan-out) subscription for a client at granted `qos`.
    pub fn subscribe(&self, id: ClientId, filter: TopicFilter, qos: QoS) {
        self.inner.router.lock().unwrap().subscribe(id, filter, qos);
    }

    /// Register a shared subscription: `id` joins `group` with `filter` at `qos`.
    pub fn subscribe_shared(&self, group: String, id: ClientId, filter: TopicFilter, qos: QoS) {
        self.inner
            .router
            .lock()
            .unwrap()
            .subscribe_shared(group, id, filter, qos);
    }

    /// Deliver a PUBLISH to its recipients: every matching normal subscriber,
    /// plus one member per matching share group (round-robin). Each recipient
    /// gets the message at the effective QoS `min(msg_qos, granted)`.
    /// Returns how many recipients the message was queued for.
    pub fn publish(&self, topic: &str, payload: &Bytes, msg_qos: QoS, retain: bool) -> usize {
        // Resolve targets under the router lock, then release it before sending.
        let targets = self.inner.router.lock().unwrap().route(topic);
        if targets.is_empty() {
            return 0;
        }
        let clients = self.inner.clients.lock().unwrap();
        let mut delivered = 0;
        for (id, granted) in targets {
            if let Some(tx) = clients.get(&id) {
                let delivery = Delivery {
                    topic: topic.to_string(),
                    payload: payload.clone(),
                    qos: msg_qos.min(granted),
                    retain,
                };
                if tx.send(delivery).is_ok() {
                    delivered += 1;
                } else {
                    debug!(client = id.0, "delivery channel closed, skipping");
                }
            }
        }
        delivered
    }
}

impl Default for Hub {
    fn default() -> Self {
        Self::new()
    }
}

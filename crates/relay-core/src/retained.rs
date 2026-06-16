//! Retained-message store — pure, I/O-free.
//!
//! MQTT lets a publisher mark a message as *retained*: the broker keeps the last
//! such message per topic and hands it to any client that subscribes afterwards,
//! so a late joiner immediately learns the topic's current value (e.g. a device's
//! last known state). Publishing a **zero-length** retained payload to a topic
//! *clears* its retained message.
//!
//! This store only holds data and answers "which retained messages match this
//! filter?"; `relay-server` decides when to store and when to replay.

use crate::message::Message;
use crate::topic::TopicFilter;
use std::collections::HashMap;

/// Last retained [`Message`] per concrete topic name.
#[derive(Debug, Default)]
pub struct RetainedStore {
    by_topic: HashMap<String, Message>,
}

impl RetainedStore {
    /// Create an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply a retained PUBLISH: store it as the topic's retained message, or —
    /// per the MQTT rule — **clear** the topic's retained message if the payload
    /// is empty. Only call this for messages whose `retain` flag is set.
    pub fn apply(&mut self, message: Message) {
        if message.payload.is_empty() {
            self.by_topic.remove(&message.topic);
        } else {
            self.by_topic.insert(message.topic.clone(), message);
        }
    }

    /// Drop the retained message for a topic, if any.
    pub fn remove(&mut self, topic: &str) {
        self.by_topic.remove(topic);
    }

    /// Every retained message whose topic matches `filter`, for replay to a
    /// freshly-subscribed client. Ordered by topic for determinism.
    pub fn matching(&self, filter: &TopicFilter) -> Vec<Message> {
        let mut out: Vec<Message> = self
            .by_topic
            .iter()
            .filter(|(topic, _)| filter.matches(topic))
            .map(|(_, msg)| msg.clone())
            .collect();
        out.sort_by(|a, b| a.topic.cmp(&b.topic));
        out
    }

    /// Number of retained topics currently held.
    pub fn len(&self) -> usize {
        self.by_topic.len()
    }

    /// Whether the store holds no retained messages.
    pub fn is_empty(&self) -> bool {
        self.by_topic.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::qos::QoS;

    fn retained(topic: &str, payload: &'static [u8]) -> Message {
        Message {
            topic: topic.to_string(),
            payload: bytes::Bytes::from_static(payload),
            qos: QoS::AtMostOnce,
            retain: true,
        }
    }

    fn filter(s: &str) -> TopicFilter {
        TopicFilter::parse(s).unwrap()
    }

    #[test]
    fn stores_and_matches_by_filter() {
        let mut s = RetainedStore::new();
        s.apply(retained("sensors/eu/temp", b"21"));
        s.apply(retained("sensors/us/temp", b"68"));
        s.apply(retained("orders/created", b"x"));

        let matched: Vec<String> = s
            .matching(&filter("sensors/+/temp"))
            .into_iter()
            .map(|m| m.topic)
            .collect();
        assert_eq!(matched, vec!["sensors/eu/temp", "sensors/us/temp"]);
    }

    #[test]
    fn latest_value_per_topic_wins() {
        let mut s = RetainedStore::new();
        s.apply(retained("a", b"old"));
        s.apply(retained("a", b"new"));
        let m = s.matching(&filter("a"));
        assert_eq!(m.len(), 1);
        assert_eq!(m[0].payload.as_ref(), b"new");
    }

    #[test]
    fn empty_payload_clears_the_retained_message() {
        let mut s = RetainedStore::new();
        s.apply(retained("a", b"value"));
        assert_eq!(s.len(), 1);

        // Zero-length retained payload clears it.
        s.apply(retained("a", b""));
        assert!(s.is_empty());
        assert!(s.matching(&filter("a")).is_empty());
    }

    #[test]
    fn wildcard_filter_collects_all_matching() {
        let mut s = RetainedStore::new();
        s.apply(retained("a/b", b"1"));
        s.apply(retained("a/c", b"2"));
        s.apply(retained("x/y", b"3"));
        assert_eq!(s.matching(&filter("a/#")).len(), 2);
    }
}

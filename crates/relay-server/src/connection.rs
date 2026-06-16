//! Per-connection MQTT loop, built on the `rmqtt-codec` v5 tokio codec.
//!
//! The `Framed` stream is split into a reader (`stream`) and a writer (`sink`).
//! A `tokio::select!` interleaves two sources:
//! - **incoming**: packets from this client (CONNECT, SUBSCRIBE, PUBLISH, …);
//! - **outgoing**: [`Delivery`]s the [`Hub`] routes *to* this client because it
//!   subscribed to a topic someone else published on.
//!
//! ## QoS
//!
//! The broker supports QoS 0 and QoS 1 (at-least-once). QoS 2 is a later step.
//!
//! - **Inbound** PUBLISH at QoS 1 is acknowledged to the publisher with PUBACK.
//! - **Outbound** delivery at QoS 1 carries a packet identifier drawn from this
//!   connection's own counter; the id is tracked as *unacknowledged* until the
//!   subscriber returns a PUBACK. (Retransmission across reconnects needs
//!   persistent sessions — a later step; for now the in-memory tracking lets us
//!   accept and clear the PUBACK correctly.)

use std::collections::HashSet;
use std::num::NonZeroU16;

use futures::{SinkExt, StreamExt};
use relay_core::{QoS, SharedSubscription, TopicFilter};
use rmqtt_codec::types::Publish;
use rmqtt_codec::v5::{
    Codec, ConnectAck, ConnectAckReason, Packet, PublishAck, PublishAckReason, PublishProperties,
    QoS as WireQoS, SubscribeAck, SubscribeAckReason,
};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tracing::{debug, info, warn};

use crate::hub::{Delivery, Hub};

/// Maximum inbound packet size we accept (256 KiB); 0 outbound = unlimited.
const MAX_INBOUND_SIZE: u32 = 256 * 1024;

/// Highest QoS the broker currently grants/delivers. QoS 2 is a later step.
const MAX_QOS: QoS = QoS::AtLeastOnce;

/// Per-connection state for QoS > 0 delivery to this client.
struct OutboundQos {
    /// Next packet identifier to hand out (1..=65535, never 0).
    next_id: u16,
    /// Packet ids delivered at QoS 1 and not yet PUBACK'd by this client.
    unacked: HashSet<u16>,
}

impl OutboundQos {
    fn new() -> Self {
        Self {
            next_id: 1,
            unacked: HashSet::new(),
        }
    }

    /// Allocate the next packet id, maintaining the "never 0" invariant.
    fn allocate(&mut self) -> NonZeroU16 {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        if self.next_id == 0 {
            self.next_id = 1;
        }
        // `id` is non-zero by the invariant we maintain on `next_id`.
        NonZeroU16::new(id).expect("packet id invariant: never 0")
    }
}

fn wire_to_core(q: WireQoS) -> QoS {
    match q {
        WireQoS::AtMostOnce => QoS::AtMostOnce,
        WireQoS::AtLeastOnce => QoS::AtLeastOnce,
        WireQoS::ExactlyOnce => QoS::ExactlyOnce,
    }
}

fn core_to_wire(q: QoS) -> WireQoS {
    match q {
        QoS::AtMostOnce => WireQoS::AtMostOnce,
        QoS::AtLeastOnce => WireQoS::AtLeastOnce,
        QoS::ExactlyOnce => WireQoS::ExactlyOnce,
    }
}

fn granted_reason(q: QoS) -> SubscribeAckReason {
    match q {
        QoS::AtMostOnce => SubscribeAckReason::GrantedQos0,
        QoS::AtLeastOnce => SubscribeAckReason::GrantedQos1,
        QoS::ExactlyOnce => SubscribeAckReason::GrantedQos2,
    }
}

/// Turn a routed [`Delivery`] into a PUBLISH packet for this connection,
/// stamping a packet id (and tracking it) when delivering at QoS 1.
fn build_publish(delivery: Delivery, qos_state: &mut OutboundQos) -> Packet {
    let qos = delivery.qos.min(MAX_QOS);
    let packet_id = if qos == QoS::AtMostOnce {
        None
    } else {
        let id = qos_state.allocate();
        qos_state.unacked.insert(id.get());
        Some(id)
    };

    Packet::Publish(Box::new(Publish {
        dup: false,
        retain: delivery.retain,
        qos: core_to_wire(qos),
        topic: delivery.topic.into(),
        packet_id,
        payload: delivery.payload,
        properties: Some(PublishProperties::default()),
    }))
}

/// Drive a single TCP client connection until it disconnects or errors.
pub async fn handle(socket: TcpStream, peer: String, hub: Hub) {
    let (id, mut rx) = hub.register();
    let (mut sink, mut stream) = Framed::new(socket, Codec::new(MAX_INBOUND_SIZE, 0)).split();
    let mut connected = false;
    let mut qos_state = OutboundQos::new();

    loop {
        tokio::select! {
            // ---- A packet arrived from this client ----
            incoming = stream.next() => {
                let packet = match incoming {
                    Some(Ok((p, _))) => p,
                    Some(Err(e)) => { warn!(%peer, error = ?e, "protocol error, dropping"); break; }
                    None => { info!(%peer, "client closed connection"); break; }
                };

                match packet {
                    Packet::Connect(connect) => {
                        info!(%peer, client_id = %connect.client_id, "CONNECT");
                        connected = true;
                        let ack = ConnectAck {
                            reason_code: ConnectAckReason::Success,
                            ..ConnectAck::default()
                        };
                        if sink.send(Packet::from(ack)).await.is_err() { break; }
                    }

                    Packet::Subscribe(sub) => {
                        if !connected { warn!(%peer, "SUBSCRIBE before CONNECT, dropping"); break; }
                        let mut status = Vec::with_capacity(sub.topic_filters.len());
                        for (filter, opts) in &sub.topic_filters {
                            // Grant the requested QoS, capped at what we support.
                            let granted = wire_to_core(opts.qos).min(MAX_QOS);
                            // `$share/{group}/{filter}` is a shared subscription
                            // (competing consumers); anything else is normal fan-out.
                            if let Some(shared) = SharedSubscription::parse(filter) {
                                info!(%peer, group = %shared.group, filter = %shared.filter.as_str(), ?granted, "SUBSCRIBE (shared)");
                                hub.subscribe_shared(shared.group, id, shared.filter, granted);
                                status.push(granted_reason(granted));
                            } else if let Some(tf) = TopicFilter::parse(filter) {
                                info!(%peer, %filter, ?granted, "SUBSCRIBE");
                                hub.subscribe(id, tf, granted);
                                status.push(granted_reason(granted));
                            } else {
                                warn!(%peer, %filter, "invalid topic filter");
                                status.push(SubscribeAckReason::TopicFilterInvalid);
                            }
                        }
                        let ack = SubscribeAck {
                            packet_id: sub.packet_id,
                            properties: Vec::new(),
                            reason_string: None,
                            status,
                        };
                        if sink.send(Packet::from(ack)).await.is_err() { break; }
                    }

                    Packet::Publish(p) => {
                        if !connected { warn!(%peer, "PUBLISH before CONNECT, dropping"); break; }
                        let topic = p.topic.to_string();
                        let msg_qos = wire_to_core(p.qos);

                        // QoS 1: acknowledge receipt to the publisher with PUBACK.
                        // (QoS 2's PUBREC/PUBREL/PUBCOMP handshake is a later step.)
                        if msg_qos == QoS::AtLeastOnce {
                            if let Some(packet_id) = p.packet_id {
                                let ack = PublishAck {
                                    packet_id,
                                    reason_code: PublishAckReason::Success,
                                    properties: Vec::new(),
                                    reason_string: None,
                                };
                                if sink.send(Packet::PublishAck(ack)).await.is_err() { break; }
                            } else {
                                warn!(%peer, "QoS 1 PUBLISH without packet id, dropping");
                                break;
                            }
                        }

                        let n = hub.publish(&topic, &p.payload, msg_qos, p.retain);
                        debug!(%peer, %topic, ?msg_qos, subscribers = n, "PUBLISH routed");
                    }

                    // A subscriber acknowledged a QoS 1 delivery we sent it.
                    Packet::PublishAck(ack) => {
                        if qos_state.unacked.remove(&ack.packet_id.get()) {
                            debug!(%peer, packet_id = ack.packet_id.get(), "PUBACK (delivery confirmed)");
                        } else {
                            debug!(%peer, packet_id = ack.packet_id.get(), "PUBACK for unknown packet id");
                        }
                    }

                    Packet::PingRequest => {
                        debug!(%peer, "PINGREQ");
                        if sink.send(Packet::PingResponse).await.is_err() { break; }
                    }

                    Packet::Disconnect(_) => { info!(%peer, "DISCONNECT"); break; }

                    other => {
                        debug!(%peer, kind = other.packet_type(), "unhandled packet (TODO)");
                    }
                }
            }

            // ---- The hub routed a message to us ----
            outgoing = rx.recv() => {
                match outgoing {
                    Some(delivery) => {
                        let packet = build_publish(delivery, &mut qos_state);
                        if sink.send(packet).await.is_err() { break; }
                    }
                    None => break, // our sender was dropped (hub deregistered us)
                }
            }
        }
    }

    hub.deregister(id);
    info!(%peer, "connection closed");
}

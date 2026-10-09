use std::{
    collections::HashMap,
    marker::PhantomData,
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use modular_mqtt_protocol::{
    ControlPacketType, Disconnect, FixedHeader, MqttVersion, Packet, PingResp, PubAck, PubComp,
    PubRec, PubRel, Publish, Qos, QosPacketIdentifier, SubAck, UnsubAck,
};

use bytes::{Bytes, BytesMut};

use crate::{
    error::BackendError,
    util::{InflightMessage, InflightMessageState},
    ClientOpts, Instant,
};

pub mod sync_backend;

#[cfg(feature = "async")]
pub mod async_backend;

const MIN_READ_TIMEOUT: Duration = Duration::from_millis(5);

/// Data shared between client and backend
pub struct Shared<V, O>
where
    V: MqttVersion,
    O: ClientOpts<V>,
{
    pub broker_addr: String,
    pub opts: O,
    pub online: RwLock<bool>,
    pub subscriptions: Mutex<Vec<V::TopicSubscription>>,
    pub next_packet_identifier: std::sync::atomic::AtomicU16,
}

pub struct BackendStateMachine<V, O, R>
where
    V: MqttVersion,
    O: ClientOpts<V>,
    R: crate::util::Notify,
{
    pub version: PhantomData<V>,
    pub receive_inflight: Vec<u16>,
    pub write_buf: BytesMut,
    pub inflight_msgs: HashMap<u16, Arc<InflightMessage<V, R>>>,
    pub next_resend_deadline: Option<Instant>,
    pub last_read: Instant,
    pub shared: Arc<Shared<V, O>>,
}

enum Action<V, R>
where
    V: MqttVersion,
    R: crate::util::Notify,
{
    None,
    SubAckSend(SubAck<V>),
    UnsubAckSend(UnsubAck<V>),
    ReceiveMsg(Publish<V, QosPacketIdentifier>),
    ServerDisconnect(Disconnect<V>),
    WakeupClient(Arc<InflightMessage<V, R>>),
}

impl<V, O, R> BackendStateMachine<V, O, R>
where
    V: MqttVersion,
    O: ClientOpts<V>,
    R: crate::util::Notify,
{
    /// How long the next blocking read may wait:
    fn next_read_timeout(&self) -> Instant {
        let timeout = match self.next_resend_deadline {
            Some(resend) => resend
                .min(self.last_read + Duration::from_secs(self.shared.opts.keep_alive().into())),
            None => self.last_read + Duration::from_secs(self.shared.opts.keep_alive().into()),
        };

        let now = Instant::now();
        let timeout = (now + MIN_READ_TIMEOUT).max(timeout);
        tracing::trace!("Next timeout in {:?}", timeout - now);
        timeout
    }

    fn re_write_qos1_and_qos2_to_write_buf(&mut self) {
        tracing::debug!("Writing qos1 and qos2 messages into stream");
        let now = Instant::now();
        for (mid, msg) in self.inflight_msgs.iter() {
            match &mut *msg.state.lock().unwrap() {
                InflightMessageState::PubAck(instant) | InflightMessageState::PubRec(instant) => {
                    msg.msg.write_to_buf(&mut self.write_buf);
                    *instant = now;
                }
                InflightMessageState::PubComp(instant) => {
                    PubRel::<V>::new_ok(*mid).write_to_buf(&mut self.write_buf);
                    *instant = now;
                }
                InflightMessageState::Sent => (),
            }
        }
    }

    /// Resends any inflight message past its `resend_interval`, and refreshes
    /// `next_resend_deadline` to the earliest remaining due time so `next_read_timeout` doesn't
    /// need its own scan over `inflight_msgs`.
    fn check_resend_msgs(&mut self) {
        let now = Instant::now();
        let mut next_deadline: Option<Instant> = None;
        for (packet_identifier, msg) in self.inflight_msgs.iter() {
            let state = msg.state.lock().unwrap().clone();
            let sent_time = match state {
                InflightMessageState::PubAck(sent_time) => {
                    if now.saturating_duration_since(sent_time) > self.shared.opts.resend_interval()
                    {
                        tracing::warn!("Resending packet with identifier {}", packet_identifier);
                        msg.msg.write_to_buf(&mut self.write_buf);
                        *msg.state.lock().unwrap() = InflightMessageState::PubAck(now);
                        now
                    } else {
                        sent_time
                    }
                }
                InflightMessageState::PubRec(sent_time) => {
                    tracing::info!("Check resend msg: {msg:?}");
                    if now.saturating_duration_since(sent_time) > self.shared.opts.resend_interval()
                    {
                        tracing::warn!("Resending packet with identifier {}", packet_identifier);
                        msg.msg.write_to_buf(&mut self.write_buf);
                        *msg.state.lock().unwrap() = InflightMessageState::PubRec(now);
                        now
                    } else {
                        sent_time
                    }
                }
                InflightMessageState::PubComp(sent_time) => {
                    if now.saturating_duration_since(sent_time) > self.shared.opts.resend_interval()
                    {
                        tracing::warn!("Resending PubRel with identifier {}", packet_identifier);
                        PubRel::<V>::new_ok(*packet_identifier).write_to_buf(&mut self.write_buf);
                        *msg.state.lock().unwrap() = InflightMessageState::PubComp(now);
                        now
                    } else {
                        sent_time
                    }
                }
                InflightMessageState::Sent => continue,
            };
            let deadline = sent_time + self.shared.opts.resend_interval();
            next_deadline = Some(next_deadline.map_or(deadline, |d| d.min(deadline)));
        }
        self.next_resend_deadline = next_deadline;
    }

    fn handle_msg(
        &mut self,
        fixed_header: FixedHeader,
        mut body: Bytes,
    ) -> Result<Action<V, R>, BackendError> {
        let body = &mut body;
        match &fixed_header.control_packet_type {
            ControlPacketType::PingResp => {
                let resp = PingResp::try_read_entire_buf(fixed_header, body)?;
                tracing::debug!("Received PingResp from server: {resp:?}");
                Ok(Action::None)
            }
            ControlPacketType::SubAck => {
                let suback = SubAck::try_read_entire_buf(fixed_header, body)?;
                tracing::debug!("Received suback: {suback:?}");
                Ok(Action::SubAckSend(suback))
            }
            ControlPacketType::UnsubscribeAck => {
                let unsuback = UnsubAck::try_read_entire_buf(fixed_header, body)?;
                tracing::debug!("Received unsuback: {unsuback:?}");
                Ok(Action::UnsubAckSend(unsuback))
            }
            ControlPacketType::Publish { .. } => {
                let msg = Publish::try_read_entire_buf(fixed_header, body)?;
                tracing::debug!("Received msg: {:?}", msg);
                match (msg.qos(), msg.packet_identifier()) {
                    (Qos::AtMostOnce, _) => (),
                    (Qos::AtLeastOnce, Some(packet_identifier)) => {
                        tracing::trace!("Respond with PubAck (mid={packet_identifier})");
                        self.receive_inflight.push(packet_identifier);
                        PubAck::<V>::new_ok(packet_identifier).write_to_buf(&mut self.write_buf);
                    }
                    (Qos::ExactlyOnce, Some(packet_identifier)) => {
                        tracing::trace!("Respond with PubRec (mid={packet_identifier})");
                        self.receive_inflight.push(packet_identifier);
                        PubRec::<V>::new_ok(packet_identifier).write_to_buf(&mut self.write_buf);
                    }
                    (Qos::AtLeastOnce, None) | (Qos::ExactlyOnce, None) => unreachable!(),
                }
                Ok(Action::ReceiveMsg(msg))
            }
            ControlPacketType::PubAck => {
                let puback = PubAck::<V>::try_read_entire_buf(fixed_header, body)?;
                if let Some(inflight) = self.inflight_msgs.remove(&puback.packet_identifier()) {
                    if matches!(
                        *inflight.state.lock().unwrap(),
                        InflightMessageState::PubAck(_)
                    ) {
                        tracing::debug!("Received PubAck for mid {}", puback.packet_identifier());
                        *inflight.state.lock().unwrap() = InflightMessageState::Sent;
                        return Ok(Action::WakeupClient(inflight));
                    }
                }
                tracing::warn!(
                    "Received unexpected PubAck for mid {}.",
                    puback.packet_identifier()
                );
                Ok(Action::None)
            }

            ControlPacketType::PubRec => {
                let pubrec = PubRec::<V>::try_read_entire_buf(fixed_header, body)?;
                if let Some(inflight) = self.inflight_msgs.get_mut(&pubrec.packet_identifier()) {
                    tracing::debug!(
                        "Received PubRec for mid {}. Responding with PubComp",
                        pubrec.packet_identifier()
                    );
                    if matches!(
                        *inflight.state.lock().unwrap(),
                        InflightMessageState::PubRec(_)
                    ) {
                        PubRel::<V>::new_ok(pubrec.packet_identifier())
                            .write_to_buf(&mut self.write_buf);
                        *inflight.state.lock().unwrap() =
                            InflightMessageState::PubComp(Instant::now());
                        return Ok(Action::None);
                    }
                }
                tracing::warn!(
                    "Received unexpected PubRec for mid {}.",
                    pubrec.packet_identifier()
                );
                Ok(Action::None)
            }
            ControlPacketType::PubComp => {
                let pub_comp = PubComp::<V>::try_read_entire_buf(fixed_header, body)?;
                tracing::trace!("Received PubComp: {pub_comp:?}");
                if let Some(inflight) = self.inflight_msgs.remove(&pub_comp.packet_identifier()) {
                    if matches!(
                        *inflight.state.lock().unwrap(),
                        InflightMessageState::PubComp(_)
                    ) {
                        tracing::debug!(
                            "Received PubComp for mid {}",
                            pub_comp.packet_identifier()
                        );
                        *inflight.state.lock().unwrap() = InflightMessageState::Sent;
                        return Ok(Action::WakeupClient(inflight));
                    }
                }
                tracing::warn!(
                    "Received unexpected PubComp for mid {}.",
                    pub_comp.packet_identifier()
                );
                Ok(Action::None)
            }

            ControlPacketType::Disconnect => {
                let disconnect = Disconnect::<V>::try_read_entire_buf(fixed_header, body)?;
                Ok(Action::ServerDisconnect(disconnect))
            }

            ControlPacketType::PubRel => {
                let pub_rel = PubRel::<V>::try_read_entire_buf(fixed_header, body)?;
                tracing::trace!("Received PubRel: {pub_rel:?}. Responding with PubComp");
                if let Some(index) = self
                    .receive_inflight
                    .iter()
                    .position(|item| *item == pub_rel.packet_identifier())
                {
                    PubComp::<V>::new_ok(pub_rel.packet_identifier())
                        .write_to_buf(&mut self.write_buf);
                    self.receive_inflight.remove(index);
                } else {
                    tracing::warn!(
                        "Received unexpected PubRel for mid {}.",
                        pub_rel.packet_identifier()
                    );
                }
                Ok(Action::None)
            }
            // Packet types which should never be received by client
            ControlPacketType::Connect => Err(BackendError::UnexpectedPacket(
                "Received Connect as client which should never happen",
            )),
            ControlPacketType::ConnAck => Err(BackendError::UnexpectedPacket(
                "Received unexpected ConnAck package",
            )),
            ControlPacketType::Subscribe => Err(BackendError::UnexpectedPacket(
                "Received Subscribe as client which should never happen",
            )),
            ControlPacketType::Unsubscribe => Err(BackendError::UnexpectedPacket(
                "Received Unsubscribe as client which should never happen",
            )),
            ControlPacketType::PingReq => Err(BackendError::UnexpectedPacket(
                "Received PingReq as client which should never happen",
            )),
            ControlPacketType::Auth => Err(BackendError::MqttError(
                modular_mqtt_protocol::Error::ProtocolError(
                    "Received Auth packet when in mqtt v3 context",
                ),
            )),
        }
    }
}

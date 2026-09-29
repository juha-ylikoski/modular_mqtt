use bytes::{Bytes, BytesMut};
use modular_mqtt_protocol::{
    ControlPacketType, Disconnect, FixedHeader, MqttVersion, Packet, PingReq, PingResp, PubAck,
    PubComp, PubRec, PubRel, Publish, Qos, QosPacketIdentifier, SubAck, Subscribe, UnsubAck,
};
use std::{
    collections::HashMap,
    io::Write,
    net::TcpStream,
    sync::{mpsc, Arc, Mutex, RwLock},
    time::{Duration, Instant},
};
use tracing::instrument;

use crate::{
    client_communication::{ClientCommunicator, SyncData, SyncWakeup},
    client_opts::{exponential_backoff, ClientOpts, OnDisconnectBehavior},
    connection::{SyncReader, SyncWriter},
    error::{BackendError, ConnectError},
    util::{connect_sync, read_into_buf, InflightMessage, InflightMessageState},
};

enum ReadFinished {
    Success(FixedHeader, Bytes),
    TimedOut,
}

enum Action<V>
where
    V: MqttVersion,
{
    None,
    SubAckSend(SubAck<V>),
    UnsubAckSend(UnsubAck<V>),
    ReceiveMsg(Publish<V, QosPacketIdentifier>),
}

pub struct Backend<V, O>
where
    V: MqttVersion,
    O: ClientOpts<V>,
{
    pub broker_addr: String,
    pub read_buf: BytesMut,
    pub write_buf: BytesMut,
    pub opts: Arc<O>,
    pub reader: SyncReader,
    pub writer: Arc<Mutex<SyncWriter>>,
    pub msg_ch: mpsc::Sender<Publish<V, QosPacketIdentifier>>,
    pub suback_comm: ClientCommunicator<SyncData<SubAck<V>>, SyncWakeup>,
    pub unsuback_comm: ClientCommunicator<SyncData<UnsubAck<V>>, SyncWakeup>,
    pub inflight_msgs: HashMap<u16, Arc<InflightMessage<V>>>,
    pub inflight_ch: mpsc::Receiver<(u16, Arc<InflightMessage<V>>)>,
    pub receive_inflight: Vec<u16>,
    pub online: Arc<RwLock<bool>>,
    pub should_die: Arc<Mutex<bool>>,
    pub next_resend_deadline: Option<Instant>,
    pub retry_count: u32,
    pub subscriptions: Arc<Mutex<Vec<V::TopicSubscription>>>,
    pub next_packet_identifier: Arc<std::sync::atomic::AtomicU16>,
}

impl<V, O> Backend<V, O>
where
    V: MqttVersion,
    O: ClientOpts<V>,
{
    fn re_write_qos1_and_qos2_to_write_buf(&mut self) {
        tracing::debug!("Writing qos1 and qos2 messages into stream");
        let now = std::time::Instant::now();
        for (mid, msg) in self.inflight_msgs.iter() {
            match &mut *msg.state.write().unwrap() {
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

    fn handle_msg(
        &mut self,
        fixed_header: FixedHeader,
        mut body: Bytes,
    ) -> Result<Action<V>, BackendError> {
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
                        *inflight.state.read().unwrap(),
                        InflightMessageState::PubAck(_)
                    ) {
                        tracing::debug!("Received PubAck for mid {}", puback.packet_identifier());
                        *inflight.state.write().unwrap() = InflightMessageState::Sent;
                        return Ok(Action::None);
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
                        *inflight.state.read().unwrap(),
                        InflightMessageState::PubRec(_)
                    ) {
                        PubRel::<V>::new_ok(pubrec.packet_identifier())
                            .write_to_buf(&mut self.write_buf);
                        *inflight.state.write().unwrap() =
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
                        *inflight.state.read().unwrap(),
                        InflightMessageState::PubComp(_)
                    ) {
                        tracing::debug!(
                            "Received PubComp for mid {}",
                            pub_comp.packet_identifier()
                        );
                        *inflight.state.write().unwrap() = InflightMessageState::Sent;
                        return Ok(Action::None);
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
                *self.online.write().unwrap() = false;
                return Err(BackendError::Disconnected(disconnect.maybe_reason_code()));
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

    /// Resends any inflight message past its `resend_interval`, and refreshes
    /// `next_resend_deadline` to the earliest remaining due time so `next_read_timeout` doesn't
    /// need its own scan over `inflight_msgs`.
    fn check_resend_msgs(&mut self) {
        let now = Instant::now();
        let mut next_deadline: Option<Instant> = None;
        for (packet_identifier, msg) in self.inflight_msgs.iter() {
            let state = msg.state.read().unwrap().clone();
            let sent_time = match state {
                InflightMessageState::PubAck(sent_time) => {
                    if now.saturating_duration_since(sent_time) > self.opts.resend_interval() {
                        tracing::warn!("Resending packet with identifier {}", packet_identifier);
                        msg.msg.write_to_buf(&mut self.write_buf);
                        *msg.state.write().unwrap() = InflightMessageState::PubAck(now);
                        now
                    } else {
                        sent_time
                    }
                }
                InflightMessageState::PubRec(sent_time) => {
                    tracing::info!("Check resend msg: {msg:?}");
                    if now.saturating_duration_since(sent_time) > self.opts.resend_interval() {
                        tracing::warn!("Resending packet with identifier {}", packet_identifier);
                        msg.msg.write_to_buf(&mut self.write_buf);
                        *msg.state.write().unwrap() = InflightMessageState::PubRec(now);
                        now
                    } else {
                        sent_time
                    }
                }
                InflightMessageState::PubComp(sent_time) => {
                    if now.saturating_duration_since(sent_time) > self.opts.resend_interval() {
                        tracing::warn!("Resending PubRel with identifier {}", packet_identifier);
                        PubRel::<V>::new_ok(*packet_identifier).write_to_buf(&mut self.write_buf);
                        *msg.state.write().unwrap() = InflightMessageState::PubComp(now);
                        now
                    } else {
                        sent_time
                    }
                }
                InflightMessageState::Sent => continue,
            };
            let deadline = sent_time + self.opts.resend_interval();
            next_deadline = Some(next_deadline.map_or(deadline, |d| d.min(deadline)));
        }
        self.next_resend_deadline = next_deadline;
    }

    /// How long the next blocking read may wait: capped to `keep_alive` (so keep-alive pings
    /// stay on schedule), but shortened to `next_resend_deadline` so a short `resend_interval`
    /// isn't silently stretched out to `keep_alive` while idle.
    fn next_read_timeout(&self) -> Duration {
        let keep_alive = Duration::from_secs(self.opts.keep_alive().into());
        match self.next_resend_deadline {
            Some(deadline) => {
                let until_deadline = deadline.saturating_duration_since(Instant::now());
                keep_alive.min(until_deadline).max(Duration::from_millis(1))
            }
            None => keep_alive,
        }
    }

    #[instrument(skip_all,fields(client_id=%self.opts.client_id()))]
    pub fn bg_thread(mut self) -> Result<(), BackendError> {
        tracing::debug!("Start listening for mqtt messages");
        let mut res = self.loop_bg_thread();
        if res.is_err() && *self.should_die.lock().unwrap() {
            tracing::info!("Shutting down!");
            return Ok(());
        }

        if let OnDisconnectBehavior::ReconnectExponentialBackoff {
            min_retry_interval,
            max_retry_interval,
        } = self.opts.on_disconnect()
        {
            loop {
                if let Err(BackendError::IoError(e)) = &res {
                    self.retry_count += 1;
                    tracing::warn!(
                        "Got io error: {e}. Trying to reconnect after {min_retry_interval:?}."
                    );
                    let wait = exponential_backoff(
                        min_retry_interval,
                        max_retry_interval,
                        self.retry_count,
                    );
                    std::thread::sleep(wait);
                    res = self.reconnect();
                    if res.is_ok() {
                        tracing::info!("Successfully reconnected!");
                        self.retry_count = 0;
                        return self.bg_thread();
                    }
                } else {
                    break;
                }
            }
        }

        tracing::error!("Background thread exited due to {res:?}");
        res
    }

    fn reconnect(&mut self) -> Result<(), BackendError> {
        tracing::info!("Reconnecting to mqtt broker");
        self.read_buf.clear();
        self.write_buf.clear();
        let stream = TcpStream::connect(&self.broker_addr)?;
        let mut reader = SyncReader::Tcp(stream.try_clone().unwrap());
        reader.set_read_timeout(Some(Duration::from_secs(self.opts.keep_alive().into())))?;
        let mut writer = SyncWriter::Tcp(stream);

        let connack = connect_sync(
            &*self.opts,
            &mut self.read_buf,
            &mut self.write_buf,
            &mut reader,
            &mut writer,
        )
        .map_err(|e| {
            if let ConnectError::IoError(e) = e {
                BackendError::IoError(e)
            } else {
                BackendError::ReconnectError(e)
            }
        })?;
        if !connack.rc_is_success() {
            return Err(BackendError::ReconnectError(O::connect_error(&connack)));
        }

        if self.opts.should_resubscribe(&connack) {
            let subs = self.subscriptions.lock().unwrap().clone();
            if !subs.is_empty() {
                let msg = Subscribe::<V>::new(
                    self.next_packet_identifier
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                    subs,
                );
                msg.write_to_buf(&mut self.write_buf);
            }
        }

        self.re_write_qos1_and_qos2_to_write_buf();
        *self.writer.lock().unwrap() = writer;
        self.reader = reader;
        self.write_buf_to_stream()?;
        Ok(())
    }

    fn loop_bg_thread(&mut self) -> Result<(), BackendError> {
        loop {
            self.check_resend_msgs();
            loop {
                match self.inflight_ch.try_recv() {
                    Ok((packet_identifier, msg)) => {
                        self.inflight_msgs.insert(packet_identifier, msg);
                    }
                    Err(mpsc::TryRecvError::Empty) => break,
                    Err(mpsc::TryRecvError::Disconnected) => {
                        return Err(BackendError::ChannelError)
                    }
                }
            }
            self.write_buf_to_stream()?;
            match self.read_next_timeout()? {
                ReadFinished::Success(fixed_header, buf) => {
                    match self.handle_msg(fixed_header, buf)? {
                        Action::None => (),
                        Action::SubAckSend(sub_ack) => self
                            .suback_comm
                            .insert(sub_ack.packet_identifier(), sub_ack),
                        Action::UnsubAckSend(unsub_ack) => self
                            .unsuback_comm
                            .insert(unsub_ack.packet_identifier(), unsub_ack),
                        Action::ReceiveMsg(publish) => self.msg_ch.send(publish)?,
                    }
                }
                ReadFinished::TimedOut => {
                    tracing::debug!("Sending ping request to broker");
                    PingReq.write_to_buf(&mut self.write_buf);
                }
            }
            self.write_buf_to_stream()?;
        }
    }

    fn write_buf_to_stream(&mut self) -> std::io::Result<()> {
        if self.write_buf.is_empty() {
            return Ok(());
        }

        let mut writer = self.writer.lock().unwrap();
        let write = self.write_buf.split();
        tracing::trace!("Send data: {write:?}");
        writer.write_all(&write[..])?;
        writer.flush()
    }

    fn read_next_msg(&mut self) -> Result<Option<(FixedHeader, Bytes)>, BackendError> {
        if *self.should_die.lock().unwrap() {
            return Err(BackendError::IoError(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "Disconnected",
            )));
        }

        // Check if we already have a packet in buffer
        if !self.read_buf.is_empty() {
            tracing::trace!("Buffer has data. Try to parse it");
            match FixedHeader::parse(&mut self.read_buf, self.opts.max_packet_size()) {
                Ok(Some(v)) => return Ok(Some(v)),
                Err(e) => return Err(BackendError::MqttError(e)),
                Ok(None) => (),
            }
        }

        self.reader
            .set_read_timeout(Some(self.next_read_timeout()))?;
        match read_into_buf(&mut self.reader, &mut self.read_buf) {
            Ok(0) => Err(BackendError::IoError(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "Stream disconnected",
            ))),
            Ok(_) => FixedHeader::parse(&mut self.read_buf, self.opts.max_packet_size())
                .map_err(BackendError::MqttError),
            Err(e) => match e.kind() {
                std::io::ErrorKind::WouldBlock => Ok(None),
                _ => Err(e)?,
            },
        }
    }

    fn read_next_timeout(&mut self) -> Result<ReadFinished, BackendError> {
        match self.read_next_msg() {
            Ok(Some((header, buf))) => {
                tracing::trace!("Received new packet: header={header:?} body={buf:?}");
                Ok(ReadFinished::Success(header, buf))
            }
            Ok(None) => Ok(ReadFinished::TimedOut),
            Err(e) => Err(e),
        }
    }
}

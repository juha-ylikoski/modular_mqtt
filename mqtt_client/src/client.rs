use rust_mqtt_protocol::{
    ConnAck, Connect, ConnectRc, ControlPacketType, Disconnect, FixedHeader, FixedHeaderError,
    MqttLastWill, MqttTopic, PacketError, PingReq, PingResp, PubAck, PubComp, PubRec, PubRel, Qos,
    ReceivedMessage, SubAck, Subscribe, TopicSubscription, Unsubscribe, UnsubscribeAck,
};
use std::{
    collections::HashMap,
    io::{BufReader, Read, Write},
    net::TcpStream,
    sync::{mpsc, Arc, Mutex, RwLock},
    time::{Duration, SystemTime},
};
use tracing::instrument;

use crate::{
    client_opts::{ClientOpts, OnDisconnectBehavior},
    error::{ClientError, ConnectError},
    sync_connection::{SyncReader, SyncWriter},
    util::{buf_with_size, InflightMessage, InflightMessageState, Message},
    RESENT_INTERVAL,
};

enum ReadFinished {
    Success(FixedHeader, Vec<u8>),
    TimedOut,
}

type BackendThread = Option<std::thread::JoinHandle<Result<(), ClientError>>>;

pub struct SyncClient {
    #[allow(unused)]
    opts: Arc<ClientOpts>,
    next_packet_identifier: Arc<std::sync::atomic::AtomicU16>,
    writer: Arc<Mutex<SyncWriter>>,
    #[allow(unused)]
    backend: Arc<std::sync::Mutex<BackendThread>>,
    msg_ch: mpsc::Receiver<ReceivedMessage>,
    suback_ch: mpsc::Receiver<SubAck>,
    unsuback_ch: mpsc::Receiver<UnsubscribeAck>,
    inflight_ch: mpsc::Sender<(u16, Arc<InflightMessage>)>,
    online: Arc<RwLock<bool>>,
    kill_bg_thread: Arc<Mutex<bool>>,
}
struct SyncClientBackend {
    #[allow(unused)]
    opts: Arc<ClientOpts>,
    reader: SyncReader,
    writer: Arc<Mutex<SyncWriter>>,
    msg_ch: mpsc::Sender<ReceivedMessage>,
    suback_ch: mpsc::Sender<SubAck>,
    unsuback_ch: mpsc::Sender<UnsubscribeAck>,
    inflight_msgs: HashMap<u16, Arc<InflightMessage>>,
    inflight_ch: mpsc::Receiver<(u16, Arc<InflightMessage>)>,
    receive_inflight: Vec<u16>,
    online: Arc<RwLock<bool>>,
    should_die: Arc<Mutex<bool>>,
}
impl SyncClient {
    pub fn connect_tcp(opts: ClientOpts, broker: String) -> Result<Self, ConnectError> {
        let stream = TcpStream::connect(&broker)?;
        let reader = SyncReader::Tcp(BufReader::new(stream.try_clone().unwrap()));
        let writer = Arc::new(Mutex::new(SyncWriter::Tcp(stream)));
        Self::connect_stream(reader, writer, opts)
    }
}
impl SyncClient {
    fn assert_online(&self) {
        if !*self.online.read().unwrap() {
            panic!("Connection to mqtt broker was disconnect. Cannot proceed.");
        }
    }
    pub fn stream(&self) -> &mpsc::Receiver<ReceivedMessage> {
        self.assert_online();
        &self.msg_ch
    }
    fn next_packet_identifier(&self) -> u16 {
        self.next_packet_identifier
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }
    fn handle_connack(reader: &mut impl Read) -> Result<ConnAck, ConnectError> {
        let header = FixedHeader::try_read_sync(reader).map_err(|e| match e {
            FixedHeaderError::IoError(error) => ConnectError::IoError(error),
            _ => ConnectError::ProtocolError(PacketError::InvalidFixedHeader(e)),
        })?;

        tracing::trace!("Got fixed header: {header:?}");
        let header = match &header.control_packet_type {
            rust_mqtt_protocol::ControlPacketType::ConnAck => header,
            _ => {
                tracing::error!("Unexpected packet when expected ConnAck. Got {header:?}");
                return Err(ConnectError::UnexpectedPacket(
                    "Expected ConnAck but did not get it",
                ));
            }
        };
        let mut buf = buf_with_size(header.remaining_length);
        reader.read_exact(&mut buf)?;
        match ConnAck::try_read(header, &buf) {
            Ok(connack) => Ok(connack),
            Err(e) => {
                tracing::trace!("Invalid payload for ConnAck: {buf:?}. Got error: {e}");
                Err(e)?
            }
        }
    }

    #[instrument(skip_all)]
    fn connect_stream(
        mut reader: SyncReader,
        writer: Arc<Mutex<SyncWriter>>,
        opts: ClientOpts,
    ) -> Result<Self, ConnectError> {
        let opts = Arc::new(opts);

        reader
            .set_read_timeout(Some(Duration::from_secs(opts.keep_alive.into())))
            .unwrap();

        let (msg_sender, msg_receiver) = mpsc::channel();
        let (sub_sender, sub_receiver) = mpsc::channel();
        let (unsub_sender, unsub_receiver) = mpsc::channel();
        let (inflight_sender, inflight_receiver) = mpsc::channel();

        let msg = Connect::new_v3(
            opts.clean_session,
            opts.keep_alive,
            &opts.client_id,
            opts.will
                .as_ref()
                .map(|will| MqttLastWill::new(&will.topic, &will.payload, will.retain, will.qos)),
            opts.username.as_deref(),
            opts.password.as_deref(),
        );
        tracing::trace!("Sending Connect: {msg:?}");
        let mut writer_l = writer.lock().unwrap();
        msg.write_to_stream(&mut *writer_l)
            .map_err(|_| crate::error::ConnectError::WriteError)?;
        writer_l.flush()?;
        drop(writer_l);

        let connack = Self::handle_connack(&mut reader)?;

        tracing::debug!("Got ConnAck: {connack:?}");

        let online = Arc::new(RwLock::new(true));
        let killer = Arc::new(Mutex::new(false));

        if connack.connect_rc == ConnectRc::Accepted {
            let bg_opts = opts.clone();
            let be = SyncClientBackend {
                opts: bg_opts,
                reader,
                writer: writer.clone(),
                msg_ch: msg_sender,
                suback_ch: sub_sender,
                unsuback_ch: unsub_sender,
                inflight_msgs: HashMap::new(),
                inflight_ch: inflight_receiver,
                online: online.clone(),
                receive_inflight: Vec::new(),
                should_die: killer.clone(),
            };
            let backend = Arc::new(Mutex::new(Some(std::thread::spawn(|| be.bg_thread()))));
            let client = Self {
                opts,
                writer,
                next_packet_identifier: Arc::new(std::sync::atomic::AtomicU16::new(1)),
                backend,
                msg_ch: msg_receiver,
                suback_ch: sub_receiver,
                unsuback_ch: unsub_receiver,
                inflight_ch: inflight_sender,
                online,
                kill_bg_thread: killer,
            };
            Ok(client)
        } else {
            Err(ConnectError::ConnectFailed(connack.connect_rc))
        }
    }
}

impl SyncClientBackend {
    fn read_next_msg(&mut self) -> Result<(FixedHeader, Vec<u8>), FixedHeaderError> {
        if *self.should_die.lock().unwrap() {
            return Err(FixedHeaderError::IoError(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "Disconnected",
            )));
        }

        let header = FixedHeader::try_read_sync(&mut self.reader)?;
        let mut buf = buf_with_size(header.remaining_length);
        tracing::trace!(
            "Read fixed header: {header:?}. Read next {} bytes",
            header.remaining_length
        );
        self.reader
            .read_exact(&mut buf)
            .map_err(FixedHeaderError::IoError)?;
        Ok((header, buf))
    }

    fn read_next_timeout(&mut self) -> Result<ReadFinished, FixedHeaderError> {
        match self.read_next_msg() {
            Ok((header, buf)) => Ok(ReadFinished::Success(header, buf)),
            Err(e) => match &e {
                FixedHeaderError::IoError(io_error) => match io_error.kind() {
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut => {
                        Ok(ReadFinished::TimedOut)
                    }
                    _ => Err(e),
                },
                _ => Err(e),
            },
        }
    }

    fn add_new_inflights(&mut self) {
        if let Ok((packet_identifier, msg)) = self.inflight_ch.try_recv() {
            self.inflight_msgs.insert(packet_identifier, msg);
        }
    }
    fn resend_msg(
        writer: &mut impl Write,
        packet_identifier: u16,
        msg: &Message,
    ) -> Result<(), std::io::Error> {
        let packet = msg.packet(true, Some(packet_identifier));
        packet.write_to_stream(writer)?;
        writer.flush()?;
        Ok(())
    }
    fn resend_pubrell(
        writer: &mut impl Write,
        packet_identifier: u16,
    ) -> Result<(), std::io::Error> {
        let packet = PubRel::new(packet_identifier);
        packet.write_to_stream(writer)?;
        writer.flush()?;
        Ok(())
    }

    fn check_resend_msgs(&mut self) -> Result<(), std::io::Error> {
        let time = SystemTime::now();
        let mut writer = self.writer.lock().unwrap();
        for (packet_identifier, msg) in self.inflight_msgs.iter() {
            let state = msg.state.read().unwrap().clone();
            match state {
                InflightMessageState::PubAck(sent_time) => {
                    if time.duration_since(sent_time).unwrap() > RESENT_INTERVAL {
                        tracing::warn!("Resending packet with identifier {}", packet_identifier);
                        Self::resend_msg(&mut *writer, *packet_identifier, &msg.msg)?;
                        *msg.state.write().unwrap() = InflightMessageState::PubAck(time);
                    }
                }
                InflightMessageState::PubRec(sent_time) => {
                    tracing::info!("Check resend msg: {msg:?}");
                    if time.duration_since(sent_time).unwrap() > RESENT_INTERVAL {
                        tracing::warn!("Resending packet with identifier {}", packet_identifier);
                        Self::resend_msg(&mut *writer, *packet_identifier, &msg.msg)?;
                        *msg.state.write().unwrap() = InflightMessageState::PubRec(time);
                    }
                }
                InflightMessageState::PubComp(sent_time) => {
                    if time.duration_since(sent_time).unwrap() > RESENT_INTERVAL {
                        tracing::warn!("Resending PubRel with identifier {}", packet_identifier);
                        Self::resend_pubrell(&mut *writer, *packet_identifier)?;
                        *msg.state.write().unwrap() = InflightMessageState::PubComp(time);
                    }
                }
                InflightMessageState::Sent => (),
            }
        }
        Ok(())
    }

    #[instrument(skip_all)]
    fn bg_thread(mut self) -> Result<(), ClientError> {
        tracing::debug!("Start listening for mqtt messages");
        let res = self.loop_bg_thread();
        if res.is_err() && *self.should_die.lock().unwrap() {
            return Ok(());
        }
        tracing::error!("Background thread exited due to {res:?}");
        res
    }

    fn loop_bg_thread(&mut self) -> Result<(), ClientError> {
        loop {
            self.check_resend_msgs()?;
            match self.read_next_timeout()? {
                ReadFinished::Success(fixed_header, vec) => self.handle_msg(fixed_header, vec)?,
                ReadFinished::TimedOut => {
                    tracing::debug!("Sending ping request to broker");
                    let mut writer = self.writer.lock().unwrap();
                    PingReq::write_to_stream(&mut *writer)?;
                    writer.flush()?;
                }
            }
        }
    }
    fn handle_msg(
        &mut self,
        fixed_header: FixedHeader,
        payload: Vec<u8>,
    ) -> Result<(), ClientError> {
        self.add_new_inflights();
        match &fixed_header.control_packet_type {
            ControlPacketType::PingResp => {
                let resp = PingResp::try_read(fixed_header)?;
                tracing::debug!("Received PingResp from server: {resp:?}");
                Ok(())
            }
            ControlPacketType::SubAck => {
                let suback = SubAck::try_read(fixed_header, &payload)?;
                tracing::debug!("Received suback: {suback:?}");
                self.suback_ch.send(suback)?;
                Ok(())
            }
            ControlPacketType::UnsubscribeAck => {
                let unsuback = UnsubscribeAck::try_read(fixed_header, &payload)?;
                tracing::debug!("Received unsuback: {unsuback:?}");
                self.unsuback_ch.send(unsuback)?;
                Ok(())
            }
            ControlPacketType::Publish { .. } => {
                let msg = ReceivedMessage::try_read(fixed_header, payload)?;
                tracing::debug!("Received msg: {:?}", msg);
                let qos = ControlPacketType::flags_qos(msg.flags)
                    .map_err(ClientError::FixedHeaderError)?;
                match (qos, msg.packet_identifier) {
                    (Qos::AtMostOnce, _) => (),
                    (Qos::AtLeastOnce, Some(packet_identifier)) => {
                        let mut writer = self.writer.lock().unwrap();
                        PubAck::new(packet_identifier).write_to_stream(&mut *writer)?;
                        writer.flush()?;
                    }
                    (Qos::ExactlyOnce, Some(packet_identifier)) => {
                        if self.receive_inflight.contains(&packet_identifier) {
                            // TODO should disconnect
                            return Err(ClientError::ProtocolError(PacketError::MalformedPacket(
                                "Received packet with non unique packet identifier",
                            )));
                        }
                        self.receive_inflight.push(packet_identifier);
                        let mut writer = self.writer.lock().unwrap();
                        PubRec::new(packet_identifier).write_to_stream(&mut *writer)?;
                        writer.flush()?;
                    }
                    (Qos::AtLeastOnce, None) | (Qos::ExactlyOnce, None) => {
                        tracing::warn!("Received message with qos={qos:?} and did not receive packet identifier. Sender is misbehaving. Discarding message!");
                        return Err(ClientError::ProtocolError(PacketError::MalformedPacket(
                            "Received qos1 or 2 message without packet identifier",
                        )));
                    }
                }
                self.msg_ch.send(msg)?;
                Ok(())
            }
            ControlPacketType::PubAck => {
                let puback = PubAck::try_read(fixed_header, &payload)?;
                if let Some(inflight) = self.inflight_msgs.remove(&puback.packet_identifier) {
                    if matches!(
                        *inflight.state.read().unwrap(),
                        InflightMessageState::PubAck(_)
                    ) {
                        tracing::debug!("Received PubAck for mid {}", puback.packet_identifier);
                        *inflight.state.write().unwrap() = InflightMessageState::Sent;
                        return Ok(());
                    }
                }
                tracing::warn!(
                    "Received unexpected PubAck for mid {}.",
                    puback.packet_identifier
                );
                Ok(())
            }

            ControlPacketType::PubRec => {
                let pubrec = PubRec::try_read(fixed_header, &payload)?;
                if let Some(inflight) = self.inflight_msgs.get_mut(&pubrec.packet_identifier) {
                    tracing::debug!("Received PubRec for mid {}.", pubrec.packet_identifier);
                    if matches!(
                        *inflight.state.read().unwrap(),
                        InflightMessageState::PubRec(_)
                    ) {
                        let mut writer = self.writer.lock().unwrap();
                        PubRel::new(pubrec.packet_identifier).write_to_stream(&mut *writer)?;
                        writer.flush()?;
                        *inflight.state.write().unwrap() =
                            InflightMessageState::PubComp(SystemTime::now());
                        return Ok(());
                    }
                }
                tracing::warn!(
                    "Received unexpected PubRec for mid {}.",
                    pubrec.packet_identifier
                );
                Ok(())
            }
            ControlPacketType::PubComp => {
                let pub_comp = PubComp::try_read(fixed_header, &payload)?;
                tracing::trace!("Received PubComp: {pub_comp:?}");
                if let Some(inflight) = self.inflight_msgs.remove(&pub_comp.packet_identifier) {
                    if matches!(
                        *inflight.state.read().unwrap(),
                        InflightMessageState::PubComp(_)
                    ) {
                        tracing::debug!("Received PubComp for mid {}", pub_comp.packet_identifier);
                        *inflight.state.write().unwrap() = InflightMessageState::Sent;
                        return Ok(());
                    }
                }
                tracing::warn!(
                    "Received unexpected PubComp for mid {}.",
                    pub_comp.packet_identifier
                );
                Ok(())
            }

            ControlPacketType::Disconnect => {
                *self.online.write().unwrap() = false;
                match self.opts.on_disconnect {
                    OnDisconnectBehavior::Panic => panic!("MQTT broker sent disconnect!"),
                }
            }

            ControlPacketType::PubRel => {
                let pub_rel = PubRel::try_read(fixed_header, &payload)?;
                if let Some(index) = self
                    .receive_inflight
                    .iter()
                    .position(|item| *item == pub_rel.packet_identifier)
                {
                    let mut writer = self.writer.lock().unwrap();
                    PubComp::new(pub_rel.packet_identifier).write_to_stream(&mut *writer)?;
                    writer.flush()?;
                    self.receive_inflight.remove(index);
                }
                Ok(())
            }
            // Packet types which should never be received by client
            ControlPacketType::Connect => Err(ClientError::UnexpectedPacket(
                "Received Connect as client which should never happen",
            )),
            ControlPacketType::ConnAck => Err(ClientError::UnexpectedPacket(
                "Received unexpected ConnAck package",
            )),
            ControlPacketType::Subscribe => Err(ClientError::UnexpectedPacket(
                "Received Subscribe as client which should never happen",
            )),
            ControlPacketType::Unsubscribe => Err(ClientError::UnexpectedPacket(
                "Received Unsubscribe as client which should never happen",
            )),
            ControlPacketType::PingReq => Err(ClientError::UnexpectedPacket(
                "Received PingReq as client which should never happen",
            )),
        }
    }
}

impl SyncClient {
    pub fn subscribe(&self, topics: Vec<MqttTopic>, qos: Qos) -> Result<SubAck, ClientError> {
        self.assert_online();
        let subs = topics
            .into_iter()
            .map(|topic| TopicSubscription::new(topic, qos))
            .collect();
        let packet_identifier = self.next_packet_identifier();
        let msg = Subscribe::new(packet_identifier, subs);
        tracing::debug!("Sending subscribe: {msg:?}");

        let mut stream = self.writer.lock().unwrap();
        msg.write_to_stream(&mut *stream)?;
        stream.flush()?;
        drop(stream);
        let suback = loop {
            let suback = self.suback_ch.recv()?;
            if suback.packet_identifier == packet_identifier {
                break suback;
            }
            tracing::warn!("Received suback with unexpected packet identifier: {suback:?}");
        };
        Ok(suback)
    }
    pub fn unsubscribe(&self, topics: Vec<MqttTopic>) -> Result<UnsubscribeAck, ClientError> {
        self.assert_online();
        let packet_identifier = self.next_packet_identifier();
        let msg = Unsubscribe::new(packet_identifier, topics);
        tracing::debug!("Sending unsubscribe: {msg:?}");

        let mut stream = self.writer.lock().unwrap();
        msg.write_to_stream(&mut *stream)?;
        stream.flush()?;
        drop(stream);
        let suback = loop {
            let suback = self.unsuback_ch.recv()?;
            if suback.packet_identifier == packet_identifier {
                break suback;
            }
            tracing::warn!("Received unsuback with unexpected packet identifier: {suback:?}");
        };
        Ok(suback)
    }

    pub fn publish(&self, msg: Message) -> Result<Option<Arc<InflightMessage>>, ClientError> {
        self.assert_online();
        let mut packet_identifier = None;
        if msg.qos != Qos::AtMostOnce {
            packet_identifier = Some(
                self.next_packet_identifier
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            );
        }

        let packet = msg.packet(false, packet_identifier);
        tracing::debug!("Sending message: {msg:?}");

        let mut stream = self.writer.lock().unwrap();
        packet.write_to_stream(&mut *stream)?;
        stream.flush()?;
        drop(stream);

        let mut inflight = None;
        if let Some(packet_identifier) = packet_identifier {
            let _inflight = Arc::new(match msg.qos {
                Qos::AtMostOnce => panic!("This is a bug!"),
                Qos::AtLeastOnce => InflightMessage {
                    state: RwLock::new(InflightMessageState::PubAck(SystemTime::now())),
                    packet_identifier,
                    msg,
                },
                Qos::ExactlyOnce => InflightMessage {
                    state: RwLock::new(InflightMessageState::PubRec(SystemTime::now())),
                    packet_identifier,
                    msg,
                },
            });

            self.inflight_ch
                .send((packet_identifier, _inflight.clone()))?;
            inflight = Some(_inflight);
        }
        Ok(inflight)
    }
    pub fn online(&self) -> bool {
        *self.online.read().unwrap()
    }

    pub fn disconnect(self) -> Result<(), ClientError> {
        self.assert_online();
        tracing::debug!("Sending Disconnect");
        let mut killer = self.kill_bg_thread.lock().unwrap();
        let mut stream = self.writer.lock().unwrap();
        *killer = true;
        drop(killer);

        Disconnect::write_to_stream(&mut *stream)?;
        stream.flush()?;
        *stream = SyncWriter::Disconnected;
        drop(stream);

        let mut backend = self.backend.lock().unwrap();
        let backend = backend.take();
        if let Some(backend) = backend {
            backend.join().unwrap().unwrap();
        }
        Ok(())
    }
}

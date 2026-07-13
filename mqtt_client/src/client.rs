use bytes::{Bytes, BytesMut};
use rust_mqtt_protocol::{
    ConnAck, Connect, ConnectRcV3, ConnectRcV5, ControlPacketType, Disconnect, FixedHeader,
    MqttTopic, MqttV3_1_1, MqttV5_0_0, PingReq, PingResp, PubAck, PubComp, PubCompReasonCode,
    PubRec, PubRecReasonCode, PubRel, PubRelReasonCode, Publish, Qos, QosPacketIdentifier, SubAck,
    Subscribe, TopicSubscription, UnsubAck, Unsubscribe,
};
use std::{
    collections::HashMap,
    io::{Read, Write},
    net::TcpStream,
    sync::{mpsc, Arc, Mutex, RwLock},
    time::{Duration, SystemTime},
};
use tracing::instrument;

use crate::{
    client_opts::{ClientOpts, MqttLastWill, OnDisconnectBehavior},
    error::{BackendError, ClientError, ConnectError},
    sync_connection::{SyncReader, SyncWriter},
    util::{InflightMessage, InflightMessageState, IntoTopicSubscription},
    RESENT_INTERVAL,
};

const STREAM_READ_CHUNK_SIZE: usize = 4096;

enum ReadFinished {
    Success(FixedHeader, Bytes),
    TimedOut,
}

type BackendThread = Option<std::thread::JoinHandle<Result<(), BackendError>>>;

#[derive(Clone)]
pub struct SyncClient<V> {
    write_buf: Arc<Mutex<BytesMut>>,
    #[allow(unused)]
    opts: Arc<ClientOpts<V>>,
    next_packet_identifier: Arc<std::sync::atomic::AtomicU16>,
    writer: Arc<Mutex<SyncWriter>>,
    backend: Arc<std::sync::Mutex<BackendThread>>,
    msg_ch: Arc<mpsc::Receiver<Publish<V, QosPacketIdentifier>>>,
    suback_ch: Arc<mpsc::Receiver<SubAck<V>>>,
    unsuback_ch: Arc<mpsc::Receiver<UnsubAck<V>>>,
    inflight_ch: mpsc::Sender<(u16, Arc<InflightMessage<V>>)>,
    online: Arc<RwLock<bool>>,
    kill_bg_thread: Arc<Mutex<bool>>,
}

struct SyncClientBackend<V> {
    read_buf: BytesMut,
    write_buf: BytesMut,
    #[allow(unused)]
    opts: Arc<ClientOpts<V>>,
    reader: SyncReader,
    writer: Arc<Mutex<SyncWriter>>,
    msg_ch: mpsc::Sender<Publish<V, QosPacketIdentifier>>,
    suback_ch: mpsc::Sender<SubAck<V>>,
    unsuback_ch: mpsc::Sender<UnsubAck<V>>,
    inflight_msgs: HashMap<u16, Arc<InflightMessage<V>>>,
    inflight_ch: mpsc::Receiver<(u16, Arc<InflightMessage<V>>)>,
    receive_inflight: Vec<u16>,
    online: Arc<RwLock<bool>>,
    should_die: Arc<Mutex<bool>>,
}

fn read_into_buf(reader: &mut impl Read, buf: &mut BytesMut) -> std::io::Result<usize> {
    tracing::trace!("Try to read data from stream");
    let old_len = buf.len();
    buf.resize(old_len + STREAM_READ_CHUNK_SIZE, 0);
    let result = reader.read(&mut buf[old_len..]);
    let n = *result.as_ref().unwrap_or(&0);
    buf.truncate(old_len + n);
    tracing::trace!("Read new data: {:?}", &buf[old_len..]);
    result
}

pub trait MqttClient<V: std::fmt::Debug>: Sized {
    fn connect_stream(
        reader: SyncReader,
        writer: Arc<Mutex<SyncWriter>>,
        opts: ClientOpts<V>,
    ) -> Result<Self, ConnectError>;
    fn topic_subscription(&self, topic: impl IntoTopicSubscription, qos: Qos) -> TopicSubscription;
    fn subscribe_packet(packet_identifier: u16, subs: Vec<TopicSubscription>) -> Subscribe<V>;
    fn unsubscribe_packet(packet_identifier: u16, topics: Vec<MqttTopic>) -> Unsubscribe<V>;
    fn disconnect_packet() -> Disconnect<V>;
}
pub trait MqttBackend<V: std::fmt::Debug>: Sized {
    fn check_resend_msgs(&mut self) -> Result<(), std::io::Error>;
    fn handle_msg(&mut self, fixed_header: FixedHeader, body: Bytes) -> Result<(), BackendError>;
}

impl<V> SyncClient<V>
where
    Self: MqttClient<V>,
    V: std::fmt::Debug,
{
    pub fn connect_tcp(opts: ClientOpts<V>, broker: String) -> Result<Self, ConnectError> {
        let stream = TcpStream::connect(&broker)?;
        let reader = SyncReader::Tcp(stream.try_clone().unwrap());
        let writer = Arc::new(Mutex::new(SyncWriter::Tcp(stream)));
        Self::connect_stream(reader, writer, opts)
    }

    fn assert_online(&self) {
        if !*self.online.read().unwrap() {
            panic!("Connection to mqtt broker was disconnect. Cannot proceed.");
        }
    }
    pub fn stream(&self) -> &mpsc::Receiver<Publish<V, QosPacketIdentifier>> {
        self.assert_online();
        &self.msg_ch
    }

    fn next_packet_identifier(&self) -> u16 {
        self.next_packet_identifier
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }
}

impl SyncClient<MqttV3_1_1> {
    fn send_connect(
        opts: &ClientOpts<MqttV3_1_1>,
        buf: &mut BytesMut,
        writer: &Mutex<SyncWriter>,
    ) -> Result<(), std::io::Error> {
        let mut msg = Connect::new_v3(
            opts.clean_session,
            opts.keep_alive,
            opts.client_id.clone(),
            opts.will.clone().map(|will| match will {
                MqttLastWill::V3 { will, .. } => will,
                MqttLastWill::V5 { .. } => unreachable!(),
            }),
            opts.username.clone(),
            opts.password.clone(),
        );
        tracing::trace!("Sending Connect: {msg:?}");
        let mut writer_l = writer.lock().unwrap();
        msg.write_to_buf(buf);
        tracing::trace!("Send data: {buf:?}");
        writer_l.write_all(&buf[..])?;
        buf.truncate(0);
        writer_l.flush()
    }

    fn handle_connack(
        buf: &mut BytesMut,
        reader: &mut impl Read,
        max_packet_size: usize,
    ) -> Result<ConnAck<MqttV3_1_1>, ConnectError> {
        let (header, mut body) = loop {
            read_into_buf(reader, buf)?;
            match FixedHeader::parse(buf, max_packet_size) {
                Ok(Some((header, body))) => {
                    if header.control_packet_type != ControlPacketType::ConnAck {
                        return Err(ConnectError::UnexpectedPacket {
                            expected: ControlPacketType::ConnAck,
                            received: header.control_packet_type,
                        });
                    }
                    break (header, body);
                }
                Ok(None) => continue,
                Err(e) => return Err(ConnectError::MqttError(e)),
            };
        };

        tracing::trace!("Got connack header={header:?} body={body:?}");

        match ConnAck::try_read_v3(header, &mut body) {
            Ok(connack) => Ok(connack),
            Err(e) => {
                tracing::trace!("Invalid payload for ConnAck: {buf:?}. Got error: {e:?}");
                Err(ConnectError::MqttError(e))
            }
        }
    }
}

impl MqttClient<MqttV3_1_1> for SyncClient<MqttV3_1_1> {
    fn connect_stream(
        mut reader: SyncReader,
        writer: Arc<Mutex<SyncWriter>>,
        opts: ClientOpts<MqttV3_1_1>,
    ) -> Result<Self, ConnectError> {
        let opts = Arc::new(opts);
        let mut read_buf = BytesMut::with_capacity(STREAM_READ_CHUNK_SIZE);
        let mut write_buf = BytesMut::with_capacity(STREAM_READ_CHUNK_SIZE);

        reader.set_read_timeout(Some(Duration::from_secs(opts.keep_alive.into())))?;

        let (msg_sender, msg_receiver) = mpsc::channel();
        let (sub_sender, sub_receiver) = mpsc::channel();
        let (unsub_sender, unsub_receiver) = mpsc::channel();
        let (inflight_sender, inflight_receiver) = mpsc::channel();

        Self::send_connect(&opts, &mut write_buf, &writer)?;
        let connack = Self::handle_connack(&mut read_buf, &mut reader, opts.max_packet_size)?;

        tracing::debug!("Got ConnAck: {connack:?}");

        let online = Arc::new(RwLock::new(true));
        let killer = Arc::new(Mutex::new(false));

        if connack.connect_rc() == ConnectRcV3::Accepted {
            let bg_opts = opts.clone();
            let be = SyncClientBackend {
                read_buf,
                write_buf,
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
                write_buf: Arc::new(Mutex::new(BytesMut::with_capacity(STREAM_READ_CHUNK_SIZE))),
                opts,
                writer,
                next_packet_identifier: Arc::new(std::sync::atomic::AtomicU16::new(1)),
                backend,
                msg_ch: Arc::new(msg_receiver),
                suback_ch: Arc::new(sub_receiver),
                unsuback_ch: Arc::new(unsub_receiver),
                inflight_ch: inflight_sender,
                online,
                kill_bg_thread: killer,
            };
            Ok(client)
        } else {
            Err(ConnectError::ConnectFailedV3(connack.connect_rc()))
        }
    }
    fn topic_subscription(&self, topic: impl IntoTopicSubscription, qos: Qos) -> TopicSubscription {
        topic.into_topic_subscription(
            false,
            qos,
            false,
            false,
            rust_mqtt_protocol::RetainHandling::SendAtSubscribe,
        )
    }

    fn subscribe_packet(
        packet_identifier: u16,
        subs: Vec<TopicSubscription>,
    ) -> Subscribe<MqttV3_1_1> {
        Subscribe::new_v3(packet_identifier, subs)
    }

    fn unsubscribe_packet(
        packet_identifier: u16,
        topics: Vec<MqttTopic>,
    ) -> Unsubscribe<MqttV3_1_1> {
        Unsubscribe::new_v3(packet_identifier, topics)
    }
    fn disconnect_packet() -> Disconnect<MqttV3_1_1> {
        Disconnect::new_v3()
    }
}

impl SyncClient<MqttV5_0_0> {
    fn send_connect(
        opts: &ClientOpts<MqttV5_0_0>,
        buf: &mut BytesMut,
        writer: &Mutex<SyncWriter>,
    ) -> Result<(), std::io::Error> {
        let mut msg = Connect::new_v5(
            opts.clean_session,
            opts.keep_alive,
            opts.client_id.clone(),
            opts.will.clone().map(|will| match will {
                MqttLastWill::V3 { .. } => unreachable!(),
                MqttLastWill::V5 { will, .. } => will,
            }),
            opts.username.clone(),
            opts.password.clone(),
        );
        tracing::trace!("Sending Connect: {msg:?}");
        let mut writer_l = writer.lock().unwrap();
        msg.write_to_buf(buf);
        tracing::trace!("Send data: {buf:?}");
        writer_l.write_all(&buf[..])?;
        buf.truncate(0);
        writer_l.flush()
    }

    fn handle_connack(
        buf: &mut BytesMut,
        reader: &mut impl Read,
        max_packet_size: usize,
    ) -> Result<ConnAck<MqttV5_0_0>, ConnectError> {
        let (header, mut body) = loop {
            read_into_buf(reader, buf)?;
            match FixedHeader::parse(buf, max_packet_size) {
                Ok(Some((header, body))) => {
                    if header.control_packet_type != ControlPacketType::ConnAck {
                        return Err(ConnectError::UnexpectedPacket {
                            expected: ControlPacketType::ConnAck,
                            received: header.control_packet_type,
                        });
                    }
                    break (header, body);
                }
                Ok(None) => continue,
                Err(e) => return Err(ConnectError::MqttError(e)),
            };
        };

        tracing::trace!("Got connack header={header:?} body={body:?}");

        match ConnAck::try_read_v5(header, &mut body) {
            Ok(connack) => Ok(connack),
            Err(e) => {
                tracing::trace!("Invalid payload for ConnAck: {buf:?}. Got error: {e:?}");
                Err(ConnectError::MqttError(e))
            }
        }
    }
}

impl MqttClient<MqttV5_0_0> for SyncClient<MqttV5_0_0> {
    fn connect_stream(
        mut reader: SyncReader,
        writer: Arc<Mutex<SyncWriter>>,
        opts: ClientOpts<MqttV5_0_0>,
    ) -> Result<Self, ConnectError> {
        let opts = Arc::new(opts);
        let mut read_buf = BytesMut::with_capacity(STREAM_READ_CHUNK_SIZE);
        let mut write_buf = BytesMut::with_capacity(STREAM_READ_CHUNK_SIZE);

        reader.set_read_timeout(Some(Duration::from_secs(opts.keep_alive.into())))?;

        let (msg_sender, msg_receiver) = mpsc::channel();
        let (sub_sender, sub_receiver) = mpsc::channel();
        let (unsub_sender, unsub_receiver) = mpsc::channel();
        let (inflight_sender, inflight_receiver) = mpsc::channel();

        Self::send_connect(&opts, &mut write_buf, &writer)?;
        let connack = Self::handle_connack(&mut read_buf, &mut reader, opts.max_packet_size)?;

        tracing::debug!("Got ConnAck: {connack:?}");

        let online = Arc::new(RwLock::new(true));
        let killer = Arc::new(Mutex::new(false));

        if connack.connect_rc() == ConnectRcV5::Accepted {
            let bg_opts = opts.clone();
            let be = SyncClientBackend {
                read_buf,
                write_buf,
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
                write_buf: Arc::new(Mutex::new(BytesMut::with_capacity(STREAM_READ_CHUNK_SIZE))),
                opts,
                writer,
                next_packet_identifier: Arc::new(std::sync::atomic::AtomicU16::new(1)),
                backend,
                msg_ch: Arc::new(msg_receiver),
                suback_ch: Arc::new(sub_receiver),
                unsuback_ch: Arc::new(unsub_receiver),
                inflight_ch: inflight_sender,
                online,
                kill_bg_thread: killer,
            };
            Ok(client)
        } else {
            Err(ConnectError::ConnectFailedV5(connack.connect_rc()))
        }
    }

    fn topic_subscription(&self, topic: impl IntoTopicSubscription, qos: Qos) -> TopicSubscription {
        match self.opts.extra_opts {
            crate::client_opts::ExtraOptions::V3 { .. } => unreachable!(),
            crate::client_opts::ExtraOptions::V5 {
                subscription_no_local,
                subscription_keep_retain,
                subscription_retain_handling,
                ..
            } => topic.into_topic_subscription(
                true,
                qos,
                subscription_no_local,
                subscription_keep_retain,
                subscription_retain_handling,
            ),
        }
    }

    fn subscribe_packet(
        packet_identifier: u16,
        subs: Vec<TopicSubscription>,
    ) -> Subscribe<MqttV5_0_0> {
        Subscribe::new_v5(packet_identifier, subs, None, Vec::new())
    }
    fn unsubscribe_packet(
        packet_identifier: u16,
        topics: Vec<MqttTopic>,
    ) -> Unsubscribe<MqttV5_0_0> {
        Unsubscribe::new_v5(packet_identifier, topics, Vec::new())
    }
    fn disconnect_packet() -> Disconnect<MqttV5_0_0> {
        Disconnect::new_v5(
            rust_mqtt_protocol::DisconnectReasonCode::Normal,
            None,
            None,
            Vec::new(),
            None,
        )
    }
}

impl<V> SyncClientBackend<V>
where
    Self: MqttBackend<V>,
    V: std::fmt::Debug,
{
    #[instrument(skip_all,fields(client_id=%self.opts.client_id))]
    fn bg_thread(mut self) -> Result<(), BackendError> {
        tracing::debug!("Start listening for mqtt messages");
        let res = self.loop_bg_thread();
        if res.is_err() && *self.should_die.lock().unwrap() {
            tracing::info!("Shutting down!");
            return Ok(());
        }
        tracing::error!("Background thread exited due to {res:?}");
        res
    }

    fn loop_bg_thread(&mut self) -> Result<(), BackendError> {
        loop {
            self.check_resend_msgs()?;
            match self.read_next_timeout()? {
                ReadFinished::Success(fixed_header, buf) => self.handle_msg(fixed_header, buf)?,
                ReadFinished::TimedOut => {
                    tracing::debug!("Sending ping request to broker");
                    PingReq::write_to_buf(&mut self.write_buf);
                    self.write_buf_to_stream()?;
                }
            }
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
            match FixedHeader::parse(&mut self.read_buf, self.opts.max_packet_size) {
                Ok(Some(v)) => return Ok(Some(v)),
                Err(e) => return Err(BackendError::MqttError(e)),
                Ok(None) => (),
            }
        }

        match read_into_buf(&mut self.reader, &mut self.read_buf) {
            Ok(0) => Err(BackendError::IoError(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "Stream disconnected",
            ))),
            Ok(_) => FixedHeader::parse(&mut self.read_buf, self.opts.max_packet_size)
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

    fn add_new_inflights(&mut self) {
        if let Ok((packet_identifier, msg)) = self.inflight_ch.try_recv() {
            self.inflight_msgs.insert(packet_identifier, msg);
        }
    }
}

impl MqttBackend<MqttV3_1_1> for SyncClientBackend<MqttV3_1_1> {
    fn check_resend_msgs(&mut self) -> Result<(), std::io::Error> {
        let time = SystemTime::now();
        for (packet_identifier, msg) in self.inflight_msgs.iter() {
            let state = msg.state.read().unwrap().clone();
            match state {
                InflightMessageState::PubAck(sent_time) => {
                    if time.duration_since(sent_time).unwrap() > RESENT_INTERVAL {
                        tracing::warn!("Resending packet with identifier {}", packet_identifier);
                        msg.msg.write_to_buf(&mut self.write_buf);
                        *msg.state.write().unwrap() = InflightMessageState::PubAck(time);
                    }
                }
                InflightMessageState::PubRec(sent_time) => {
                    tracing::info!("Check resend msg: {msg:?}");
                    if time.duration_since(sent_time).unwrap() > RESENT_INTERVAL {
                        tracing::warn!("Resending packet with identifier {}", packet_identifier);
                        msg.msg.write_to_buf(&mut self.write_buf);
                        *msg.state.write().unwrap() = InflightMessageState::PubRec(time);
                    }
                }
                InflightMessageState::PubComp(sent_time) => {
                    if time.duration_since(sent_time).unwrap() > RESENT_INTERVAL {
                        tracing::warn!("Resending PubRel with identifier {}", packet_identifier);
                        PubRel::new_v3(*packet_identifier).write_to_buf(&mut self.write_buf);
                        *msg.state.write().unwrap() = InflightMessageState::PubComp(time);
                    }
                }
                InflightMessageState::Sent => (),
            }
        }
        self.write_buf_to_stream()
    }

    fn handle_msg(
        &mut self,
        fixed_header: FixedHeader,
        mut body: Bytes,
    ) -> Result<(), BackendError> {
        let body = &mut body;
        self.add_new_inflights();
        let res = match &fixed_header.control_packet_type {
            ControlPacketType::PingResp => {
                let resp = PingResp::try_read(fixed_header, body)?;
                tracing::debug!("Received PingResp from server: {resp:?}");
                Ok(())
            }
            ControlPacketType::SubAck => {
                let suback = SubAck::try_read_v3(fixed_header, body)?;
                tracing::debug!("Received suback: {suback:?}");
                self.suback_ch.send(suback)?;
                Ok(())
            }
            ControlPacketType::UnsubscribeAck => {
                let unsuback = UnsubAck::try_read_v3(fixed_header, body)?;
                tracing::debug!("Received unsuback: {unsuback:?}");
                self.unsuback_ch.send(unsuback)?;
                Ok(())
            }
            ControlPacketType::Publish { .. } => {
                let msg = Publish::try_read_v3(fixed_header, body)?;
                tracing::debug!("Received msg: {:?}", msg);
                match (msg.qos(), msg.packet_identifier()) {
                    (Qos::AtMostOnce, _) => (),
                    (Qos::AtLeastOnce, Some(packet_identifier)) => {
                        tracing::trace!("Respond with PubAck (mid={packet_identifier})");
                        self.receive_inflight.push(packet_identifier);
                        PubAck::new_v3(packet_identifier).write_to_buf(&mut self.write_buf);
                    }
                    (Qos::ExactlyOnce, Some(packet_identifier)) => {
                        tracing::trace!("Respond with PubRec (mid={packet_identifier})");
                        self.receive_inflight.push(packet_identifier);
                        PubRec::new_v3(packet_identifier).write_to_buf(&mut self.write_buf);
                    }
                    (Qos::AtLeastOnce, None) | (Qos::ExactlyOnce, None) => unreachable!(),
                }
                self.msg_ch.send(msg)?;
                Ok(())
            }
            ControlPacketType::PubAck => {
                let puback = PubAck::try_read_v3(fixed_header, body)?;
                if let Some(inflight) = self.inflight_msgs.remove(&puback.packet_identifier()) {
                    if matches!(
                        *inflight.state.read().unwrap(),
                        InflightMessageState::PubAck(_)
                    ) {
                        tracing::debug!("Received PubAck for mid {}", puback.packet_identifier());
                        *inflight.state.write().unwrap() = InflightMessageState::Sent;
                        return Ok(());
                    }
                }
                tracing::warn!(
                    "Received unexpected PubAck for mid {}.",
                    puback.packet_identifier()
                );
                Ok(())
            }

            ControlPacketType::PubRec => {
                let pubrec = PubRec::try_read_v3(fixed_header, body)?;
                if let Some(inflight) = self.inflight_msgs.get_mut(&pubrec.packet_identifier()) {
                    tracing::debug!(
                        "Received PubRec for mid {}. Responding with PubComp",
                        pubrec.packet_identifier()
                    );
                    if matches!(
                        *inflight.state.read().unwrap(),
                        InflightMessageState::PubRec(_)
                    ) {
                        PubRel::new_v3(pubrec.packet_identifier())
                            .write_to_buf(&mut self.write_buf);
                        *inflight.state.write().unwrap() =
                            InflightMessageState::PubComp(SystemTime::now());
                        return Ok(());
                    }
                }
                tracing::warn!(
                    "Received unexpected PubRec for mid {}.",
                    pubrec.packet_identifier()
                );
                Ok(())
            }
            ControlPacketType::PubComp => {
                let pub_comp = PubComp::try_read_v3(fixed_header, body)?;
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
                        return Ok(());
                    }
                }
                tracing::warn!(
                    "Received unexpected PubComp for mid {}.",
                    pub_comp.packet_identifier()
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
                let pub_rel = PubRel::try_read_v3(fixed_header, body)?;
                tracing::trace!("Received PubRel: {pub_rel:?}. Responding with PubComp");
                if let Some(index) = self
                    .receive_inflight
                    .iter()
                    .position(|item| *item == pub_rel.packet_identifier())
                {
                    PubComp::new_v3(pub_rel.packet_identifier()).write_to_buf(&mut self.write_buf);
                    self.receive_inflight.remove(index);
                } else {
                    tracing::warn!(
                        "Received unexpected PubRel for mid {}.",
                        pub_rel.packet_identifier()
                    );
                }
                Ok(())
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
                rust_mqtt_protocol::Error::ProtocolError(
                    "Received Auth packet when in mqtt v3 context",
                ),
            )),
        };
        if res.is_ok() {
            self.write_buf_to_stream()?;
        }
        res
    }
}

impl MqttBackend<MqttV5_0_0> for SyncClientBackend<MqttV5_0_0> {
    fn check_resend_msgs(&mut self) -> Result<(), std::io::Error> {
        let time = SystemTime::now();
        for (packet_identifier, msg) in self.inflight_msgs.iter() {
            let state = msg.state.read().unwrap().clone();
            match state {
                InflightMessageState::PubAck(sent_time) => {
                    if time.duration_since(sent_time).unwrap() > RESENT_INTERVAL {
                        tracing::warn!("Resending packet with identifier {}", packet_identifier);
                        msg.msg.write_to_buf(&mut self.write_buf);
                        *msg.state.write().unwrap() = InflightMessageState::PubAck(time);
                    }
                }
                InflightMessageState::PubRec(sent_time) => {
                    tracing::info!("Check resend msg: {msg:?}");
                    if time.duration_since(sent_time).unwrap() > RESENT_INTERVAL {
                        tracing::warn!("Resending packet with identifier {}", packet_identifier);
                        msg.msg.write_to_buf(&mut self.write_buf);
                        *msg.state.write().unwrap() = InflightMessageState::PubRec(time);
                    }
                }
                InflightMessageState::PubComp(sent_time) => {
                    if time.duration_since(sent_time).unwrap() > RESENT_INTERVAL {
                        tracing::warn!("Resending PubRel with identifier {}", packet_identifier);
                        PubRel::new_v5(
                            *packet_identifier,
                            rust_mqtt_protocol::PubRelReasonCode::Success,
                            None,
                            Vec::new(),
                        )
                        .write_to_buf(&mut self.write_buf);
                        *msg.state.write().unwrap() = InflightMessageState::PubComp(time);
                    }
                }
                InflightMessageState::Sent => (),
            }
        }
        self.write_buf_to_stream()
    }

    fn handle_msg(
        &mut self,
        fixed_header: FixedHeader,
        mut body: Bytes,
    ) -> Result<(), BackendError> {
        let body = &mut body;
        self.add_new_inflights();
        let res = match &fixed_header.control_packet_type {
            ControlPacketType::PingResp => {
                let resp = PingResp::try_read(fixed_header, body)?;
                tracing::debug!("Received PingResp from server: {resp:?}");
                Ok(())
            }
            ControlPacketType::SubAck => {
                let suback = SubAck::try_read_v5(fixed_header, body)?;
                tracing::debug!("Received suback: {suback:?}");
                self.suback_ch.send(suback)?;
                Ok(())
            }
            ControlPacketType::UnsubscribeAck => {
                let unsuback = UnsubAck::try_read_v5(fixed_header, body)?;
                tracing::debug!("Received unsuback: {unsuback:?}");
                self.unsuback_ch.send(unsuback)?;
                Ok(())
            }
            ControlPacketType::Publish { .. } => {
                let msg = Publish::try_read_v5(fixed_header, body)?;
                tracing::debug!("Received msg: {:?}", msg);
                match (msg.qos(), msg.packet_identifier()) {
                    (Qos::AtMostOnce, _) => (),
                    (Qos::AtLeastOnce, Some(packet_identifier)) => {
                        tracing::trace!("Respond with PubAck (mid={packet_identifier})");
                        self.receive_inflight.push(packet_identifier);
                        PubAck::new_v5(
                            packet_identifier,
                            rust_mqtt_protocol::PubAckReasonCode::Success,
                            None,
                            Vec::new(),
                        )
                        .write_to_buf(&mut self.write_buf);
                    }
                    (Qos::ExactlyOnce, Some(packet_identifier)) => {
                        tracing::trace!("Respond with PubRec (mid={packet_identifier})");
                        self.receive_inflight.push(packet_identifier);
                        PubRec::new_v5(
                            packet_identifier,
                            PubRecReasonCode::Success,
                            None,
                            Vec::new(),
                        )
                        .write_to_buf(&mut self.write_buf);
                    }
                    (Qos::AtLeastOnce, None) | (Qos::ExactlyOnce, None) => unreachable!(),
                }
                self.msg_ch.send(msg)?;
                Ok(())
            }
            ControlPacketType::PubAck => {
                let puback = PubAck::try_read_v5(fixed_header, body)?;
                if let Some(inflight) = self.inflight_msgs.remove(&puback.packet_identifier()) {
                    if matches!(
                        *inflight.state.read().unwrap(),
                        InflightMessageState::PubAck(_)
                    ) {
                        tracing::debug!("Received PubAck for mid {}", puback.packet_identifier());
                        *inflight.state.write().unwrap() = InflightMessageState::Sent;
                        return Ok(());
                    }
                }
                tracing::warn!(
                    "Received unexpected PubAck for mid {}.",
                    puback.packet_identifier()
                );
                Ok(())
            }

            ControlPacketType::PubRec => {
                let pubrec = PubRec::try_read_v5(fixed_header, body)?;
                if let Some(inflight) = self.inflight_msgs.get_mut(&pubrec.packet_identifier()) {
                    tracing::debug!(
                        "Received PubRec for mid {}. Responding with PubComp",
                        pubrec.packet_identifier()
                    );
                    if matches!(
                        *inflight.state.read().unwrap(),
                        InflightMessageState::PubRec(_)
                    ) {
                        PubRel::new_v5(
                            pubrec.packet_identifier(),
                            PubRelReasonCode::Success,
                            None,
                            Vec::new(),
                        )
                        .write_to_buf(&mut self.write_buf);
                        *inflight.state.write().unwrap() =
                            InflightMessageState::PubComp(SystemTime::now());
                        return Ok(());
                    }
                }
                tracing::warn!(
                    "Received unexpected PubRec for mid {}.",
                    pubrec.packet_identifier()
                );
                Ok(())
            }
            ControlPacketType::PubComp => {
                let pub_comp = PubComp::try_read_v5(fixed_header, body)?;
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
                        return Ok(());
                    }
                }
                tracing::warn!(
                    "Received unexpected PubComp for mid {}.",
                    pub_comp.packet_identifier()
                );
                Ok(())
            }

            ControlPacketType::Disconnect => {
                *self.online.write().unwrap() = false;
                let disconnect = Disconnect::try_read_v5(fixed_header, body)?;
                match self.opts.on_disconnect {
                    OnDisconnectBehavior::Panic => {
                        panic!("MQTT broker sent disconnect {disconnect:?}!")
                    }
                }
            }

            ControlPacketType::PubRel => {
                let pub_rel = PubRel::try_read_v5(fixed_header, body)?;
                tracing::trace!("Received PubRel: {pub_rel:?}. Responding with PubComp");
                if let Some(index) = self
                    .receive_inflight
                    .iter()
                    .position(|item| *item == pub_rel.packet_identifier())
                {
                    PubComp::new_v5(
                        pub_rel.packet_identifier(),
                        PubCompReasonCode::Success,
                        None,
                        Vec::new(),
                    )
                    .write_to_buf(&mut self.write_buf);
                    self.receive_inflight.remove(index);
                }
                Ok(())
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
            ControlPacketType::Auth => todo!(),
        };
        if res.is_ok() {
            self.write_buf_to_stream()?;
        }
        res
    }
}

impl<V> SyncClient<V> {
    fn with_write_buf(&self, inner: impl Fn(&mut BytesMut)) -> std::io::Result<()> {
        let mut write_buf = self.write_buf.lock().unwrap();
        let mut writer = self.writer.lock().unwrap();
        inner(&mut write_buf);
        let out = write_buf.split();
        tracing::trace!("Send data: {out:?}");
        writer.write_all(&out[..])
    }
    fn handle_recv_error(&self) -> ClientError {
        let be = self.backend.lock().unwrap().take().unwrap();
        if be.is_finished() {
            return match be.join() {
                Ok(Ok(_)) => {
                    panic!("Backend thread has freed sender but did not error out");
                }
                Ok(Err(e)) => ClientError::BackendError(e),
                Err(e) => ClientError::BackendCrashed(e),
            };
        } else {
            panic!("Backend thread has freed sender but is not dead");
        }
    }
}

impl<V> SyncClient<V>
where
    Self: MqttClient<V>,
    V: std::fmt::Debug,
{
    pub fn subscribe(
        &self,
        topics: Vec<impl IntoTopicSubscription>,
        qos: Qos,
    ) -> Result<SubAck<V>, ClientError> {
        self.assert_online();
        let subs = topics
            .into_iter()
            .map(|topic| self.topic_subscription(topic, qos))
            .collect();
        let packet_identifier = self.next_packet_identifier();
        let msg = Self::subscribe_packet(packet_identifier, subs);
        tracing::debug!("Sending subscribe: {msg:?}");

        self.with_write_buf(|write_buf| {
            msg.write_to_buf(write_buf);
        })?;

        let suback = loop {
            let suback = match self.suback_ch.recv() {
                Ok(v) => v,
                Err(_) => return Err(self.handle_recv_error()),
            };
            if suback.packet_identifier() == packet_identifier {
                break suback;
            }
            tracing::warn!("Received suback with unexpected packet identifier: {suback:?}");
        };
        Ok(suback)
    }
    pub fn unsubscribe(&self, topics: Vec<MqttTopic>) -> Result<UnsubAck<V>, ClientError> {
        self.assert_online();
        let packet_identifier = self.next_packet_identifier();
        let msg = Self::unsubscribe_packet(packet_identifier, topics);
        tracing::debug!("Sending unsubscribe: {msg:?}");

        self.with_write_buf(|buf| msg.write_to_buf(buf))?;
        let suback = loop {
            let suback = match self.unsuback_ch.recv() {
                Ok(v) => v,
                Err(_) => return Err(self.handle_recv_error()),
            };
            if suback.packet_identifier() == packet_identifier {
                break suback;
            }
            tracing::warn!("Received unsuback with unexpected packet identifier: {suback:?}");
        };
        Ok(suback)
    }

    pub fn publish(
        &self,
        msg: Publish<V, Qos>,
    ) -> Result<Option<Arc<InflightMessage<V>>>, ClientError> {
        self.assert_online();

        let msg = msg.assign_packet_identifier(
            || {
                self.next_packet_identifier
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            },
            false,
        );
        tracing::debug!("Sending message: {msg:?}");

        self.with_write_buf(|buf| msg.write_to_buf(buf))?;

        let mut inflight = None;
        if let Some(packet_identifier) = msg.packet_identifier() {
            let _inflight = Arc::new(match msg.qos() {
                Qos::AtMostOnce => unreachable!(),
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

            if self
                .inflight_ch
                .send((packet_identifier, _inflight.clone()))
                .is_err()
            {
                return Err(self.handle_recv_error());
            }
            inflight = Some(_inflight);
        }
        Ok(inflight)
    }
    pub fn online(&self) -> bool {
        *self.online.read().unwrap()
    }

    fn kill_bg_thread(&self) {
        let mut killer = self.kill_bg_thread.lock().unwrap();
        *killer = true;
    }

    pub fn disconnect(self) -> Result<(), ClientError> {
        self.assert_online();
        tracing::debug!("Sending Disconnect");
        self.kill_bg_thread();

        self.with_write_buf(|buf| Self::disconnect_packet().write_to_buf(buf))?;
        let mut stream = self.writer.lock().unwrap();
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

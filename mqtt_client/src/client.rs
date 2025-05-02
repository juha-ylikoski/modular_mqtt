use rust_mqtt_protocol::{
    ConnAck, Connect, ConnectRc, ControlPacketType, FixedHeader, FixedHeaderError, MqttLastWill,
    PacketError, PingReq, PingResp, PubAck, PubComp, PubRec, PubRel, Qos, ReceivedMessage, SubAck,
    Subscribe, TopicSubscription,
};
use std::{
    collections::HashMap,
    fmt::write,
    io::{BufReader, Read},
    net::TcpStream,
    sync::{mpsc, Arc, RwLock},
    time::{Duration, SystemTime},
};
use tracing::instrument;

use crate::{
    client_opts::ClientOpts,
    error::{ClientError, ConnectError},
    sync_connection::SyncStream,
    util::{buf_with_size, InflightMessage, InflightMessageState, Message},
    RESENT_INTERVAL,
};

pub trait MqttClient {
    fn subscribe(&mut self, topics: Vec<String>, qos: Qos) -> Result<SubAck, ClientError>;
    fn publish(&mut self, msg: Message) -> Result<Option<Arc<InflightMessage>>, ClientError>;
}

enum ReadFinished {
    Success(FixedHeader, Vec<u8>),
    TimedOut,
}

pub struct SyncClient<S: SyncStream> {
    #[allow(unused)]
    opts: Arc<ClientOpts>,
    next_packet_identifier: Arc<std::sync::atomic::AtomicU16>,
    writer: S,
    #[allow(unused)]
    backend: Arc<std::thread::JoinHandle<Result<(), ClientError>>>,
    msg_ch: mpsc::Receiver<ReceivedMessage>,
    suback_ch: mpsc::Receiver<SubAck>,
    inflight_ch: mpsc::Sender<(u16, Arc<InflightMessage>)>,
}
struct SyncClientBackend<S: SyncStream> {
    #[allow(unused)]
    opts: Arc<ClientOpts>,
    reader: BufReader<S>,
    writer: S,
    msg_ch: mpsc::Sender<ReceivedMessage>,
    suback_ch: mpsc::Sender<SubAck>,
    inflight_msgs: HashMap<u16, Arc<InflightMessage>>,
    inflight_ch: mpsc::Receiver<(u16, Arc<InflightMessage>)>,
}
impl SyncClient<TcpStream> {
    pub fn connect(opts: ClientOpts, broker: String) -> Result<Self, ConnectError> {
        let stream = TcpStream::connect(&broker)?;
        Self::connect_stream(stream, opts)
    }
}
impl<S: SyncStream + 'static> SyncClient<S> {
    pub fn stream(&self) -> &mpsc::Receiver<ReceivedMessage> {
        &self.msg_ch
    }
    fn next_packet_identifier(&self) -> u16 {
        self.next_packet_identifier
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }
    fn handle_connack(reader: &mut BufReader<S>) -> Result<ConnAck, ConnectError> {
        let header = FixedHeader::try_read(reader).map_err(|e| match e {
            FixedHeaderError::IoError(error) => ConnectError::IoError(error),
            _ => ConnectError::ProtocolError(PacketError::from(e)),
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
    pub fn connect_stream(mut stream: S, opts: ClientOpts) -> Result<Self, ConnectError> {
        let opts = Arc::new(opts);

        stream
            .set_read_timeout(Some(Duration::from_secs(opts.keep_alive.into())))
            .unwrap();

        let mut reader = BufReader::new(stream.try_clone()?);
        let (msg_sender, msg_receiver) = mpsc::channel();
        let (sub_sender, sub_receiver) = mpsc::channel();
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
        msg.write_to_stream(&mut stream)
            .map_err(|_| crate::error::ConnectError::WriteError)?;

        let connack = Self::handle_connack(&mut reader)?;

        tracing::debug!("Got ConnAck: {connack:?}");

        if connack.connect_rc == ConnectRc::Accepted {
            let bg_opts = opts.clone();
            let bg_stream = stream.try_clone()?;
            let be = SyncClientBackend {
                opts: bg_opts,
                reader,
                writer: bg_stream,
                msg_ch: msg_sender,
                suback_ch: sub_sender,
                inflight_msgs: HashMap::new(),
                inflight_ch: inflight_receiver,
            };
            let backend = Arc::new(std::thread::spawn(|| be.bg_thread()));
            let client = Self {
                opts,
                writer: stream,
                next_packet_identifier: Arc::new(std::sync::atomic::AtomicU16::new(1)),
                backend,
                msg_ch: msg_receiver,
                suback_ch: sub_receiver,
                inflight_ch: inflight_sender,
            };
            Ok(client)
        } else {
            Err(ConnectError::ConnectFailed(connack.connect_rc))
        }
    }
}

impl<S: SyncStream> SyncClientBackend<S> {
    fn read_next_msg(&mut self) -> Result<(FixedHeader, Vec<u8>), FixedHeaderError> {
        let header = FixedHeader::try_read(&mut self.reader)?;
        let mut buf = buf_with_size(header.remaining_length);
        tracing::trace!(
            "Read fixed header: {header:?}. Read next {} bytes",
            header.remaining_length
        );
        self.reader.read_exact(&mut buf)?;
        Ok((header, buf))
    }

    fn read_next_timeout(&mut self) -> Result<ReadFinished, FixedHeaderError> {
        match self.read_next_msg() {
            Ok((header, buf)) => Ok(ReadFinished::Success(header, buf)),
            Err(e) => {
                tracing::trace!("fail read next: {e:?}");
                match &e {
                    FixedHeaderError::IoError(io_error) => match io_error.kind() {
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut => {
                            Ok(ReadFinished::TimedOut)
                        }
                        _ => Err(e),
                    },
                    _ => Err(e),
                }
            }
        }
    }

    fn add_new_inflights(&mut self) {
        if let Ok((packet_identifier, msg)) = self.inflight_ch.try_recv() {
            self.inflight_msgs.insert(packet_identifier, msg);
        }
    }
    fn resend_msg(
        writer: &mut S,
        packet_identifier: u16,
        msg: &Message,
    ) -> Result<(), std::io::Error> {
        let packet = msg.packet(true, Some(packet_identifier));
        packet.write_to_stream(writer)?;
        writer.flush().unwrap();
        Ok(())
    }
    fn resend_pubrell(writer: &mut S, packet_identifier: u16) -> Result<(), std::io::Error> {
        let packet = PubRel::new(packet_identifier);
        packet.write_to_stream(writer)?;
        writer.flush().unwrap();
        Ok(())
    }

    fn check_resend_msgs(&mut self) -> Result<(), std::io::Error> {
        let time = SystemTime::now();
        for (packet_identifier, msg) in self.inflight_msgs.iter() {
            let state = msg.state.read().unwrap().clone();
            match state {
                InflightMessageState::PubAck(sent_time) => {
                    if time.duration_since(sent_time).unwrap() > RESENT_INTERVAL {
                        tracing::warn!("Resending packet with identifier {}", packet_identifier);
                        Self::resend_msg(&mut self.writer, *packet_identifier, &msg.msg)?;
                        *msg.state.write().unwrap() = InflightMessageState::PubAck(time);
                    }
                }
                InflightMessageState::PubRec(sent_time) => {
                    if time.duration_since(sent_time).unwrap() > RESENT_INTERVAL {
                        tracing::warn!("Resending packet with identifier {}", packet_identifier);
                        Self::resend_msg(&mut self.writer, *packet_identifier, &msg.msg)?;
                        *msg.state.write().unwrap() = InflightMessageState::PubRec(time);
                    }
                }
                InflightMessageState::PubComp(sent_time) => {
                    if time.duration_since(sent_time).unwrap() > RESENT_INTERVAL {
                        tracing::warn!("Resending PubRel with identifier {}", packet_identifier);
                        Self::resend_pubrell(&mut self.writer, *packet_identifier)?;
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
        loop {
            self.check_resend_msgs()?;
            match self.read_next_timeout()? {
                ReadFinished::Success(fixed_header, vec) => self.handle_msg(fixed_header, vec)?,
                ReadFinished::TimedOut => {
                    tracing::debug!("Sending ping request to broker");
                    PingReq::write_to_stream(&mut self.writer)?;
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
            ControlPacketType::UnsubscribeAck => todo!(),
            ControlPacketType::Publish { .. } => {
                let msg = ReceivedMessage::try_read(fixed_header, payload)?;
                tracing::debug!("Received msg: {:?}", msg);
                self.msg_ch.send(msg)?;
                Ok(())
            }
            ControlPacketType::PubAck => {
                let puback = PubAck::try_read(fixed_header, &payload);
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
                let pubrec = PubRec::try_read(fixed_header, &payload);
                if let Some(inflight) = self.inflight_msgs.get_mut(&pubrec.packet_identifier) {
                    tracing::debug!("Received PubRec for mid {}.", pubrec.packet_identifier);
                    if matches!(
                        *inflight.state.read().unwrap(),
                        InflightMessageState::PubRec(_)
                    ) {
                        PubRel::new(pubrec.packet_identifier).write_to_stream(&mut self.writer)?;
                        self.writer.flush()?;
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
                let pub_comp = PubComp::try_read(fixed_header, &payload);
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

            ControlPacketType::Disconnect => todo!(),

            // Packet types which should never be received by client
            ControlPacketType::PubRel => Err(ClientError::UnexpectedPacket(
                "Received PubRel as client which should never happen",
            )),
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

impl<S: SyncStream + 'static> MqttClient for SyncClient<S> {
    fn subscribe(&mut self, topics: Vec<String>, qos: Qos) -> Result<SubAck, ClientError> {
        let subs = topics
            .into_iter()
            .map(|topic| TopicSubscription::new(topic, qos))
            .collect();
        let msg = Subscribe::new(self.next_packet_identifier(), subs);
        tracing::debug!("Sending subscribe: {msg:?}");

        msg.write_to_stream(&mut self.writer)?;
        let suback = self.suback_ch.recv()?;
        Ok(suback)
    }
    fn publish(&mut self, msg: Message) -> Result<Option<Arc<InflightMessage>>, ClientError> {
        let mut packet_identifier = None;
        if msg.qos != Qos::AtMostOnce {
            packet_identifier = Some(
                self.next_packet_identifier
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            );
        }
        let packet = msg.packet(false, packet_identifier);
        tracing::debug!("Sending message: {msg:?}");
        packet.write_to_stream(&mut self.writer)?;
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
        self.writer.flush().unwrap();
        Ok(inflight)
    }
}

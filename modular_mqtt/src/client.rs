use bytes::{Bytes, BytesMut};
use modular_mqtt_protocol::{
    ConnAck, ControlPacketType, Disconnect, FixedHeader, MqttV3_1_1, MqttV5_0_0, MqttVersion,
    Packet, PingReq, PingResp, PubAck, PubComp, PubRec, PubRel, Publish, Qos, QosPacketIdentifier,
    SubAck, Subscribe, TopicSubscriptionV3, TopicSubscriptionV5, UnsubAck, Unsubscribe,
};
use std::{
    collections::HashMap,
    io::{Read, Write},
    net::TcpStream,
    sync::{mpsc, Arc, Mutex, RwLock},
    time::{Duration, Instant},
};
use tracing::instrument;

use crate::{
    client_communication::{ClientCommunicator, SyncData, SyncWakeup},
    client_opts::{exponential_backoff, ClientOpts, MqttOptions, OnDisconnectBehavior},
    connection::{SyncReader, SyncWriter, Writer},
    error::{BackendError, ClientError, ConnectError},
    util::{InflightMessage, InflightMessageState, IntoTopicSubscription},
};

const STREAM_READ_CHUNK_SIZE: usize = 4096;

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

#[derive(Clone)]
pub struct Client<V, W>
where
    W: Writer,
    V: MqttVersion + MqttOptions,
{
    write_buf: Arc<W::Mutex<BytesMut>>,
    #[allow(unused)]
    opts: Arc<ClientOpts<V>>,
    next_packet_identifier: Arc<std::sync::atomic::AtomicU16>,
    writer: Arc<W::Mutex<W>>,
    backend: Arc<W::Mutex<Option<W::BgTask>>>,
    suback_comm: ClientCommunicator<W::CommunicatorData<SubAck<V>>, W::CommunicatorWakeup>,
    unsuback_comm: ClientCommunicator<W::CommunicatorData<UnsubAck<V>>, W::CommunicatorWakeup>,
    inflight_ch: W::MpscSender<(u16, Arc<InflightMessage<V>>)>,
    online: Arc<RwLock<bool>>,
    kill_bg_thread: Arc<W::Mutex<bool>>,
}

struct ClientBackend<V, W, R>
where
    W: Writer,
    V: MqttVersion + MqttOptions,
{
    broker_addr: String,
    read_buf: BytesMut,
    write_buf: BytesMut,
    opts: Arc<ClientOpts<V>>,
    reader: R,
    writer: Arc<W::Mutex<W>>,
    msg_ch: W::MpscSender<Publish<V, QosPacketIdentifier>>,
    suback_comm: ClientCommunicator<W::CommunicatorData<SubAck<V>>, W::CommunicatorWakeup>,
    unsuback_comm: ClientCommunicator<W::CommunicatorData<UnsubAck<V>>, W::CommunicatorWakeup>,
    inflight_msgs: HashMap<u16, Arc<InflightMessage<V>>>,
    inflight_ch: W::MpscReceiver<(u16, Arc<InflightMessage<V>>)>,
    receive_inflight: Vec<u16>,
    online: Arc<RwLock<bool>>,
    should_die: Arc<W::Mutex<bool>>,
    next_resend_deadline: Option<Instant>,
    retry_count: u32,
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

#[cfg(feature = "async")]
async fn read_into_buf_async(
    reader: &mut crate::connection::async_stream::AsyncReader,
    buf: &mut BytesMut,
) -> std::io::Result<usize> {
    use tokio::io::AsyncReadExt;

    tracing::trace!("Try to read data from stream");
    let old_len = buf.len();
    buf.resize(old_len + STREAM_READ_CHUNK_SIZE, 0);
    let result = reader.read(&mut buf[old_len..]).await;
    let n = *result.as_ref().unwrap_or(&0);
    buf.truncate(old_len + n);
    tracing::trace!("Read new data: {:?}", &buf[old_len..]);
    result
}

pub trait MqttClient<V: MqttVersion>: Sized {
    fn topic_subscription(
        &self,
        topic: impl IntoTopicSubscription<V>,
        qos: Qos,
    ) -> V::TopicSubscription;
    fn connect_error(connack: &ConnAck<V>) -> ConnectError;
    fn subscribe_packet(packet_identifier: u16, subs: Vec<V::TopicSubscription>) -> Subscribe<V>;
    fn unsubscribe_packet(packet_identifier: u16, topics: Vec<String>) -> Unsubscribe<V>;
    fn disconnect_packet() -> Disconnect<V>;
}

impl<V, W> Client<V, W>
where
    Self: MqttClient<V>,
    V: MqttVersion + MqttOptions,
    W: Writer,
{
    fn assert_online(&self) {
        if !*self.online.read().unwrap() {
            panic!("Connection to mqtt broker was disconnect. Cannot proceed.");
        }
    }
    fn next_packet_identifier(&self) -> u16 {
        self.next_packet_identifier
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    fn send_connect(opts: &ClientOpts<V>, buf: &mut BytesMut) {
        let msg = opts.connect_msg();
        tracing::trace!("Sending Connect: {msg:?}");
        msg.write_to_buf(buf);
    }
    fn handle_connack(header: FixedHeader, body: &mut Bytes) -> Result<ConnAck<V>, ConnectError> {
        tracing::trace!("Got connack header={header:?} body={body:?}");

        match ConnAck::try_read_entire_buf(header, body) {
            Ok(connack) => Ok(connack),
            Err(e) => {
                tracing::trace!("Invalid body for ConnAck: {body:?}. Got error: {e:?}");
                Err(ConnectError::MqttError(e))
            }
        }
    }
}

impl<V> Client<V, SyncWriter>
where
    Self: MqttClient<V>,
    V: MqttVersion + MqttOptions,
{
    pub fn connect_tcp(
        opts: ClientOpts<V>,
        broker: String,
    ) -> Result<
        (
            std::sync::mpsc::Receiver<Publish<V, QosPacketIdentifier>>,
            Self,
        ),
        ConnectError,
    > {
        let stream = TcpStream::connect(&broker)?;
        let mut reader = SyncReader::Tcp(stream.try_clone().unwrap());
        let writer = Arc::new(Mutex::new(SyncWriter::Tcp(stream)));
        let opts = Arc::new(opts);
        let mut read_buf = BytesMut::with_capacity(STREAM_READ_CHUNK_SIZE);
        let mut write_buf = BytesMut::with_capacity(STREAM_READ_CHUNK_SIZE);

        reader.set_read_timeout(Some(Duration::from_secs(opts.keep_alive.into())))?;

        let (msg_sender, msg_receiver) = mpsc::channel();
        let suback_comm = ClientCommunicator::<SyncData<_>, SyncWakeup>::new(opts.ack_retention);
        let unsuback_comm = ClientCommunicator::<SyncData<_>, SyncWakeup>::new(opts.ack_retention);
        let (inflight_sender, inflight_receiver) = mpsc::channel();

        Self::send_connect(&opts, &mut write_buf);
        {
            tracing::trace!("Send data: {write_buf:?}");
            let mut writer_l = writer.lock().unwrap();
            writer_l.write_all(&write_buf[..])?;
            write_buf.truncate(0);
            writer_l.flush()?;
        }

        let (header, mut body) = loop {
            read_into_buf(&mut reader, &mut read_buf)?;
            match FixedHeader::parse(&mut read_buf, opts.max_packet_size) {
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
        let connack = Self::handle_connack(header, &mut body)?;

        tracing::debug!("Got ConnAck: {connack:?}");

        let online = Arc::new(RwLock::new(true));
        let killer = Arc::new(Mutex::new(false));

        if connack.rc_is_success() {
            let bg_opts = opts.clone();
            let be = ClientBackend {
                broker_addr: broker,
                read_buf,
                write_buf,
                opts: bg_opts,
                reader,
                writer: writer.clone(),
                msg_ch: msg_sender,
                suback_comm: suback_comm.clone(),
                unsuback_comm: unsuback_comm.clone(),
                inflight_msgs: HashMap::new(),
                inflight_ch: inflight_receiver,
                online: online.clone(),
                receive_inflight: Vec::new(),
                next_resend_deadline: None,
                should_die: killer.clone(),
                retry_count: 0,
            };
            let backend = Arc::new(Mutex::new(Some(std::thread::spawn(|| be.bg_thread()))));
            let client = Self {
                write_buf: Arc::new(Mutex::new(BytesMut::with_capacity(STREAM_READ_CHUNK_SIZE))),
                opts,
                writer,
                next_packet_identifier: Arc::new(std::sync::atomic::AtomicU16::new(1)),
                backend,
                suback_comm,
                unsuback_comm,
                inflight_ch: inflight_sender,
                online,
                kill_bg_thread: killer,
            };
            Ok((msg_receiver, client))
        } else {
            Err(Self::connect_error(&connack))
        }
    }
}

#[cfg(feature = "async")]
impl<V> Client<V, crate::connection::async_stream::AsyncWriter>
where
    Self: MqttClient<V>,
    V: MqttVersion + MqttOptions,
{
    pub async fn connect_tcp(
        opts: ClientOpts<V>,
        broker: String,
    ) -> Result<
        (
            tokio::sync::mpsc::Receiver<Publish<V, QosPacketIdentifier>>,
            Self,
        ),
        ConnectError,
    > {
        use tokio::io::AsyncWriteExt;
        use tokio::sync::Mutex;

        use crate::client_communication::async_communicator::{AsyncData, AsyncWakeup};
        use crate::connection::async_stream::{AsyncReader, AsyncWriter};

        let stream = tokio::net::TcpStream::connect(&broker).await?;
        let (read_half, write_half) = stream.into_split();
        let mut reader = AsyncReader::Tcp(read_half);
        let writer = Arc::new(Mutex::new(AsyncWriter::Tcp(write_half)));
        let opts = Arc::new(opts);
        let mut read_buf = BytesMut::with_capacity(STREAM_READ_CHUNK_SIZE);
        let mut write_buf = BytesMut::with_capacity(STREAM_READ_CHUNK_SIZE);

        let (msg_sender, msg_receiver) = tokio::sync::mpsc::channel(100);
        let suback_comm = ClientCommunicator::<AsyncData<_>, AsyncWakeup>::new(opts.ack_retention);
        let unsuback_comm =
            ClientCommunicator::<AsyncData<_>, AsyncWakeup>::new(opts.ack_retention);
        let (inflight_sender, inflight_receiver) = tokio::sync::mpsc::channel(100);

        Self::send_connect(&opts, &mut write_buf);
        {
            tracing::trace!("Send data: {write_buf:?}");
            let mut writer_l = writer.lock().await;
            writer_l.write_all(&write_buf[..]).await?;
            write_buf.truncate(0);
            writer_l.flush().await?;
        }

        let (header, mut body) = loop {
            read_into_buf_async(&mut reader, &mut read_buf).await?;
            match FixedHeader::parse(&mut read_buf, opts.max_packet_size) {
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
        let connack = Self::handle_connack(header, &mut body)?;

        tracing::debug!("Got ConnAck: {connack:?}");

        let online = Arc::new(RwLock::new(true));
        let killer = Arc::new(Mutex::new(false));

        if connack.rc_is_success() {
            let bg_opts = opts.clone();
            let be = ClientBackend {
                broker_addr: broker,
                read_buf,
                write_buf,
                opts: bg_opts,
                reader,
                writer: writer.clone(),
                msg_ch: msg_sender,
                suback_comm: suback_comm.clone(),
                unsuback_comm: unsuback_comm.clone(),
                inflight_msgs: HashMap::new(),
                inflight_ch: inflight_receiver,
                online: online.clone(),
                receive_inflight: Vec::new(),
                next_resend_deadline: None,
                should_die: killer.clone(),
                retry_count: 0,
            };
            let backend = Arc::new(Mutex::new(Some(tokio::task::spawn(be.bg_thread()))));
            let client = Self {
                write_buf: Arc::new(Mutex::new(BytesMut::with_capacity(STREAM_READ_CHUNK_SIZE))),
                opts,
                writer,
                next_packet_identifier: Arc::new(std::sync::atomic::AtomicU16::new(1)),
                backend,
                suback_comm,
                unsuback_comm,
                inflight_ch: inflight_sender,
                online,
                kill_bg_thread: killer,
            };
            Ok((msg_receiver, client))
        } else {
            Err(Self::connect_error(&connack))
        }
    }
}

impl<W> MqttClient<MqttV3_1_1> for Client<MqttV3_1_1, W>
where
    W: Writer,
{
    fn topic_subscription(
        &self,
        topic: impl IntoTopicSubscription<MqttV3_1_1>,
        qos: Qos,
    ) -> TopicSubscriptionV3 {
        topic.into_topic_subscription(
            qos,
            false,
            false,
            modular_mqtt_protocol::RetainHandling::SendAtSubscribe,
        )
    }

    fn connect_error(connack: &ConnAck<MqttV3_1_1>) -> ConnectError {
        ConnectError::ConnectFailedV3(connack.connect_rc())
    }

    fn subscribe_packet(
        packet_identifier: u16,
        subs: Vec<TopicSubscriptionV3>,
    ) -> Subscribe<MqttV3_1_1> {
        Subscribe::new(packet_identifier, subs)
    }

    fn unsubscribe_packet(packet_identifier: u16, topics: Vec<String>) -> Unsubscribe<MqttV3_1_1> {
        Unsubscribe::new_v3(packet_identifier, topics)
    }
    fn disconnect_packet() -> Disconnect<MqttV3_1_1> {
        Disconnect::new_v3()
    }
}

impl<W> MqttClient<MqttV5_0_0> for Client<MqttV5_0_0, W>
where
    W: Writer,
{
    fn topic_subscription(
        &self,
        topic: impl IntoTopicSubscription<MqttV5_0_0>,
        qos: Qos,
    ) -> TopicSubscriptionV5 {
        topic.into_topic_subscription(
            qos,
            self.opts.extra_opts.subscription_no_local,
            self.opts.extra_opts.subscription_keep_retain,
            self.opts.extra_opts.subscription_retain_handling,
        )
    }

    fn connect_error(connack: &ConnAck<MqttV5_0_0>) -> ConnectError {
        ConnectError::ConnectFailedV5(connack.connect_rc())
    }

    fn subscribe_packet(
        packet_identifier: u16,
        subs: Vec<TopicSubscriptionV5>,
    ) -> Subscribe<MqttV5_0_0> {
        Subscribe::new_with_options(packet_identifier, subs, None, Vec::new())
    }
    fn unsubscribe_packet(packet_identifier: u16, topics: Vec<String>) -> Unsubscribe<MqttV5_0_0> {
        Unsubscribe::new_v5(packet_identifier, topics, Vec::new())
    }
    fn disconnect_packet() -> Disconnect<MqttV5_0_0> {
        Disconnect::new_v5(
            modular_mqtt_protocol::DisconnectReasonCode::Normal,
            None,
            None,
            Vec::new(),
            None,
        )
    }
}

impl<V> ClientBackend<V, SyncWriter, SyncReader>
where
    V: MqttVersion + MqttOptions + std::fmt::Debug,
{
    #[instrument(skip_all,fields(client_id=%self.opts.client_id))]
    fn bg_thread(mut self) -> Result<(), BackendError> {
        tracing::debug!("Start listening for mqtt messages");
        let mut res = self.loop_bg_thread();
        if res.is_err() && *self.should_die.lock().unwrap() {
            tracing::info!("Shutting down!");
            return Ok(());
        }

        if let OnDisconnectBehavior::ReconnectExponentialBackoff {
            min_retry_interval,
            max_retry_interval,
        } = self.opts.on_disconnect
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
        let reader = SyncReader::Tcp(stream.try_clone().unwrap());
        reader.set_read_timeout(Some(Duration::from_secs(self.opts.keep_alive.into())))?;
        let writer = SyncWriter::Tcp(stream);
        *self.writer.lock().unwrap() = writer;
        self.reader = reader;
        self.re_write_qos1_and_qos2_to_write_buf();
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
            match FixedHeader::parse(&mut self.read_buf, self.opts.max_packet_size) {
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
}

#[cfg(feature = "async")]
impl<V>
    ClientBackend<
        V,
        crate::connection::async_stream::AsyncWriter,
        crate::connection::async_stream::AsyncReader,
    >
where
    V: MqttVersion + MqttOptions + std::fmt::Debug,
{
    #[instrument(skip_all,fields(client_id=%self.opts.client_id))]
    async fn bg_thread(mut self) -> Result<(), BackendError> {
        tracing::debug!("Start listening for mqtt messages");
        let mut res = self.loop_bg_thread().await;
        if res.is_err() && *self.should_die.lock().await {
            tracing::info!("Shutting down!");
            return Ok(());
        }

        if let OnDisconnectBehavior::ReconnectExponentialBackoff {
            min_retry_interval,
            max_retry_interval,
        } = self.opts.on_disconnect
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
                    tokio::time::sleep(wait).await;
                    res = self.reconnect().await;
                    if res.is_ok() {
                        tracing::info!("Successfully reconnected!");
                        self.retry_count = 0;
                        return self.bg_thread().await;
                    }
                }
            }
        }

        tracing::error!("Background thread exited due to {res:?}");
        res
    }

    async fn reconnect(&mut self) -> Result<(), BackendError> {
        tracing::info!("Reconnecting to mqtt broker");
        self.read_buf.clear();
        self.write_buf.clear();
        let stream = tokio::net::TcpStream::connect(&self.broker_addr).await?;
        let (read_half, write_half) = stream.into_split();
        let reader = crate::connection::async_stream::AsyncReader::Tcp(read_half);
        let writer = crate::connection::async_stream::AsyncWriter::Tcp(write_half);
        *self.writer.lock().await = writer;
        self.reader = reader;
        self.re_write_qos1_and_qos2_to_write_buf();
        self.write_buf_to_stream().await?;
        Ok(())
    }

    async fn loop_bg_thread(&mut self) -> Result<(), BackendError> {
        loop {
            self.check_resend_msgs();
            while !self.inflight_ch.is_empty() {
                if let Some((packet_identifier, msg)) = self.inflight_ch.recv().await {
                    self.inflight_msgs.insert(packet_identifier, msg);
                } else {
                    return Err(BackendError::ChannelError);
                }
            }
            self.write_buf_to_stream().await?;
            match self.read_next_timeout().await? {
                ReadFinished::Success(fixed_header, buf) => {
                    match self.handle_msg(fixed_header, buf)? {
                        Action::None => (),
                        Action::SubAckSend(sub_ack) => {
                            self.suback_comm
                                .insert(sub_ack.packet_identifier(), sub_ack)
                                .await
                        }
                        Action::UnsubAckSend(unsub_ack) => {
                            self.unsuback_comm
                                .insert(unsub_ack.packet_identifier(), unsub_ack)
                                .await
                        }
                        Action::ReceiveMsg(publish) => self.msg_ch.send(publish).await?,
                    }
                }
                ReadFinished::TimedOut => {
                    tracing::debug!("Sending ping request to broker");
                    PingReq.write_to_buf(&mut self.write_buf);
                }
            }
            self.write_buf_to_stream().await?;
        }
    }

    async fn write_buf_to_stream(&mut self) -> std::io::Result<()> {
        use tokio::io::AsyncWriteExt;

        if self.write_buf.is_empty() {
            return Ok(());
        }

        let mut writer = self.writer.lock().await;
        let write = self.write_buf.split();
        tracing::trace!("Send data: {write:?}");
        writer.write_all(&write[..]).await?;
        writer.flush().await
    }

    async fn read_next_msg(&mut self) -> Result<Option<(FixedHeader, Bytes)>, BackendError> {
        if *self.should_die.lock().await {
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

        match read_into_buf_async(&mut self.reader, &mut self.read_buf).await {
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

    async fn read_next_timeout(&mut self) -> Result<ReadFinished, BackendError> {
        match self.read_next_msg().await {
            Ok(Some((header, buf))) => {
                tracing::trace!("Received new packet: header={header:?} body={buf:?}");
                Ok(ReadFinished::Success(header, buf))
            }
            Ok(None) => Ok(ReadFinished::TimedOut),
            Err(e) => Err(e),
        }
    }
}

impl<V, W, R> ClientBackend<V, W, R>
where
    V: MqttVersion + MqttOptions + std::fmt::Debug,
    W: Writer,
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
                    if now.saturating_duration_since(sent_time) > self.opts.resend_interval {
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
                    if now.saturating_duration_since(sent_time) > self.opts.resend_interval {
                        tracing::warn!("Resending packet with identifier {}", packet_identifier);
                        msg.msg.write_to_buf(&mut self.write_buf);
                        *msg.state.write().unwrap() = InflightMessageState::PubRec(now);
                        now
                    } else {
                        sent_time
                    }
                }
                InflightMessageState::PubComp(sent_time) => {
                    if now.saturating_duration_since(sent_time) > self.opts.resend_interval {
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
            let deadline = sent_time + self.opts.resend_interval;
            next_deadline = Some(next_deadline.map_or(deadline, |d| d.min(deadline)));
        }
        self.next_resend_deadline = next_deadline;
    }

    /// How long the next blocking read may wait: capped to `keep_alive` (so keep-alive pings
    /// stay on schedule), but shortened to `next_resend_deadline` so a short `resend_interval`
    /// isn't silently stretched out to `keep_alive` while idle.
    fn next_read_timeout(&self) -> Duration {
        let keep_alive = Duration::from_secs(self.opts.keep_alive.into());
        match self.next_resend_deadline {
            Some(deadline) => {
                let until_deadline = deadline.saturating_duration_since(Instant::now());
                keep_alive.min(until_deadline).max(Duration::from_millis(1))
            }
            None => keep_alive,
        }
    }
}

impl<V, W> Client<V, W>
where
    Self: MqttClient<V>,
    V: MqttVersion + MqttOptions,
    W: Writer,
{
    fn build_subscribe_msg(
        &self,
        buf: &mut BytesMut,
        topics: Vec<impl IntoTopicSubscription<V>>,
        qos: Qos,
    ) -> Subscribe<V> {
        self.assert_online();
        let subs = topics
            .into_iter()
            .map(|topic| self.topic_subscription(topic, qos))
            .collect();
        let packet_identifier = self.next_packet_identifier();
        let msg = Self::subscribe_packet(packet_identifier, subs);
        tracing::debug!("Sending subscribe: {msg:?}");
        msg.write_to_buf(buf);
        msg
    }
    fn build_unsubscribe_msg(&self, buf: &mut BytesMut, topics: Vec<String>) -> Unsubscribe<V> {
        self.assert_online();
        let packet_identifier = self.next_packet_identifier();
        let msg = Self::unsubscribe_packet(packet_identifier, topics);
        tracing::debug!("Sending unsubscribe: {msg:?}");
        msg.write_to_buf(buf);
        msg
    }
}

impl<V> Client<V, SyncWriter>
where
    Self: MqttClient<V>,
    V: MqttVersion + MqttOptions,
{
    fn flush(&self, write_buf: &mut BytesMut) -> std::io::Result<()> {
        let mut writer = self.writer.lock().unwrap();
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

    pub fn subscribe(
        &self,
        topics: Vec<impl IntoTopicSubscription<V>>,
        qos: Qos,
        timeout: Duration,
    ) -> Result<SubAck<V>, ClientError> {
        let mut write_buf = self.write_buf.lock().unwrap();
        let msg = self.build_subscribe_msg(&mut write_buf, topics, qos);
        self.flush(&mut write_buf)?;

        let suback = self.suback_comm.get(msg.packet_identifier(), timeout)?;
        Ok(suback)
    }
    pub fn unsubscribe(
        &self,
        topics: Vec<String>,
        timeout: Duration,
    ) -> Result<UnsubAck<V>, ClientError> {
        let mut write_buf = self.write_buf.lock().unwrap();
        let msg = self.build_unsubscribe_msg(&mut write_buf, topics);
        self.flush(&mut write_buf)?;
        let suback = self.unsuback_comm.get(msg.packet_identifier(), timeout)?;
        Ok(suback)
    }

    pub fn publish(
        &self,
        msg: Publish<V, Qos>,
    ) -> Result<Option<Arc<InflightMessage<V>>>, ClientError> {
        let mut write_buf = self.write_buf.lock().unwrap();
        self.assert_online();

        let msg = msg.assign_packet_identifier(
            || {
                self.next_packet_identifier
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            },
            false,
        );
        tracing::debug!("Sending message: {msg:?}");

        let mut inflight = None;
        if let Some(packet_identifier) = msg.packet_identifier() {
            let _inflight = Arc::new(match msg.qos() {
                Qos::AtMostOnce => unreachable!(),
                Qos::AtLeastOnce => InflightMessage {
                    state: RwLock::new(InflightMessageState::PubAck(Instant::now())),
                    packet_identifier,
                    msg,
                },
                Qos::ExactlyOnce => InflightMessage {
                    state: RwLock::new(InflightMessageState::PubRec(Instant::now())),
                    packet_identifier,
                    msg,
                },
            });

            _inflight.msg.write_to_buf(&mut *write_buf);

            if self
                .inflight_ch
                .send((packet_identifier, _inflight.clone()))
                .is_err()
            {
                return Err(self.handle_recv_error());
            }
            inflight = Some(_inflight);
        } else {
            msg.write_to_buf(&mut *write_buf);
        }
        self.flush(&mut write_buf)?;
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
        let mut write_buf = self.write_buf.lock().unwrap();
        self.assert_online();
        tracing::debug!("Sending Disconnect");
        self.kill_bg_thread();

        Self::disconnect_packet().write_to_buf(&mut *write_buf);
        self.flush(&mut write_buf)?;
        let mut stream = self.writer.lock().unwrap();
        let old_stream = std::mem::replace(&mut *stream, SyncWriter::Disconnected);
        drop(stream);
        if let SyncWriter::Tcp(tcp) = old_stream {
            // Unblocks the background thread's in-progress socket read (which can otherwise
            // block for up to `keep_alive` seconds) so it notices `kill_bg_thread` promptly.
            let _ = tcp.shutdown(std::net::Shutdown::Both);
        }

        let mut backend = self.backend.lock().unwrap();
        let backend = backend.take();
        if let Some(backend) = backend {
            backend.join().unwrap().unwrap();
        }
        Ok(())
    }
}

#[cfg(feature = "async")]
impl<V> Client<V, crate::connection::async_stream::AsyncWriter>
where
    Self: MqttClient<V>,
    V: MqttVersion + MqttOptions,
{
    async fn flush(&self, write_buf: &mut BytesMut) -> std::io::Result<()> {
        use tokio::io::AsyncWriteExt;

        let mut writer = self.writer.lock().await;
        let out = write_buf.split();
        tracing::trace!("Send data: {out:?}");
        writer.write_all(&out[..]).await
    }
    async fn handle_recv_error(&self) -> ClientError {
        let be = self.backend.lock().await.take().unwrap();
        if be.is_finished() {
            match be.await {
                Ok(Ok(_)) => {
                    panic!("Backend thread has freed sender but did not error out");
                }
                Ok(Err(e)) => ClientError::BackendError(e),
                Err(e) => ClientError::BackendCrashed(Box::new(e)),
            }
        } else {
            panic!("Backend thread has freed sender but is not dead");
        }
    }

    pub async fn subscribe(
        &self,
        topics: Vec<impl IntoTopicSubscription<V>>,
        qos: Qos,
    ) -> Result<SubAck<V>, ClientError> {
        let mut write_buf = self.write_buf.lock().await;
        let msg = self.build_subscribe_msg(&mut write_buf, topics, qos);
        self.flush(&mut write_buf).await?;

        let suback = self.suback_comm.get(msg.packet_identifier()).await?;
        Ok(suback)
    }
    pub async fn unsubscribe(&self, topics: Vec<String>) -> Result<UnsubAck<V>, ClientError> {
        let mut write_buf = self.write_buf.lock().await;
        let msg = self.build_unsubscribe_msg(&mut write_buf, topics);
        self.flush(&mut write_buf).await?;
        let suback = self.unsuback_comm.get(msg.packet_identifier()).await?;
        Ok(suback)
    }

    pub async fn publish(
        &self,
        msg: Publish<V, Qos>,
    ) -> Result<Option<Arc<InflightMessage<V>>>, ClientError> {
        let mut write_buf = self.write_buf.lock().await;
        self.assert_online();

        let msg = msg.assign_packet_identifier(
            || {
                self.next_packet_identifier
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            },
            false,
        );
        tracing::debug!("Sending message: {msg:?}");

        let mut inflight = None;
        if let Some(packet_identifier) = msg.packet_identifier() {
            let _inflight = Arc::new(match msg.qos() {
                Qos::AtMostOnce => unreachable!(),
                Qos::AtLeastOnce => InflightMessage {
                    state: RwLock::new(InflightMessageState::PubAck(Instant::now())),
                    packet_identifier,
                    msg,
                },
                Qos::ExactlyOnce => InflightMessage {
                    state: RwLock::new(InflightMessageState::PubRec(Instant::now())),
                    packet_identifier,
                    msg,
                },
            });

            _inflight.msg.write_to_buf(&mut *write_buf);

            if self
                .inflight_ch
                .send((packet_identifier, _inflight.clone()))
                .await
                .is_err()
            {
                return Err(self.handle_recv_error().await);
            }
            inflight = Some(_inflight);
        } else {
            msg.write_to_buf(&mut *write_buf);
        }
        self.flush(&mut write_buf).await?;
        Ok(inflight)
    }

    pub fn online(&self) -> bool {
        *self.online.read().unwrap()
    }

    async fn kill_bg_thread(&self) {
        let mut killer = self.kill_bg_thread.lock().await;
        *killer = true;
    }

    pub async fn disconnect(self) -> Result<(), ClientError> {
        use crate::connection::async_stream::AsyncWriter;

        let mut write_buf = self.write_buf.lock().await;
        self.assert_online();
        tracing::debug!("Sending Disconnect");
        self.kill_bg_thread().await;

        Self::disconnect_packet().write_to_buf(&mut *write_buf);
        self.flush(&mut write_buf).await?;
        let mut stream = self.writer.lock().await;
        let old_stream = std::mem::replace(&mut *stream, AsyncWriter::Disconnected);
        drop(stream);
        if let AsyncWriter::Tcp(mut tcp) = old_stream {
            // Unblocks the background thread's in-progress socket read (which can otherwise
            // block for up to `keep_alive` seconds) so it notices `kill_bg_thread` promptly.

            use tokio::io::AsyncWriteExt;
            tcp.shutdown().await?;
        }

        let mut backend = self.backend.lock().await;
        let backend = backend.take();
        if let Some(backend) = backend {
            backend.await.unwrap().unwrap();
        }
        Ok(())
    }
}

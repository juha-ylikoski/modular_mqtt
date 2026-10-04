use std::{
    future::Future,
    io::{Read, Write},
    sync::Arc,
    time::Duration,
};

use bytes::{Bytes, BytesMut};
use modular_mqtt::{
    error::{ClientError, ConnectError},
    util::{InflightMessage, IntoTopicSubscription},
    ClientOpts, ClientOptsV3, ClientOptsV5, SyncClient,
};
use modular_mqtt_protocol::{
    FixedHeader, MqttLastWill, MqttLastWill3_1_1, MqttTopic, MqttVersion, Publish, Qos,
    QosPacketIdentifier, SubAck,
};

/// Socket buffer size used when reading packets. Only affects how many packets can be
/// picked up per read, never correctness.
const CHUNK: usize = 4096;

#[derive(Default)]
pub struct GenericClientOpts(pub ClientOptsV5);

impl From<GenericClientOpts> for ClientOptsV5 {
    fn from(value: GenericClientOpts) -> Self {
        value.0
    }
}

impl From<GenericClientOpts> for ClientOptsV3 {
    fn from(value: GenericClientOpts) -> Self {
        ClientOptsV3 {
            client_id: value.0.client_id,
            keep_alive: value.0.keep_alive,
            clean_session: value.0.clean_session,
            will: value.0.will.map(|w| {
                MqttLastWill3_1_1::new(
                    MqttTopic::try_from(w.topic()).unwrap(),
                    w.payload(),
                    w.qos(),
                    w.retain(),
                )
            }),
            username: value.0.username,
            password: value.0.password,
            on_disconnect: value.0.on_disconnect,
            max_packet_size: value.0.max_packet_size,
            resend_interval: value.0.resend_interval,
            ack_retention: value.0.ack_retention,
        }
    }
}

#[allow(unused)]
pub enum JoinHandle<T> {
    Sync(std::thread::JoinHandle<T>),
    #[cfg(feature = "async")]
    Async(tokio::task::JoinHandle<T>),
}

#[allow(unused)]
impl<T> JoinHandle<T> {
    pub async fn join(self) -> T {
        match self {
            JoinHandle::Sync(join_handle) => join_handle.join().unwrap(),
            #[cfg(feature = "async")]
            JoinHandle::Async(join_handle) => join_handle.await.unwrap(),
        }
    }
}

#[allow(unused)]
pub enum ChannelRx<T> {
    Sync(std::sync::mpsc::Receiver<T>),
    #[cfg(feature = "async")]
    Async(tokio::sync::mpsc::Receiver<T>),
}

#[allow(unused)]
pub enum ChannelTx<T> {
    Sync(std::sync::mpsc::Sender<T>),
    #[cfg(feature = "async")]
    Async(tokio::sync::mpsc::Sender<T>),
}

#[allow(unused)]
impl<T> ChannelRx<T> {
    pub async fn recv(&mut self) -> T {
        match self {
            ChannelRx::Sync(receiver) => receiver.recv().unwrap(),
            #[cfg(feature = "async")]
            ChannelRx::Async(receiver) => receiver.recv().await.unwrap(),
        }
    }
}

#[allow(unused)]
impl<T> ChannelTx<T> {
    pub async fn send(&self, v: T) {
        match self {
            ChannelTx::Sync(sender) => sender.send(v).unwrap(),
            #[cfg(feature = "async")]
            ChannelTx::Async(sender) => sender.send(v).await.unwrap(),
        }
    }
}

/// A fake broker listening on a loopback port.
///
/// Which listener backs it depends on the harness: blocking `std::net` for `SyncHarness`,
/// tokio for `AsyncHarness`. Test cases are generic over `Harness`, so they name this type
/// and never either concrete one.
#[allow(unused)]
pub enum Broker {
    Sync(std::net::TcpListener),
    #[cfg(feature = "async")]
    Async(tokio::net::TcpListener),
}

#[allow(unused)]
impl Broker {
    /// The address a client should connect to.
    pub fn addr(&self) -> String {
        match self {
            Broker::Sync(listener) => listener.local_addr().unwrap().to_string(),
            #[cfg(feature = "async")]
            Broker::Async(listener) => listener.local_addr().unwrap().to_string(),
        }
    }

    /// Accept the client's connection. Blocks until it arrives.
    pub async fn accept(&mut self) -> Stream {
        match self {
            Broker::Sync(listener) => {
                let (stream, _) = listener.accept().unwrap();
                Stream::Sync {
                    stream,
                    buf: BytesMut::with_capacity(CHUNK),
                }
            }
            #[cfg(feature = "async")]
            Broker::Async(listener) => {
                let (stream, _) = listener.accept().await.unwrap();
                Stream::Async {
                    stream,
                    buf: BytesMut::with_capacity(CHUNK),
                }
            }
        }
    }
}

/// One accepted broker connection, with MQTT-packet-oriented reads and writes.
///
/// Incoming bytes are buffered, so a packet split across several segments is reassembled and
/// several packets arriving in one segment are returned one at a time — the framing a real
/// broker has to do, and something a test broker must not get wrong.
#[allow(unused)]
pub enum Stream {
    Sync {
        stream: std::net::TcpStream,
        buf: BytesMut,
    },
    #[cfg(feature = "async")]
    Async {
        stream: tokio::net::TcpStream,
        buf: BytesMut,
    },
}

/// Take one complete packet out of `buf`, or `None` if more bytes are needed.
fn take_packet(buf: &mut BytesMut) -> Option<(FixedHeader, Bytes)> {
    match FixedHeader::parse(buf, modular_mqtt_protocol::MAX_MQTT_PACKET_SIZE) {
        Ok(Some(packet)) => Some(packet),
        Ok(None) => None,
        Err(e) => panic!("Malformed packet from client: {e:?}. Buffered: {buf:?}"),
    }
}

fn extend(buf: &mut BytesMut, chunk: &[u8]) {
    buf.extend_from_slice(chunk);
}

#[allow(unused)]
impl Stream {
    /// Read the next packet, waiting as long as it takes.
    ///
    /// # Panics
    /// If the client closes the connection before a complete packet arrives, or sends a
    /// packet that does not parse.
    pub async fn read_packet(&mut self) -> (FixedHeader, Bytes) {
        match self {
            Stream::Sync { stream, buf } => {
                // A previous `read_packet_timeout` may have left a timeout on the socket.
                stream.set_read_timeout(None).unwrap();
                loop {
                    if let Some(packet) = take_packet(buf) {
                        return packet;
                    }
                    let mut chunk = [0u8; CHUNK];
                    let n = stream.read(&mut chunk).unwrap();
                    assert!(n > 0, "connection closed while waiting for a packet");
                    extend(buf, &chunk[..n]);
                }
            }
            #[cfg(feature = "async")]
            Stream::Async { stream, buf } => {
                use tokio::io::AsyncReadExt;
                loop {
                    if let Some(packet) = take_packet(buf) {
                        return packet;
                    }
                    let mut chunk = [0u8; CHUNK];
                    let n = stream.read(&mut chunk).await.unwrap();
                    assert!(n > 0, "connection closed while waiting for a packet");
                    extend(buf, &chunk[..n]);
                }
            }
        }
    }

    /// Read the next packet if one arrives within `timeout`.
    ///
    /// Returns `None` on timeout, and also when the client closed the connection — this is
    /// how a test asserts that something did *not* arrive.
    pub async fn read_packet_timeout(&mut self, timeout: Duration) -> Option<(FixedHeader, Bytes)> {
        match self {
            Stream::Sync { stream, buf } => {
                stream.set_read_timeout(Some(timeout)).unwrap();
                loop {
                    if let Some(packet) = take_packet(buf) {
                        return Some(packet);
                    }
                    let mut chunk = [0u8; CHUNK];
                    match stream.read(&mut chunk) {
                        Ok(0) | Err(_) => return None,
                        Ok(n) => extend(buf, &chunk[..n]),
                    }
                }
            }
            #[cfg(feature = "async")]
            Stream::Async { stream, buf } => {
                use tokio::io::AsyncReadExt;
                loop {
                    if let Some(packet) = take_packet(buf) {
                        return Some(packet);
                    }
                    let mut chunk = [0u8; CHUNK];
                    match tokio::time::timeout(timeout, stream.read(&mut chunk)).await {
                        Ok(Ok(0)) | Ok(Err(_)) | Err(_) => return None,
                        Ok(Ok(n)) => extend(buf, &chunk[..n]),
                    }
                }
            }
        }
    }

    /// Encode one packet with `f` and send it.
    pub async fn write_packet(&mut self, f: impl FnOnce(&mut BytesMut)) {
        let mut buf = BytesMut::with_capacity(CHUNK);
        f(&mut buf);
        match self {
            Stream::Sync { stream, .. } => stream.write_all(&buf).unwrap(),
            #[cfg(feature = "async")]
            Stream::Async { stream, .. } => {
                use tokio::io::AsyncWriteExt;
                stream.write_all(&buf).await.unwrap()
            }
        }
    }
}

#[allow(unused)]
pub trait Harness {
    type Client<V: MqttVersion, O: ClientOpts<V>>;
    type Receiver<V: MqttVersion>;
    type Notify: modular_mqtt::util::Notify;

    /// Bind a fake broker on a random loopback port.
    async fn broker() -> Broker;

    fn channel<T: Send + 'static>() -> (ChannelTx<T>, ChannelRx<T>);

    /// Run `fut` somewhere it can block without stalling the test: a dedicated thread for
    /// `SyncHarness`, a runtime task for `AsyncHarness`.
    fn spawn<T: Send + 'static>(fut: impl Future<Output = T> + Send + 'static) -> JoinHandle<T>;

    async fn sleep(d: Duration);

    async fn connect<V, O>(
        opts: GenericClientOpts,
        addr: String,
    ) -> Result<(Self::Receiver<V>, Self::Client<V, O>), ConnectError>
    where
        V: MqttVersion,
        O: ClientOpts<V> + From<GenericClientOpts>;

    fn online<V, O>(c: &Self::Client<V, O>) -> bool
    where
        V: MqttVersion,
        O: ClientOpts<V>;

    async fn publish<V, O>(
        c: &Self::Client<V, O>,
        msg: Publish<V, Qos>,
    ) -> Result<Option<Arc<InflightMessage<V, Self::Notify>>>, ClientError>
    where
        V: MqttVersion,
        O: ClientOpts<V>;

    async fn subscribe<V, O, T>(
        c: &Self::Client<V, O>,
        topics: Vec<T>,
        qos: Qos,
        timeout: Duration,
    ) -> Result<SubAck<V>, ClientError>
    where
        V: MqttVersion,
        O: ClientOpts<V>,
        T: IntoTopicSubscription<V>;

    async fn disconnect<V, O>(c: Self::Client<V, O>) -> Result<(), ClientError>
    where
        V: MqttVersion,
        O: ClientOpts<V>;

    async fn recv<V>(r: &mut Self::Receiver<V>) -> Publish<V, QosPacketIdentifier>
    where
        V: MqttVersion;

    async fn wait_until_delivered<V>(r: Arc<InflightMessage<V, Self::Notify>>)
    where
        V: MqttVersion;
}

#[allow(unused)]
pub struct SyncHarness;
#[allow(unused)]
#[cfg(feature = "async")]
pub struct AsyncHarness;

impl Harness for SyncHarness {
    type Client<V: MqttVersion, O: ClientOpts<V>> = SyncClient<V, O>;
    type Receiver<V: MqttVersion> = std::sync::mpsc::Receiver<Publish<V, QosPacketIdentifier>>;
    type Notify = modular_mqtt::util::Sync;

    async fn broker() -> Broker {
        Broker::Sync(std::net::TcpListener::bind("127.0.0.1:0").unwrap())
    }

    fn channel<T: Send + 'static>() -> (ChannelTx<T>, ChannelRx<T>) {
        let (tx, rx) = std::sync::mpsc::channel();
        (ChannelTx::Sync(tx), ChannelRx::Sync(rx))
    }

    fn spawn<T: Send + 'static>(fut: impl Future<Output = T> + Send + 'static) -> JoinHandle<T> {
        JoinHandle::Sync(std::thread::spawn(|| futures::executor::block_on(fut)))
    }

    async fn sleep(d: Duration) {
        std::thread::sleep(d);
    }

    async fn connect<V, O>(
        opts: GenericClientOpts,
        addr: String,
    ) -> Result<(Self::Receiver<V>, Self::Client<V, O>), ConnectError>
    where
        V: MqttVersion,
        O: ClientOpts<V> + From<GenericClientOpts>,
    {
        SyncClient::connect(O::from(opts), addr)
    }

    fn online<V, O>(c: &Self::Client<V, O>) -> bool
    where
        V: MqttVersion,
        O: ClientOpts<V>,
    {
        c.online()
    }

    async fn publish<V, O>(
        c: &Self::Client<V, O>,
        msg: Publish<V, Qos>,
    ) -> Result<Option<Arc<InflightMessage<V, Self::Notify>>>, ClientError>
    where
        V: MqttVersion,
        O: ClientOpts<V>,
    {
        c.publish(msg)
    }

    async fn subscribe<V, O, T>(
        c: &Self::Client<V, O>,
        topics: Vec<T>,
        qos: Qos,
        timeout: Duration,
    ) -> Result<SubAck<V>, ClientError>
    where
        V: MqttVersion,
        O: ClientOpts<V>,
        T: IntoTopicSubscription<V>,
    {
        c.subscribe(topics, qos, timeout)
    }

    async fn disconnect<V, O>(c: Self::Client<V, O>) -> Result<(), ClientError>
    where
        V: MqttVersion,
        O: ClientOpts<V>,
    {
        c.disconnect()
    }

    async fn recv<V>(r: &mut Self::Receiver<V>) -> Publish<V, QosPacketIdentifier>
    where
        V: MqttVersion,
    {
        r.recv().unwrap()
    }

    async fn wait_until_delivered<V>(r: Arc<InflightMessage<V, Self::Notify>>)
    where
        V: MqttVersion,
    {
        r.wait_until_delivered();
    }
}

#[cfg(feature = "async")]
impl Harness for AsyncHarness {
    type Client<V: MqttVersion, O: ClientOpts<V>> = modular_mqtt::AsyncClient<V, O>;
    type Receiver<V: MqttVersion> = tokio::sync::mpsc::Receiver<Publish<V, QosPacketIdentifier>>;
    type Notify = modular_mqtt::util::Async;

    async fn broker() -> Broker {
        Broker::Async(tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap())
    }

    fn channel<T: Send + 'static>() -> (ChannelTx<T>, ChannelRx<T>) {
        let (tx, rx) = tokio::sync::mpsc::channel(10);
        (ChannelTx::Async(tx), ChannelRx::Async(rx))
    }

    fn spawn<T: Send + 'static>(fut: impl Future<Output = T> + Send + 'static) -> JoinHandle<T> {
        // The broker runs on the runtime, so nothing in it may block: `Broker` and `Stream`
        // use tokio sockets for this harness.
        JoinHandle::Async(tokio::task::spawn(fut))
    }

    async fn sleep(d: Duration) {
        tokio::time::sleep(d).await
    }

    async fn connect<V, O>(
        opts: GenericClientOpts,
        addr: String,
    ) -> Result<(Self::Receiver<V>, Self::Client<V, O>), ConnectError>
    where
        V: MqttVersion,
        O: ClientOpts<V> + From<GenericClientOpts>,
    {
        modular_mqtt::AsyncClient::connect(O::from(opts), addr).await
    }

    fn online<V, O>(c: &Self::Client<V, O>) -> bool
    where
        V: MqttVersion,
        O: ClientOpts<V>,
    {
        c.online()
    }

    async fn publish<V, O>(
        c: &Self::Client<V, O>,
        msg: Publish<V, Qos>,
    ) -> Result<Option<Arc<InflightMessage<V, Self::Notify>>>, ClientError>
    where
        V: MqttVersion,
        O: ClientOpts<V>,
    {
        c.publish(msg).await
    }

    async fn subscribe<V, O, T>(
        c: &Self::Client<V, O>,
        topics: Vec<T>,
        qos: Qos,
        timeout: Duration,
    ) -> Result<SubAck<V>, ClientError>
    where
        V: MqttVersion,
        O: ClientOpts<V>,
        T: IntoTopicSubscription<V>,
    {
        match tokio::time::timeout(timeout, c.subscribe(topics, qos)).await {
            Ok(v) => v,
            Err(_) => Err(ClientError::Timeout),
        }
    }

    async fn disconnect<V, O>(c: Self::Client<V, O>) -> Result<(), ClientError>
    where
        V: MqttVersion,
        O: ClientOpts<V>,
    {
        c.disconnect().await
    }

    async fn recv<V>(r: &mut Self::Receiver<V>) -> Publish<V, QosPacketIdentifier>
    where
        V: MqttVersion,
    {
        r.recv().await.unwrap()
    }

    async fn wait_until_delivered<V>(r: Arc<InflightMessage<V, Self::Notify>>)
    where
        V: MqttVersion,
    {
        r.wait_until_delivered().await;
    }
}

pub fn init_logging() {
    static START: std::sync::Once = std::sync::Once::new();
    START.call_once(|| {
        let collector = tracing_subscriber::fmt::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_file(true)
            .with_line_number(true)
            .without_time()
            .with_target(false)
            .finish();
        tracing::dispatcher::set_global_default(collector.into()).unwrap();
    });
}

use std::{
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
pub trait Harness {
    type Client<V: MqttVersion, O: ClientOpts<V>>;
    type Receiver<V: MqttVersion>;
    type Notify: modular_mqtt::util::Notify;

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

#[allow(unused)]
pub fn read_packet<R: Read>(reader: &mut R) -> (FixedHeader, Bytes) {
    let mut buf = BytesMut::zeroed(4096);
    let mut len = 0;
    loop {
        let n = reader.read(&mut buf[len..]).unwrap();
        assert!(n > 0, "connection closed while waiting for a packet");
        len += n;
        buf.truncate(len);
        if let Some((header, body)) =
            FixedHeader::parse(&mut buf, modular_mqtt_protocol::MAX_MQTT_PACKET_SIZE).unwrap()
        {
            return (header, body);
        }
        buf.resize(len + 4096, 0);
    }
}

#[allow(unused)]
pub fn write_packet<W: Write>(writer: &mut W, fun: impl FnOnce(&mut BytesMut)) {
    let mut buf = BytesMut::with_capacity(4096);
    fun(&mut buf);
    buf.truncate(buf.len());
    let buf = buf.split();
    writer.write_all(&buf[..]).unwrap();
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

use std::io::{Read, Write};
#[cfg(feature = "async")]
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use modular_mqtt_protocol::{
    ConnAck, ControlPacketType, FixedHeader, MqttV3_1_1, MqttV5_0_0, MqttVersion, Packet, Publish,
    Qos, QosPacketIdentifier, RetainHandling, TopicSubscription, TopicSubscriptionV3,
    TopicSubscriptionV5,
};

use crate::{
    client_opts::ClientOpts,
    connection::{SyncReader, SyncWriter},
    error::ConnectError,
    Instant,
};

pub const STREAM_READ_CHUNK_SIZE: usize = 4096;

#[derive(Debug, Clone, PartialEq)]
pub enum InflightMessageState {
    PubAck(Instant),
    PubRec(Instant),
    PubComp(Instant),
    Sent,
}

#[derive(Debug)]
pub struct Sync;
#[cfg(feature = "async")]
#[derive(Debug)]
pub struct Async;

pub trait Notify: std::fmt::Debug {
    type Notifier: std::fmt::Debug;
}

impl Notify for Sync {
    type Notifier = std::sync::Condvar;
}

#[cfg(feature = "async")]
impl Notify for Async {
    type Notifier = tokio::sync::Notify;
}

#[derive(Debug)]
pub struct InflightMessage<V: MqttVersion, R: Notify> {
    pub state: std::sync::Mutex<InflightMessageState>,
    pub delivered: R::Notifier,
    pub packet_identifier: u16,
    pub msg: Publish<V, QosPacketIdentifier>,
}

impl<V: MqttVersion, R: Notify> InflightMessage<V, R> {
    pub fn packet_identifier(&self) -> u16 {
        self.packet_identifier
    }
}

impl<V: MqttVersion> InflightMessage<V, Sync> {
    pub fn wait_until_delivered(&self) {
        let mut state = self.state.lock().unwrap();
        while InflightMessageState::Sent != *state {
            state = self.delivered.wait(state).unwrap();
        }
    }
    pub fn mark_delivered(&self) {
        *self.state.lock().unwrap() = InflightMessageState::Sent;
        self.delivered.notify_all();
    }
}

#[cfg(feature = "async")]
impl<V: MqttVersion> InflightMessage<V, Async> {
    pub async fn wait_until_delivered(&self) {
        // register interest *before* checking, so a mark_delivered between
        // the check and the await is not lost
        let notified = self.delivered.notified();
        tokio::pin!(notified);
        notified.as_mut().enable(); // registers with the Notify now

        if InflightMessageState::Sent == *self.state.lock().unwrap() {
            return;
        }
        notified.await;
    }
    pub fn mark_delivered(&self) {
        self.delivered.notify_waiters();
    }
}

pub trait IntoTopicSubscription<V: MqttVersion> {
    fn into_topic_subscription(
        self,
        qos: Qos,
        no_local: bool,
        keep_retain: bool,
        retain_handling: RetainHandling,
    ) -> V::TopicSubscription;
}

impl IntoTopicSubscription<MqttV3_1_1> for String {
    fn into_topic_subscription(
        self,
        qos: Qos,
        _no_local: bool,
        _keep_retain: bool,
        _retain_handling: RetainHandling,
    ) -> TopicSubscriptionV3 {
        TopicSubscriptionV3::new(self, qos)
    }
}
impl IntoTopicSubscription<MqttV5_0_0> for String {
    fn into_topic_subscription(
        self,
        qos: Qos,
        no_local: bool,
        keep_retain: bool,
        retain_handling: RetainHandling,
    ) -> TopicSubscriptionV5 {
        TopicSubscriptionV5::new(self, qos, no_local, keep_retain, retain_handling)
    }
}

impl IntoTopicSubscription<MqttV3_1_1> for &str {
    fn into_topic_subscription(
        self,
        qos: Qos,
        _no_local: bool,
        _keep_retain: bool,
        _retain_handling: RetainHandling,
    ) -> TopicSubscriptionV3 {
        TopicSubscriptionV3::new(self.to_string(), qos)
    }
}
impl IntoTopicSubscription<MqttV5_0_0> for &str {
    fn into_topic_subscription(
        self,
        qos: Qos,
        no_local: bool,
        keep_retain: bool,
        retain_handling: RetainHandling,
    ) -> TopicSubscriptionV5 {
        TopicSubscriptionV5::new(
            self.to_string(),
            qos,
            no_local,
            keep_retain,
            retain_handling,
        )
    }
}

pub fn read_into_buf(reader: &mut impl Read, buf: &mut BytesMut) -> std::io::Result<usize> {
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
pub async fn read_into_buf_async(
    reader: &mut crate::connection::async_stream::AsyncReader,
    buf: &mut BytesMut,
    timeout: tokio::time::Instant,
) -> Result<std::io::Result<usize>, tokio::time::error::Elapsed> {
    use tokio::io::AsyncReadExt;

    tracing::trace!("Try to read data from stream. Timeout={timeout:?}");
    let old_len = buf.len();
    buf.resize(old_len + STREAM_READ_CHUNK_SIZE, 0);
    let result = match tokio::time::timeout_at(timeout, reader.read(&mut buf[old_len..])).await {
        Ok(r) => r,
        Err(timeout) => {
            buf.truncate(old_len);
            return Err(timeout);
        }
    };
    let n = *result.as_ref().unwrap_or(&0);
    buf.truncate(old_len + n);
    tracing::trace!("Read new data: {:?}", &buf[old_len..]);
    Ok(result)
}

fn send_connect<V, O>(opts: &O, buf: &mut BytesMut)
where
    V: MqttVersion,
    O: ClientOpts<V>,
{
    let msg = opts.connect_msg();
    tracing::trace!("Sending Connect: {msg:?}");
    msg.write_to_buf(buf);
}
fn handle_connack<V>(header: FixedHeader, body: &mut Bytes) -> Result<ConnAck<V>, ConnectError>
where
    V: MqttVersion,
{
    tracing::trace!("Got connack header={header:?} body={body:?}");

    match ConnAck::try_read_entire_buf(header, body) {
        Ok(connack) => Ok(connack),
        Err(e) => {
            tracing::trace!("Invalid body for ConnAck: {body:?}. Got error: {e:?}");
            Err(ConnectError::MqttError(e))
        }
    }
}

pub fn connect_sync<V, O>(
    opts: &O,
    read_buf: &mut BytesMut,
    write_buf: &mut BytesMut,
    reader: &mut SyncReader,
    writer: &mut SyncWriter,
) -> Result<ConnAck<V>, ConnectError>
where
    V: MqttVersion,
    O: ClientOpts<V>,
{
    send_connect(opts, write_buf);
    {
        tracing::trace!("Send data: {write_buf:?}");
        writer.write_all(&write_buf[..])?;
        write_buf.truncate(0);
        writer.flush()?;
    }

    let (header, mut body) = loop {
        read_into_buf(reader, read_buf)?;
        match FixedHeader::parse(read_buf, opts.max_packet_size()) {
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
    let connack = handle_connack(header, &mut body)?;

    tracing::debug!("Got ConnAck: {connack:?}");
    Ok(connack)
}

#[cfg(feature = "async")]
pub async fn connect_async<V, O>(
    opts: &O,
    read_buf: &mut BytesMut,
    write_buf: &mut BytesMut,
    reader: &mut crate::connection::async_stream::AsyncReader,
    writer: &mut crate::connection::async_stream::AsyncWriter,
) -> Result<ConnAck<V>, ConnectError>
where
    V: MqttVersion,
    O: ClientOpts<V>,
{
    send_connect(opts, write_buf);
    {
        use tokio::io::AsyncWriteExt;

        tracing::trace!("Send data: {write_buf:?}");
        writer.write_all(&write_buf[..]).await?;
        write_buf.truncate(0);
        writer.flush().await?;
    }

    let (header, mut body) = loop {
        match read_into_buf_async(
            reader,
            read_buf,
            tokio::time::Instant::now() + Duration::from_secs(opts.keep_alive().into()),
        )
        .await
        {
            Ok(Ok(_)) => (),
            Ok(Err(e)) => Err(e)?,
            Err(_) => Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "Timed out waiting for connack",
            ))?,
        };
        match FixedHeader::parse(read_buf, opts.max_packet_size()) {
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
    let connack = handle_connack(header, &mut body)?;

    tracing::debug!("Got ConnAck: {connack:?}");
    Ok(connack)
}

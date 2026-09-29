use std::{
    io::{Read, Write},
    time::Duration,
};

use bytes::{Bytes, BytesMut};
use modular_mqtt::{error::ClientError, ClientOptsV3, ClientOptsV5, SyncClient};
use modular_mqtt_protocol::{
    FixedHeader, MqttLastWill, MqttLastWill3_1_1, MqttTopic, MqttV3_1_1, MqttV5_0_0, MqttVersion,
    Qos, SubAck,
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

pub trait CommonOperations<V: MqttVersion> {
    fn disconnect(self) -> Result<(), ClientError>;
    fn subscribe(
        &self,
        topics: Vec<&str>,
        qos: Qos,
        timeout: Duration,
    ) -> Result<SubAck<V>, ClientError>;
}

impl CommonOperations<MqttV3_1_1> for SyncClient<MqttV3_1_1, ClientOptsV3> {
    fn disconnect(self) -> Result<(), ClientError> {
        self.disconnect()
    }

    fn subscribe(
        &self,
        topics: Vec<&str>,
        qos: Qos,
        timeout: Duration,
    ) -> Result<SubAck<MqttV3_1_1>, ClientError> {
        self.subscribe(topics, qos, timeout)
    }
}

impl CommonOperations<MqttV5_0_0> for SyncClient<MqttV5_0_0, ClientOptsV5> {
    fn disconnect(self) -> Result<(), ClientError> {
        self.disconnect()
    }

    fn subscribe(
        &self,
        topics: Vec<&str>,
        qos: Qos,
        timeout: Duration,
    ) -> Result<SubAck<MqttV5_0_0>, ClientError> {
        self.subscribe(topics, qos, timeout)
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

use std::io::{Read, Write};

use bytes::{Bytes, BytesMut};
use rust_mqtt_protocol::FixedHeader;

#[allow(unused)]
pub fn read_packet<R: Read>(reader: &mut R) -> (FixedHeader, Bytes) {
    let mut buf = BytesMut::zeroed(4096);
    let len = reader.read(&mut buf[..]).unwrap();
    buf.truncate(len);
    let (header, body) = FixedHeader::parse(&mut buf, rust_mqtt_protocol::MAX_MQTT_PACKET_SIZE)
        .unwrap()
        .unwrap();
    (header, body)
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

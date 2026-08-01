use std::io::{Read, Write};

use bytes::{Bytes, BytesMut};
use modular_mqtt_protocol::FixedHeader;

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

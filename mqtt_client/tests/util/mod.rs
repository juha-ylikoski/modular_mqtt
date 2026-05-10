use std::io::Read;

use mqtt_client::util::buf_with_size;
use rust_mqtt_protocol::FixedHeader;

pub fn read_packet<R: Read>(reader: &mut R) -> (FixedHeader, Vec<u8>) {
    let header = FixedHeader::try_read_sync(reader).unwrap();
    let mut buf = buf_with_size(header.remaining_length);
    reader.read_exact(&mut buf).unwrap();
    (header, buf)
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

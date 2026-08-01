#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use modular_mqtt_protocol::{FixedHeader, MAX_MQTT_PACKET_SIZE};

fuzz_target!(|data: &[u8]| {
    let Some((max_bytes, rest)) = data.split_first_chunk::<4>() else {
        return;
    };
    // Vary max_packet_size to exercise the PacketTooLarge boundary as well.
    let max_packet_size = u32::from_le_bytes(*max_bytes) as usize % (MAX_MQTT_PACKET_SIZE + 1);

    let mut buf = BytesMut::from(rest);
    match FixedHeader::parse(&mut buf, max_packet_size) {
        Ok(Some((header, body))) => {
            assert_eq!(body.len(), header.remaining_length);

            let mut encoded = BytesMut::new();
            header.write_to_buf(&mut encoded);
            encoded.extend_from_slice(&body);

            let (header2, body2) = FixedHeader::parse(&mut encoded, MAX_MQTT_PACKET_SIZE)
                .expect("re-parsing an encoded fixed header must not error")
                .expect("an encoded fixed header must be complete");
            assert_eq!(header, header2);
            assert_eq!(body, body2);
        }
        Ok(None) | Err(_) => {}
    }
});

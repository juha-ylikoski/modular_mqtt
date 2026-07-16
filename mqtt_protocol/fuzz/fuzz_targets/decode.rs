#![no_main]

use bytes::{Bytes, BytesMut};
use libfuzzer_sys::fuzz_target;
use rust_mqtt_protocol::{
    Auth, ConnAck, ControlPacketType, Disconnect, FixedHeader, MqttV3_1_1, MqttV5_0_0, Packet,
    PingReq, PingResp, PubAck, PubComp, PubRec, PubRel, Publish, QosPacketIdentifier, SubAck,
    Subscribe, UnsubAck, Unsubscribe, VersionedConnect, MAX_MQTT_PACKET_SIZE,
};

fn clone_header(header: &FixedHeader) -> FixedHeader {
    FixedHeader::new(header.control_packet_type, header.remaining_length)
}

/// Decode `body` as `P`; on success re-encode, re-parse and re-decode, and
/// require the result to equal the original packet.
fn round_trip<P: Packet + PartialEq + std::fmt::Debug>(header: FixedHeader, body: &Bytes) {
    let mut data = body.clone();
    let Ok(packet) = P::try_read_entire_buf(header, &mut data) else {
        return;
    };

    let mut encoded = BytesMut::new();
    packet.write_to_buf(&mut encoded);

    let (header2, body2) = FixedHeader::parse(&mut encoded, MAX_MQTT_PACKET_SIZE)
        .expect("re-parsing an encoded packet must not error")
        .expect("an encoded packet must be complete");
    assert!(
        encoded.is_empty(),
        "encoder produced bytes beyond the declared remaining length"
    );

    let mut data2 = body2;
    let packet2 =
        P::try_read_entire_buf(header2, &mut data2).expect("re-decoding an encoded packet");
    assert_eq!(packet, packet2, "decode(encode(p)) != p");
}

macro_rules! both_versions {
    ($ty:ident, $header:expr, $body:expr) => {{
        round_trip::<$ty<MqttV3_1_1>>(clone_header(&$header), $body);
        round_trip::<$ty<MqttV5_0_0>>(clone_header(&$header), $body);
    }};
}

fuzz_target!(|data: &[u8]| {
    let mut buf = BytesMut::from(data);
    let Ok(Some((header, body))) = FixedHeader::parse(&mut buf, MAX_MQTT_PACKET_SIZE) else {
        return;
    };

    match header.control_packet_type {
        ControlPacketType::Connect => round_trip::<VersionedConnect>(header, &body),
        ControlPacketType::ConnAck => both_versions!(ConnAck, header, &body),
        ControlPacketType::Publish { .. } => {
            round_trip::<Publish<MqttV3_1_1, QosPacketIdentifier>>(clone_header(&header), &body);
            round_trip::<Publish<MqttV5_0_0, QosPacketIdentifier>>(clone_header(&header), &body);
        }
        ControlPacketType::PubAck => both_versions!(PubAck, header, &body),
        ControlPacketType::PubRec => both_versions!(PubRec, header, &body),
        ControlPacketType::PubRel => both_versions!(PubRel, header, &body),
        ControlPacketType::PubComp => both_versions!(PubComp, header, &body),
        ControlPacketType::Subscribe => both_versions!(Subscribe, header, &body),
        ControlPacketType::SubAck => both_versions!(SubAck, header, &body),
        ControlPacketType::Unsubscribe => both_versions!(Unsubscribe, header, &body),
        ControlPacketType::UnsubscribeAck => both_versions!(UnsubAck, header, &body),
        ControlPacketType::PingReq => round_trip::<PingReq>(header, &body),
        ControlPacketType::PingResp => round_trip::<PingResp>(header, &body),
        ControlPacketType::Disconnect => both_versions!(Disconnect, header, &body),
        ControlPacketType::Auth => round_trip::<Auth<MqttV5_0_0>>(header, &body),
    }
});

#![no_main]

use bytes::{Bytes, BytesMut};
use libfuzzer_sys::fuzz_target;
use modular_mqtt_protocol::{
    Auth, ConnAck, ControlPacketType, Disconnect, FixedHeader, MqttV3_1_1, MqttV5_0_0, Packet,
    PingReq, PingResp, PubAck, PubComp, PubRec, PubRel, Publish, Qos, QosPacketIdentifier, SubAck,
    Subscribe, UnsubAck, Unsubscribe, VersionedConnect, MAX_MQTT_PACKET_SIZE,
};

fn clone_header(header: &FixedHeader) -> FixedHeader {
    FixedHeader::new(header.control_packet_type, header.remaining_length)
}

/// Read one MQTT Variable Byte Integer.
///
/// Only called for packets that already decoded successfully, so a malformed
/// value here means the harness is looking at the wrong offset and should fail
/// loudly instead of silently disabling the check.
fn read_variable_len_int(buf: &mut &[u8]) -> u64 {
    let mut value = 0u64;
    let mut multiplier = 1u64;
    for _ in 0..4 {
        let (&byte, rest) = buf.split_first().expect("truncated Property Length");
        *buf = rest;
        value += u64::from(byte & 0x7f) * multiplier;
        if byte & 0x80 == 0 {
            return value;
        }
        multiplier *= 128;
    }
    panic!("invalid Property Length");
}

/// The v5 Property Length declared by a packet that decoded successfully.
///
/// Short forms that omit the Property Length field (acks with Remaining Length
/// < 4, DISCONNECT < 2, AUTH with Remaining Length 0) implicitly declare 0.
fn declared_properties_len(header: &FixedHeader, body: &Bytes) -> u64 {
    let vbi_at = |offset: usize| -> u64 {
        let mut rest = body.get(offset..).unwrap_or_else(|| {
            panic!(
                "no Property Length at offset {offset} in {:?} body of length {}",
                header.control_packet_type,
                body.len()
            )
        });
        read_variable_len_int(&mut rest)
    };

    let u16_at = |offset: usize| -> usize {
        let bytes = body.get(offset..offset + 2).unwrap_or_else(|| {
            panic!(
                "no length field at offset {offset} in {:?} body of length {}",
                header.control_packet_type,
                body.len()
            )
        });
        u16::from_be_bytes([bytes[0], bytes[1]]) as usize
    };

    match header.control_packet_type {
        ControlPacketType::Connect => {
            // Protocol Name Length, Protocol Name, Protocol Level, Flags, Keep Alive.
            vbi_at(2 + u16_at(0) + 1 + 1 + 2)
        }
        ControlPacketType::ConnAck => {
            // Connect Acknowledge Flags, Connect Reason Code.
            vbi_at(2)
        }
        ControlPacketType::Publish { qos, .. } => {
            let packet_identifier_len = if qos == Qos::AtMostOnce { 0 } else { 2 };
            vbi_at(2 + u16_at(0) + packet_identifier_len)
        }
        ControlPacketType::PubAck
        | ControlPacketType::PubRec
        | ControlPacketType::PubRel
        | ControlPacketType::PubComp => {
            if header.remaining_length < 4 {
                0
            } else {
                // Packet Identifier, Reason Code.
                vbi_at(3)
            }
        }
        ControlPacketType::Subscribe
        | ControlPacketType::SubAck
        | ControlPacketType::Unsubscribe
        | ControlPacketType::UnsubscribeAck => {
            // Packet Identifier.
            vbi_at(2)
        }
        ControlPacketType::Disconnect => {
            if header.remaining_length < 2 {
                0
            } else {
                // Disconnect Reason Code.
                vbi_at(1)
            }
        }
        ControlPacketType::Auth => {
            if header.remaining_length == 0 {
                0
            } else {
                // Authenticate Reason Code.
                vbi_at(1)
            }
        }
        ControlPacketType::PingReq | ControlPacketType::PingResp => {
            panic!("PING packets have no Properties section")
        }
    }
}

fn connect_is_v5(body: &Bytes) -> bool {
    body.get(0..2)
        .map(|len| u16::from_be_bytes([len[0], len[1]]) as usize)
        .and_then(|protocol_name_len| body.get(2 + protocol_name_len))
        .map(|protocol_level| *protocol_level == 5)
        .unwrap_or(false)
}

/// Decode `body` as `P`; on success re-encode, re-parse and re-decode, and
/// require the result to equal the original packet.
///
/// For v5 packets (`check_properties`) the decoded properties must not exceed
/// the declared Property Length. An over-read decoder moves bytes from the
/// payload into the properties, so re-encoding declares a *larger* Property
/// Length even though `decode(encode(p)) == p` still holds. The check is
/// one-directional on purpose: re-encoding may legitimately produce a *shorter*
/// Properties block (dropped duplicate Subscription Identifiers, minimal VBIs),
/// but it can never legitimately produce a longer one.
fn round_trip<P: Packet + PartialEq + std::fmt::Debug>(
    header: FixedHeader,
    body: &Bytes,
    check_properties: bool,
) {
    let header_for_check = clone_header(&header);

    let mut data = body.clone();
    let Ok(packet) = P::try_read_entire_buf(header, &mut data) else {
        return;
    };

    let declared = if check_properties {
        Some(declared_properties_len(&header_for_check, body))
    } else {
        None
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

    if let Some(declared) = declared {
        let canonical = declared_properties_len(&header2, &body2);
        assert!(
            canonical <= declared,
            "decoder consumed more Property bytes than the packet declares \
             (declared {declared}, re-encoded {canonical})"
        );
    }

    let mut data2 = body2;
    let packet2 =
        P::try_read_entire_buf(header2, &mut data2).expect("re-decoding an encoded packet");
    assert_eq!(packet, packet2, "decode(encode(p)) != p");
}

macro_rules! both_versions {
    ($ty:ident, $header:expr, $body:expr) => {{
        round_trip::<$ty<MqttV3_1_1>>(clone_header(&$header), $body, false);
        round_trip::<$ty<MqttV5_0_0>>(clone_header(&$header), $body, true);
    }};
}

fuzz_target!(|data: &[u8]| {
    let mut buf = BytesMut::from(data);
    let Ok(Some((header, body))) = FixedHeader::parse(&mut buf, MAX_MQTT_PACKET_SIZE) else {
        return;
    };

    match header.control_packet_type {
        ControlPacketType::Connect => {
            let check_properties = connect_is_v5(&body);
            round_trip::<VersionedConnect>(header, &body, check_properties);
        }
        ControlPacketType::ConnAck => both_versions!(ConnAck, header, &body),
        ControlPacketType::Publish { .. } => {
            round_trip::<Publish<MqttV3_1_1, QosPacketIdentifier>>(
                clone_header(&header),
                &body,
                false,
            );
            round_trip::<Publish<MqttV5_0_0, QosPacketIdentifier>>(
                clone_header(&header),
                &body,
                true,
            );
        }
        ControlPacketType::PubAck => both_versions!(PubAck, header, &body),
        ControlPacketType::PubRec => both_versions!(PubRec, header, &body),
        ControlPacketType::PubRel => both_versions!(PubRel, header, &body),
        ControlPacketType::PubComp => both_versions!(PubComp, header, &body),
        ControlPacketType::Subscribe => both_versions!(Subscribe, header, &body),
        ControlPacketType::SubAck => both_versions!(SubAck, header, &body),
        ControlPacketType::Unsubscribe => both_versions!(Unsubscribe, header, &body),
        ControlPacketType::UnsubscribeAck => both_versions!(UnsubAck, header, &body),
        ControlPacketType::PingReq => round_trip::<PingReq>(header, &body, false),
        ControlPacketType::PingResp => round_trip::<PingResp>(header, &body, false),
        ControlPacketType::Disconnect => both_versions!(Disconnect, header, &body),
        ControlPacketType::Auth => round_trip::<Auth<MqttV5_0_0>>(header, &body, true),
    }
});

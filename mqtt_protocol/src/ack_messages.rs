use bytes::{Buf, BufMut, Bytes};

use crate::{
    util::{extract_str, read_variable_len_int, variable_len_int_size, write_variable_len_int},
    version::PacketProperties,
    ControlPacketType, Error, FixedHeader, MalformedPacket, MqttV5_0_0, MqttVersion, Packet,
    Property, PropertyIdentifier, UserProperty,
};

pub trait ReasonCode:
    TryFrom<u8, Error = Error> + std::fmt::Debug + Clone + Copy + PartialEq + Send + Sync + Default
{
    fn as_u8(&self) -> u8;
    fn can_be_omitted() -> bool;
}

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum PubAckReasonCode {
    /// The message is accepted. Publication of the QoS 1 or 2 message proceeds.
    #[default]
    Success = 0,
    /// The message is accepted but there are no subscribers. This is sent only
    /// by the Server. If the Server knows that there are no matching subscribers,
    /// it MAY use this Reason Code instead of 0x00 (Success).
    NoMatchingSubscribes = 16,
    /// The receiver does not accept the publish but either does not want to
    /// reveal the reason, or it does not match one of the other values.
    UnspecifiedError = 128,
    /// The PUBLISH is valid but the receiver is not willing to accept it.
    ImplementationSpecificError = 131,
    /// The PUBLISH is not authorized.
    NotAuthorized = 135,
    /// The Topic Name is not malformed, but is not accepted by this Client or Server.
    TopicNameInvalid = 144,
    /// The Packet Identifier is already in use. This might indicate a mismatch
    /// in the Session State between the Client and Server.
    PacketIdentifierInUse = 145,
    /// An implementation or administrative imposed limit has been exceeded.
    QuotaExceeded = 151,
    /// The payload format does not match the specified Payload Format Indicator.
    PayloadFormatInvalid = 153,
}

pub type PubRecReasonCode = PubAckReasonCode;

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum PubRelReasonCode {
    /// Message released.
    #[default]
    Success = 0,
    /// The Packet Identifier is not known.
    /// This is not an error during recovery, but at other times indicates
    /// a mismatch between the Session State on the Client and Server.
    PacketIdentifierNotFound = 146,
}

pub type PubCompReasonCode = PubRelReasonCode;

impl TryFrom<u8> for PubAckReasonCode {
    type Error = crate::Error;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Success),
            16 => Ok(Self::NoMatchingSubscribes),
            128 => Ok(Self::UnspecifiedError),
            131 => Ok(Self::ImplementationSpecificError),
            135 => Ok(Self::NotAuthorized),
            144 => Ok(Self::TopicNameInvalid),
            145 => Ok(Self::PacketIdentifierInUse),
            151 => Ok(Self::QuotaExceeded),
            153 => Ok(Self::PayloadFormatInvalid),
            _ => Err(MalformedPacket::new("Invalid PubAck/PubRec reason code")),
        }
    }
}

impl ReasonCode for PubAckReasonCode {
    fn as_u8(&self) -> u8 {
        *self as u8
    }
    fn can_be_omitted() -> bool {
        true
    }
}

impl TryFrom<u8> for PubRelReasonCode {
    type Error = crate::Error;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Success),
            146 => Ok(Self::PacketIdentifierNotFound),
            _ => Err(MalformedPacket::new("Invalid PubRel/PubComp reason code")),
        }
    }
}

impl ReasonCode for PubRelReasonCode {
    fn as_u8(&self) -> u8 {
        *self as u8
    }
    fn can_be_omitted() -> bool {
        true
    }
}

#[derive(Debug, PartialEq, Default)]
pub struct PubAckData<R: ReasonCode> {
    reason_code: R,
    /// UTF-8 Encoded String representing the reason associated with this response.
    /// This Reason String is a human readable string designed for diagnostics
    /// and is not intended to be parsed by the receiver
    reason: Option<String>,
    /// UTF-8 String Pair. This property can be used to provide additional
    /// diagnostic or other information
    user_property: Vec<UserProperty>,
}

impl<R: ReasonCode> PacketProperties for PubAckData<R> {
    fn try_read(data: &mut Bytes) -> Result<Self, Error> {
        // If fixed header remaining length == 2 -> reason code = 0x00 and everything is omitted
        // (except for unsuback for some reason)
        if R::can_be_omitted() && !data.has_remaining() {
            return Ok(Self {
                reason_code: R::try_from(0)?,
                reason: None,
                user_property: Vec::new(),
            });
        }

        let reason_code = R::try_from(data.try_get_u8()?)?;

        let len_properties = read_variable_len_int(data)? as usize;

        let mut properties = PubAckData {
            reason_code,
            reason: None,
            user_property: Vec::new(),
        };

        if data.remaining() < len_properties {
            return Err(MalformedPacket::new("Packet too short to parse"));
        }

        let data = &mut data.split_to(len_properties);

        while data.has_remaining() {
            let property_identifier = crate::util::read_variable_len_int(data)?;
            let property_identifier = PropertyIdentifier::try_from(property_identifier)?;
            match property_identifier {
                PropertyIdentifier::Reason => {
                    if properties.reason.is_some() {
                        return Err(Error::ProtocolError("Reason specified multiple times"));
                    }
                    properties.reason = Some(extract_str(data)?.to_string());
                }
                PropertyIdentifier::UserProperty => {
                    let key = extract_str(data)?.to_string();
                    let value = extract_str(data)?.to_string();
                    let property = UserProperty { key, value };
                    properties.user_property.push(property);
                }
                _ => {
                    return Err(MalformedPacket::new(
                        "Received unexpected property for connect",
                    ))
                }
            };
        }
        Ok(properties)
    }

    fn write_properties(&self, buf: &mut impl BufMut) {
        let property_len = self.properties_len();
        // If fixed header remaining length == 2 -> reason code = 0x00 + everything is omitted
        // (except for unsuback for some reason)
        if R::can_be_omitted() && self.reason_code.as_u8() == 0 && property_len == 0 {
            return;
        }
        buf.put_u8(self.reason_code.as_u8());
        write_variable_len_int(property_len as u64, buf);
        self.reason
            .serialize(crate::PropertyIdentifier::Reason, buf);
        self.user_property
            .serialize(crate::PropertyIdentifier::UserProperty, buf);
    }

    fn properties_block_len(&self) -> usize {
        let l = self.properties_len();
        if R::can_be_omitted() && l == 0 && self.reason_code.as_u8() == 0 {
            0
        } else {
            1 + variable_len_int_size(l) + l
        }
    }

    fn properties_len(&self) -> usize {
        self.reason.property_len() + self.user_property.property_len()
    }
}

#[derive(Debug, PartialEq)]
struct PubAckType<V: MqttVersion, R: ReasonCode> {
    packet_type: ControlPacketType,
    packet_identifier: u16,
    // v5
    properties: V::AckTypeProperties<R>,
}

impl<V, R: ReasonCode> PubAckType<V, R>
where
    V: MqttVersion,
{
    fn write_to_buf(&self, buf: &mut impl BufMut) {
        let remaining_length = if self.properties.properties_block_len() == 0 {
            2
        } else {
            2 + self.properties.properties_block_len()
        };
        let fixed_header = FixedHeader::new(self.packet_type, remaining_length);
        fixed_header.write_to_buf(buf);
        buf.put_u16(self.packet_identifier);
        self.properties.write_properties(buf);
    }

    fn try_read(header: FixedHeader, data: &mut Bytes) -> Result<Self, Error> {
        let packet_identifier = data.try_get_u16()?;

        let properties = V::AckTypeProperties::try_read(data)?;

        Ok(PubAckType {
            packet_type: header.control_packet_type,
            packet_identifier,
            properties,
        })
    }
}

impl<R: ReasonCode, V: MqttVersion> PubAckType<V, R>
where
    crate::Error: From<<R as TryFrom<u8>>::Error>,
{
    fn new_ok(packet_type: ControlPacketType, packet_identifier: u16) -> Self {
        PubAckType {
            packet_type,
            packet_identifier,
            properties: V::AckTypeProperties::default(),
        }
    }
}

impl<R: ReasonCode> PubAckType<MqttV5_0_0, R>
where
    crate::Error: From<<R as TryFrom<u8>>::Error>,
{
    fn new_v5(
        packet_type: ControlPacketType,
        packet_identifier: u16,
        reason_code: R,
        reason: Option<String>,
        user_property: Vec<UserProperty>,
    ) -> Self {
        let properties = PubAckData {
            reason_code,
            reason,
            user_property,
        };

        PubAckType {
            packet_type,
            packet_identifier,
            properties,
        }
    }
}

macro_rules! create_pub_ack_type {
    (#[doc = $doc:expr] $name:ident, $control_packet_type:ident, $reason_code:ty) => {
        #[derive(Debug, PartialEq)]
        #[doc = $doc]
        pub struct $name<V: MqttVersion>(PubAckType<V, $reason_code>);

        impl<V: MqttVersion> $name<V> {
            pub fn new_ok(packet_identifier: u16) -> Self {
                Self(PubAckType::new_ok(
                    ControlPacketType::$control_packet_type,
                    packet_identifier,
                ))
            }
            pub fn packet_identifier(&self) -> u16 {
                self.0.packet_identifier
            }
        }

        impl<V: MqttVersion> Packet for $name<V> {
            fn try_read(header: FixedHeader, data: &mut Bytes) -> Result<Self, crate::Error> {
                assert_eq!(
                    header.control_packet_type,
                    ControlPacketType::$control_packet_type
                );
                PubAckType::try_read(header, data).map(Self)
            }
            fn write_to_buf(&self, buf: &mut impl BufMut) {
                self.0.write_to_buf(buf)
            }
        }

        impl $name<MqttV5_0_0> {
            pub fn new_v5(
                packet_identifier: u16,
                reason_code: $reason_code,
                reason: Option<String>,
                user_property: Vec<UserProperty>,
            ) -> Self {
                Self(PubAckType::new_v5(
                    ControlPacketType::$control_packet_type,
                    packet_identifier,
                    reason_code,
                    reason,
                    user_property,
                ))
            }

            pub fn reason(&self) -> Option<&String> {
                self.0.properties.reason.as_ref()
            }
            pub fn reason_code(&self) -> $reason_code {
                self.0.properties.reason_code
            }
            pub fn user_property(&self) -> &[UserProperty] {
                &self.0.properties.user_property
            }
        }
    };
}

create_pub_ack_type!(
    /// A PUBACK Packet is the response to a PUBLISH Packet with QoS level 1.
    PubAck,
    PubAck,
    PubAckReasonCode
);
create_pub_ack_type!(
    /// A PUBREC Packet is the response to a PUBLISH Packet with QoS level 2.
    PubRec,
    PubRec,
    PubRecReasonCode
);
create_pub_ack_type!(
    /// A PUBREL Packet is the response to a PUBREC Packet. It is the third packet of the QoS 2 protocol exchange.
    PubRel,
    PubRel,
    PubRelReasonCode
);
create_pub_ack_type!(
    /// The PUBCOMP Packet is the response to a PUBREL Packet. It is the fourth and final packet of the QoS 2 protocol exchange.
    PubComp,
    PubComp,
    PubCompReasonCode
);

macro_rules! make_tests {
    ($name:ident, $test_name:ident, $test_packet_type:expr, $reason_type:ty, $reason_code:ident) => {
        #[cfg(test)]
        mod $test_name {

            use super::*;

            mod v3 {
                use super::*;

                use crate::MqttV3_1_1;

                use bytes::BytesMut;

                #[test]
                fn serialize() {
                    let mut buf = Vec::new();
                    let msg = $name::<MqttV3_1_1>::new_ok(42);
                    msg.write_to_buf(&mut buf);
                    assert_eq!(&buf, &[$test_packet_type, 2, 0, 42]);
                }
                #[test]
                fn deserialize() {
                    let msg = [$test_packet_type, 2, 0, 42];
                    let expected = $name::<MqttV3_1_1>::new_ok(42);
                    let mut reader = BytesMut::from(&msg[..]);
                    let (header, mut body) =
                        FixedHeader::parse(&mut reader, crate::MAX_MQTT_PACKET_SIZE)
                            .unwrap()
                            .unwrap();
                    assert_eq!(
                        $name::<MqttV3_1_1>::try_read(header, &mut body).unwrap(),
                        expected
                    );
                }
            }

            mod v5 {
                use super::*;

                use bytes::BytesMut;

                #[test]
                fn serialize() {
                    let mut buf = Vec::new();
                    let msg = $name::new_v5(42, <$reason_type>::$reason_code, None, Vec::new());
                    msg.write_to_buf(&mut buf);
                    assert_eq!(
                        &buf,
                        &[
                            $test_packet_type,
                            4,
                            0,
                            42,
                            <$reason_type>::$reason_code as u8,
                            0
                        ]
                    );
                }

                #[test]
                fn serialize_short() {
                    let mut buf = Vec::new();
                    let msg =
                        $name::new_v5(42, <$reason_type>::try_from(0).unwrap(), None, Vec::new());
                    msg.write_to_buf(&mut buf);

                    if <$reason_type>::can_be_omitted() {
                        assert_eq!(&buf, &[$test_packet_type, 2, 0, 42,]);
                    } else {
                        assert_eq!(&buf, &[$test_packet_type, 4, 0, 42, 0, 0]);
                    }
                }

                #[test]
                fn serialize_reason() {
                    let mut buf = Vec::new();
                    let msg = $name::new_v5(
                        42,
                        <$reason_type>::$reason_code,
                        Some("test".to_string()),
                        Vec::new(),
                    );
                    msg.write_to_buf(&mut buf);
                    assert_eq!(
                        &buf,
                        &[
                            $test_packet_type,
                            11,
                            0,
                            42,
                            <$reason_type>::$reason_code as u8,
                            7,
                            31,
                            0,
                            4,
                            b't',
                            b'e',
                            b's',
                            b't'
                        ]
                    );
                }
                #[test]
                fn serialize_user_property() {
                    let mut buf = Vec::new();
                    let msg = $name::new_v5(
                        42,
                        <$reason_type>::$reason_code,
                        None,
                        vec![UserProperty {
                            key: "key".to_string(),
                            value: "value".to_string(),
                        }],
                    );
                    msg.write_to_buf(&mut buf);
                    assert_eq!(
                        &buf,
                        &[
                            $test_packet_type,
                            17,
                            0,
                            42,
                            <$reason_type>::$reason_code as u8,
                            13,
                            38,
                            0,
                            3,
                            b'k',
                            b'e',
                            b'y',
                            0,
                            5,
                            b'v',
                            b'a',
                            b'l',
                            b'u',
                            b'e'
                        ]
                    );
                }
                #[test]
                fn deserialize() {
                    let msg = [
                        $test_packet_type,
                        4,
                        0,
                        42,
                        <$reason_type>::$reason_code as u8,
                        0,
                    ];
                    let expected =
                        $name::new_v5(42, <$reason_type>::$reason_code, None, Vec::new());
                    let mut reader = BytesMut::from(&msg[..]);
                    let (header, mut body) =
                        FixedHeader::parse(&mut reader, crate::MAX_MQTT_PACKET_SIZE)
                            .unwrap()
                            .unwrap();
                    assert_eq!(
                        $name::<MqttV5_0_0>::try_read(header, &mut body).unwrap(),
                        expected
                    );
                }

                #[test]
                fn deserialize_short() {
                    let msg = if <$reason_type>::can_be_omitted() {
                        &[$test_packet_type, 2, 0, 42][..]
                    } else {
                        &[$test_packet_type, 4, 0, 42, 0, 0][..]
                    };
                    let expected =
                        $name::new_v5(42, <$reason_type>::try_from(0).unwrap(), None, Vec::new());
                    let mut reader = BytesMut::from(&msg[..]);
                    let (header, mut body) =
                        FixedHeader::parse(&mut reader, crate::MAX_MQTT_PACKET_SIZE)
                            .unwrap()
                            .unwrap();
                    assert_eq!(
                        $name::<MqttV5_0_0>::try_read(header, &mut body).unwrap(),
                        expected
                    );
                }

                #[test]
                fn deserialize_reason() {
                    let msg = [
                        $test_packet_type,
                        11,
                        0,
                        42,
                        <$reason_type>::$reason_code as u8,
                        7,
                        31,
                        0,
                        4,
                        b't',
                        b'e',
                        b's',
                        b't',
                    ];
                    let expected = $name::new_v5(
                        42,
                        <$reason_type>::$reason_code,
                        Some("test".to_string()),
                        Vec::new(),
                    );
                    let mut reader = BytesMut::from(&msg[..]);
                    let (header, mut body) =
                        FixedHeader::parse(&mut reader, crate::MAX_MQTT_PACKET_SIZE)
                            .unwrap()
                            .unwrap();
                    assert_eq!(
                        $name::<MqttV5_0_0>::try_read(header, &mut body).unwrap(),
                        expected
                    );
                }
                #[test]
                fn deserialize_user_property() {
                    let msg = [
                        $test_packet_type,
                        17,
                        0,
                        42,
                        <$reason_type>::$reason_code as u8,
                        13,
                        38,
                        0,
                        3,
                        b'k',
                        b'e',
                        b'y',
                        0,
                        5,
                        b'v',
                        b'a',
                        b'l',
                        b'u',
                        b'e',
                    ];
                    let expected = $name::new_v5(
                        42,
                        <$reason_type>::$reason_code,
                        None,
                        vec![UserProperty {
                            key: "key".to_string(),
                            value: "value".to_string(),
                        }],
                    );
                    let mut reader = BytesMut::from(&msg[..]);
                    let (header, mut body) =
                        FixedHeader::parse(&mut reader, crate::MAX_MQTT_PACKET_SIZE)
                            .unwrap()
                            .unwrap();
                    assert_eq!($name::try_read(header, &mut body).unwrap(), expected);
                }
            }
        }
    };
}

make_tests!(
    PubAck,
    test_puback,
    64,
    PubAckReasonCode,
    NoMatchingSubscribes
);
make_tests!(PubRec, test_pubrec, 80, PubRecReasonCode, NotAuthorized);
make_tests!(
    PubRel,
    test_pubrel,
    96 | 2,
    PubRelReasonCode,
    PacketIdentifierNotFound
);
make_tests!(
    PubComp,
    test_pubcomp,
    112,
    PubCompReasonCode,
    PacketIdentifierNotFound
);

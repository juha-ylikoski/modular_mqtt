use std::marker::PhantomData;

use bytes::{Buf, BufMut, Bytes};

use crate::{
    util::{extract_str, read_variable_len_int, variable_len_int_size, write_variable_len_int},
    ControlPacketType, Error, FixedHeader, MalformedPacket, MqttV3_1_1, MqttV5_0_0, Property,
    PropertyIdentifier, UserProperty,
};

trait ReasonCode: TryFrom<u8> {
    fn as_u8(&self) -> u8;
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PubAckReasonCode {
    /// The message is accepted. Publication of the QoS 1 or 2 message proceeds.
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

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PubRelReasonCode {
    /// Message released.
    Success = 0,
    /// The Packet Identifier is not known.
    /// This is not an error during recovery, but at other times indicates
    /// a mismatch between the Session State on the Client and Server.
    PacketIdentifierNotFound = 146,
}

pub type PubCompReasonCode = PubRelReasonCode;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum UnsubAckReasonCode {
    /// The subscription is deleted.
    Success = 0,
    /// No matching Topic Filter is being used by the Client.
    NoSubscriptionExisted = 17,
    /// The unsubscribe could not be completed and the Server either does not wish to reveal the reason or none of the other Reason Codes apply.
    UnspecifiedError = 128,
    /// The UNSUBSCRIBE is valid but the Server does not accept it.
    ImplementationSpecificError = 131,
    /// The Client is not authorized to unsubscribe.
    NotAuthorized = 135,
    /// The Topic Filter is correctly formed but is not allowed for this Client.
    TopicFilterInvalid = 143,
    /// The specified Packet Identifier is already in use.
    PacketIdentifierInUse = 145,
}

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
}

impl TryFrom<u8> for UnsubAckReasonCode {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Success),
            17 => Ok(Self::NoSubscriptionExisted),
            128 => Ok(Self::UnspecifiedError),
            131 => Ok(Self::ImplementationSpecificError),
            135 => Ok(Self::NotAuthorized),
            143 => Ok(Self::TopicFilterInvalid),
            145 => Ok(Self::PacketIdentifierInUse),
            _ => Err(MalformedPacket::new("Invalid UnsubAck reason code")),
        }
    }
}
impl ReasonCode for UnsubAckReasonCode {
    fn as_u8(&self) -> u8 {
        *self as u8
    }
}

#[derive(Debug, PartialEq)]
enum PubAckData<V, R> {
    V3,
    V5 {
        procol_version: PhantomData<V>,
        reason_code: R,
        /// UTF-8 Encoded String representing the reason associated with this response.
        /// This Reason String is a human readable string designed for diagnostics
        /// and is not intended to be parsed by the receiver
        reason: Option<String>,
        /// UTF-8 String Pair. This property can be used to provide additional
        /// diagnostic or other information
        user_property: Vec<UserProperty>,
    },
}

#[derive(Debug, PartialEq)]
struct PubAckType<V, R> {
    fixed_header: FixedHeader,
    packet_identifier: u16,
    // v5
    data: PubAckData<V, R>,
}

impl<V, R: ReasonCode> PubAckType<V, R>
where
    crate::Error: From<<R as TryFrom<u8>>::Error>,
{
    fn properties_len(&self) -> usize {
        match &self.data {
            PubAckData::V3 => 0,
            PubAckData::V5 {
                reason,
                user_property,
                ..
            } => reason.property_len() + user_property.property_len(),
        }
    }
    fn write_to_buf(&self, buf: &mut impl BufMut) {
        self.fixed_header.write_to_buf(buf);
        buf.put_u16(self.packet_identifier);
        let property_len = self.properties_len();
        match &self.data {
            PubAckData::V3 => (),
            PubAckData::V5 {
                reason_code,
                reason,
                user_property,
                ..
            } => {
                buf.put_u8(reason_code.as_u8());
                write_variable_len_int(property_len as u64, buf);
                reason.serialize(crate::PropertyIdentifier::Reason, buf);
                user_property.serialize(crate::PropertyIdentifier::UserProperty, buf);
            }
        };
    }

    fn try_read_v3(header: FixedHeader, data: &mut Bytes) -> Result<Self, Error> {
        Ok(PubAckType {
            fixed_header: header,
            packet_identifier: data.try_get_u16()?,
            data: PubAckData::V3,
        })
    }

    fn new_v3(packet_type: ControlPacketType, packet_identifier: u16) -> Self {
        PubAckType {
            fixed_header: FixedHeader::new(packet_type, 2),
            packet_identifier,
            data: PubAckData::V3,
        }
    }

    fn new_v5(
        packet_type: ControlPacketType,
        packet_identifier: u16,
        reason_code: R,
        reason: Option<String>,
        user_property: Vec<UserProperty>,
    ) -> Self {
        let mut msg = PubAckType {
            fixed_header: FixedHeader::new(packet_type, 0),
            packet_identifier,
            data: PubAckData::V5 {
                procol_version: PhantomData,
                reason_code,
                reason,
                user_property,
            },
        };
        let property_len = msg.properties_len();
        let property_len_int_size = variable_len_int_size(property_len);
        let remaining_length = 2 + 1 + property_len_int_size + property_len;
        msg.fixed_header.remaining_length = remaining_length;
        msg
    }

    fn try_read_v5(header: FixedHeader, data: &mut Bytes) -> Result<Self, Error> {
        let packet_identifier = data.try_get_u16()?;
        let reason_code = R::try_from(data.try_get_u8()?)?;

        let len_properties = read_variable_len_int(data)? as usize;

        let mut reason = None;
        let mut user_property = Vec::new();

        if data.remaining() < len_properties {
            return Err(MalformedPacket::new("Packet too short to parse"));
        }

        let properties_end = data.remaining() - len_properties;

        while data.remaining() > properties_end {
            let property_identifier = crate::util::read_variable_len_int(data)?;
            let property_identifier = PropertyIdentifier::try_from(property_identifier)?;
            match property_identifier {
                PropertyIdentifier::Reason => {
                    if reason.is_some() {
                        return Err(Error::ProtocolError("Reason specified multiple times"));
                    }
                    reason = Some(extract_str(data)?.to_string());
                }
                PropertyIdentifier::UserProperty => {
                    let key = extract_str(data)?.to_string();
                    let value = extract_str(data)?.to_string();
                    let property = UserProperty { key, value };
                    user_property.push(property);
                }
                _ => {
                    return Err(MalformedPacket::new(
                        "Received unexpected property for connect",
                    ))
                }
            };
        }

        Ok(PubAckType {
            fixed_header: header,
            packet_identifier,
            data: PubAckData::V5 {
                procol_version: PhantomData,
                reason_code,
                reason,
                user_property,
            },
        })
    }
}

macro_rules! create_pub_ack_type {
    (#[doc = $doc:expr] $name:ident, $control_packet_type:ident, $reason_code:ty) => {
        #[derive(Debug, PartialEq)]
        #[doc = $doc]
        pub struct $name<V>(PubAckType<V, $reason_code>);

        impl<V> $name<V> {
            pub fn write_to_buf(&self, buf: &mut impl BufMut) {
                self.0.write_to_buf(buf)
            }
            pub fn packet_identifier(&self) -> u16 {
                self.0.packet_identifier
            }
        }

        impl $name<MqttV3_1_1> {
            pub fn new_v3(packet_identifier: u16) -> Self {
                Self(PubAckType::new_v3(
                    ControlPacketType::$control_packet_type,
                    packet_identifier,
                ))
            }
            pub fn try_read_v3(
                header: FixedHeader,
                data: &mut Bytes,
            ) -> Result<Self, crate::Error> {
                PubAckType::try_read_v3(header, data).map(Self)
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
            pub fn try_read_v5(header: FixedHeader, data: &mut Bytes) -> Result<Self, Error> {
                PubAckType::try_read_v5(header, data).map(Self)
            }

            pub fn reason(&self) -> Option<&String> {
                match &self.0.data {
                    PubAckData::V3 { .. } => unreachable!(),
                    PubAckData::V5 { reason, .. } => reason.as_ref(),
                }
            }
            pub fn reason_code(&self) -> $reason_code {
                match &self.0.data {
                    PubAckData::V3 { .. } => unreachable!(),
                    PubAckData::V5 { reason_code, .. } => *reason_code,
                }
            }
            pub fn user_property(&self) -> &[UserProperty] {
                match &self.0.data {
                    PubAckData::V3 { .. } => unreachable!(),
                    PubAckData::V5 { user_property, .. } => &user_property,
                }
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
    /// A PUBACK Packet is the response to a PUBLISH Packet with QoS level 1.
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

create_pub_ack_type!(
    /// The UNSUBACK Packet is sent by the Server to the Client to confirm receipt of an UNSUBSCRIBE Packet.
    UnsubAck,
    UnsubscribeAck,
    UnsubAckReasonCode
);

macro_rules! make_tests {
    ($name:ident, $test_name:ident, $test_packet_type:expr, $reason_type:ty, $reason_code:ident) => {
        #[cfg(test)]
        mod $test_name {

            use super::*;

            mod v3 {
                use super::*;

                use bytes::BytesMut;

                #[test]
                fn serialize() {
                    let mut buf = Vec::new();
                    let msg = $name::new_v3(42);
                    msg.write_to_buf(&mut buf);
                    assert_eq!(&buf, &[$test_packet_type, 2, 0, 42]);
                }
                #[test]
                fn deserialize() {
                    let msg = [$test_packet_type, 2, 0, 42];
                    let expected = $name::new_v3(42);
                    let mut reader = BytesMut::from(&msg[..]);
                    let (header, mut body) =
                        FixedHeader::parse(&mut reader, crate::MAX_MQTT_PACKET_SIZE)
                            .unwrap()
                            .unwrap();
                    assert_eq!($name::try_read_v3(header, &mut body).unwrap(), expected);
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
                    assert_eq!($name::try_read_v5(header, &mut body).unwrap(), expected);
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
                    assert_eq!($name::try_read_v5(header, &mut body).unwrap(), expected);
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
                    assert_eq!($name::try_read_v5(header, &mut body).unwrap(), expected);
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
make_tests!(PubRel, test_pubrel, 96 | 2, PubRelReasonCode, Success);
make_tests!(
    PubComp,
    test_pubcomp,
    112,
    PubCompReasonCode,
    PacketIdentifierNotFound
);

make_tests!(
    UnsubAck,
    test_unsuback,
    176,
    UnsubAckReasonCode,
    NotAuthorized
);

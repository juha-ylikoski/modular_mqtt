use std::{io::Write, marker::PhantomData};

use crate::{
    util::{extract_str, read_variable_len_int, variable_len_int_size, write_variable_len_int},
    Error, MalformedPacket, MqttV3_1_1, MqttV5_0_0, Property, PropertyIdentifier,
    ReceivedUserProperty,
};

use super::fixed_header::{ControlPacketType, FixedHeader};

#[derive(Debug, PartialEq)]
pub enum SubRcV3 {
    SuccessQos0 = 0,
    SuccessQos1 = 1,
    SuccessQos2 = 2,
    Failure = 0x80,
}

#[derive(Debug, PartialEq)]
pub enum SubRcV5 {
    /// The subscription is accepted and the maximum QoS sent will be QoS 0. This might be a lower QoS than was requested.
    SuccessQos0 = 0,
    /// The subscription is accepted and the maximum QoS sent will be QoS 1. This might be a lower QoS than was requested.
    SuccessQos1 = 1,
    /// The subscription is accepted and any received QoS will be sent to this subscription.
    SuccessQos2 = 2,
    /// The subscription is not accepted and the Server either does not wish to reveal the reason or none of the other Reason Codes apply.
    Failure = 0x80,
    /// The PUBLISH is valid but the receiver is not willing to accept it.
    ImplementationSpecificError = 131,
    /// The PUBLISH is not authorized.
    NotAuthorized = 135,
    /// The Topic Filter is correctly formed but is not allowed for this Client.
    TopicFilterInvalid = 143,
    /// The specified Packet Identifier is already in use.
    PacketIdentifierInUse = 145,
    /// An implementation or administrative imposed limit has been exceeded.
    QuotaExceeded = 151,
    /// The Server does not support Shared Subscriptions for this Client.
    SharedSubscriptionsNotSupported = 158,
    /// The Server does not support Subscription Identifiers; the subscription is not accepted.
    SubscriptionIdentifiersNotSupported = 161,
    /// The Server does not support Wildcard Subscriptions; the subscription is not accepted.
    WildcardSubscriptionsNotSupported = 162,
}

impl TryFrom<u8> for SubRcV5 {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(SubRcV5::SuccessQos0),
            1 => Ok(SubRcV5::SuccessQos1),
            2 => Ok(SubRcV5::SuccessQos2),
            0x80 => Ok(SubRcV5::Failure),
            131 => Ok(SubRcV5::ImplementationSpecificError),
            135 => Ok(SubRcV5::NotAuthorized),
            143 => Ok(SubRcV5::TopicFilterInvalid),
            145 => Ok(SubRcV5::PacketIdentifierInUse),
            151 => Ok(SubRcV5::QuotaExceeded),
            158 => Ok(SubRcV5::SharedSubscriptionsNotSupported),
            161 => Ok(SubRcV5::SubscriptionIdentifiersNotSupported),
            162 => Ok(SubRcV5::WildcardSubscriptionsNotSupported),
            _ => Err(MalformedPacket::new("Invalid subscribe return code")),
        }
    }
}
impl SubRcV5 {
    pub fn is_success(&self) -> bool {
        *self == SubRcV5::SuccessQos0
            || *self == SubRcV5::SuccessQos1
            || *self == SubRcV5::SuccessQos2
    }
}

impl TryFrom<u8> for SubRcV3 {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(SubRcV3::SuccessQos0),
            1 => Ok(SubRcV3::SuccessQos1),
            2 => Ok(SubRcV3::SuccessQos2),
            0x80 => Ok(SubRcV3::Failure),
            _ => Err(MalformedPacket::new("Invalid subscribe return code")),
        }
    }
}

impl SubRcV3 {
    pub fn is_success(&self) -> bool {
        *self != SubRcV3::Failure
    }
}

#[derive(Debug, PartialEq)]
pub enum SubAckData<V> {
    V3 {
        protocol_level: PhantomData<V>,
        return_codes: Vec<SubRcV3>,
    },
    V5 {
        protocol_level: PhantomData<V>,
        return_codes: Vec<SubRcV5>,
        /// UTF-8 Encoded String representing the reason associated with this response.
        /// This Reason String is a human readable string designed for diagnostics
        /// and is not intended to be parsed by the receiver
        reason: Option<String>,
        /// UTF-8 String Pair. This property can be used to provide additional
        /// diagnostic or other information
        user_property: Vec<ReceivedUserProperty>,
    },
}

#[derive(Debug, PartialEq)]
/// A SUBACK Packet is sent by the Server to the Client to confirm receipt and processing of a SUBSCRIBE Packet.
pub struct SubAck<V> {
    pub fixed_header: FixedHeader,
    pub packet_identifier: u16,
    pub data: SubAckData<V>,
}

impl<V> SubAck<V> {
    pub fn write_to_stream(self, writer: &mut impl Write) -> Result<usize, std::io::Error> {
        let mut length = self.fixed_header.write_to_stream(writer)?;
        writer.write_all(&[
            ((self.packet_identifier & 0xff00) >> 8) as u8,
            (self.packet_identifier & 0xff) as u8,
        ])?;
        length += 2;

        match self.data {
            SubAckData::V3 { return_codes, .. } => {
                for rc in return_codes {
                    writer.write_all(&[rc as u8])?;
                    length += 1;
                }
            }
            SubAckData::V5 {
                return_codes,
                reason,
                user_property,
                ..
            } => {
                let user_property: &[ReceivedUserProperty] = &user_property;
                let reason: Option<&str> = reason.as_deref();
                let property_len = user_property.property_len() + reason.property_len();
                length += write_variable_len_int(property_len as u64, writer)?;
                length += reason.serialize(PropertyIdentifier::Reason, writer)?
                    + user_property.serialize(PropertyIdentifier::UserProperty, writer)?;
                for rc in return_codes {
                    writer.write_all(&[rc as u8])?;
                    length += 1;
                }
            }
        }

        writer.flush()?;
        Ok(length)
    }
}
impl SubAck<MqttV3_1_1> {
    pub fn new_v3(packet_identifier: u16, return_codes: Vec<SubRcV3>) -> Self {
        Self {
            fixed_header: FixedHeader::new(ControlPacketType::SubAck, 2 + return_codes.len()),
            packet_identifier,
            data: SubAckData::V3 {
                protocol_level: PhantomData,
                return_codes,
            },
        }
    }
    pub fn try_read_v3(header: FixedHeader, data: &[u8]) -> Result<Self, Error> {
        let packet_identifier = u16::from_be_bytes([data[0], data[1]]);
        let mut remaining = header.remaining_length - 2;
        let mut index = 2;
        let mut return_codes = Vec::new();
        while remaining > 0 {
            return_codes.push(SubRcV3::try_from(data[index])?);
            index += 1;
            remaining -= 1;
        }
        Ok(Self {
            fixed_header: header,
            packet_identifier,
            data: SubAckData::V3 {
                protocol_level: PhantomData,
                return_codes,
            },
        })
    }
}

impl SubAck<MqttV5_0_0> {
    pub fn new_v5(
        packet_identifier: u16,
        return_codes: Vec<SubRcV5>,
        reason: Option<String>,
        user_property: Vec<ReceivedUserProperty>,
    ) -> Self {
        let _user_property: &[ReceivedUserProperty] = &user_property;
        let _reason: Option<&str> = reason.as_deref();
        let property_len = _user_property.property_len() + _reason.property_len();
        Self {
            fixed_header: FixedHeader::new(
                ControlPacketType::SubAck,
                2 + variable_len_int_size(property_len) + property_len + return_codes.len(),
            ),
            packet_identifier,
            data: SubAckData::V5 {
                protocol_level: PhantomData,
                return_codes,
                user_property,
                reason,
            },
        }
    }
    pub fn try_read_v5(header: FixedHeader, data: &[u8]) -> Result<Self, Error> {
        let packet_identifier = u16::from_be_bytes([data[0], data[1]]);

        let (i, len_properties) = read_variable_len_int(&data[2..])?;
        let len_properties = len_properties as usize;
        let mut index = 2 + i;
        let mut reason = None;
        let mut user_property = Vec::new();

        let mut i = 0;

        while i < len_properties {
            let (len, property_identifier) = crate::util::read_variable_len_int(&data[index..])?;
            let property_identifier = PropertyIdentifier::try_from(property_identifier)?;
            i += len;
            index += len;
            let property_value = &data[index..];
            match property_identifier {
                PropertyIdentifier::Reason => {
                    if reason.is_some() {
                        return Err(Error::ProtocolError("Reason specified multiple times"));
                    }
                    let _reason = extract_str(property_value)?.to_string();
                    let len = 2 + _reason.len();
                    reason = Some(_reason);
                    i += len;
                    index += len;
                }
                PropertyIdentifier::UserProperty => {
                    let key = extract_str(property_value)?.to_string();
                    let value = extract_str(&property_value[2 + key.len()..])?.to_string();
                    let len = 2 + key.len() + 2 + value.len();
                    let property = ReceivedUserProperty { key, value };
                    user_property.push(property);
                    i += len;
                    index += len;
                }
                _ => {
                    return Err(MalformedPacket::new(
                        "Received unexpected property for connect",
                    ))
                }
            };
        }

        let mut return_codes = Vec::new();
        while index < data.len() {
            return_codes.push(SubRcV5::try_from(data[index])?);
            index += 1;
        }

        Ok(Self {
            fixed_header: header,
            packet_identifier,
            data: SubAckData::V5 {
                protocol_level: PhantomData,
                return_codes,
                reason,
                user_property,
            },
        })
    }
}

#[cfg(test)]
mod test_v3 {
    use super::*;
    use std::io::BufWriter;
    use std::io::{BufReader, Read};

    #[test]
    fn serialize() {
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        let msg = SubAck::new_v3(
            42,
            vec![SubRcV3::SuccessQos0, SubRcV3::SuccessQos1, SubRcV3::Failure],
        );
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
        assert_eq!(&buf, &[144, 5, 0, 42, 0, 1, 0x80]);
    }

    #[test]
    fn deserialize() {
        let msg = [144, 5, 0, 42, 0, 1, 0x80];
        let expected = SubAck::new_v3(
            42,
            vec![SubRcV3::SuccessQos0, SubRcV3::SuccessQos1, SubRcV3::Failure],
        );
        let mut reader = BufReader::new(&msg[..]);
        let header = FixedHeader::try_read_sync(&mut reader).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(SubAck::try_read_v3(header, &data[..]).unwrap(), expected);
    }
}

#[cfg(test)]
mod test_v5 {
    use super::*;
    use std::io::BufWriter;
    use std::io::{BufReader, Read};

    #[test]
    fn serialize() {
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        let msg = SubAck::new_v5(
            42,
            vec![SubRcV5::SuccessQos0, SubRcV5::SuccessQos1, SubRcV5::Failure],
            None,
            vec![],
        );
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
        assert_eq!(&buf, &[144, 6, 0, 42, 0, 0, 1, 0x80]);
    }

    #[test]
    fn serialize_properties() {
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        let msg = SubAck::new_v5(
            42,
            vec![SubRcV5::SuccessQos0, SubRcV5::SuccessQos1, SubRcV5::Failure],
            Some("reason".to_string()),
            vec![ReceivedUserProperty {
                key: "property1".to_string(),
                value: "value1".to_string(),
            }],
        );
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
        assert_eq!(
            &buf,
            &[
                144, 35, 0, 42, // properties
                29, // reason
                31, 0, 6, b'r', b'e', b'a', b's', b'o', b'n', // User property
                38, 0, 9, b'p', b'r', b'o', b'p', b'e', b'r', b't', b'y', b'1', 0, 6, b'v', b'a',
                b'l', b'u', b'e', b'1', // payload
                0, 1, 0x80
            ]
        );
    }

    #[test]
    fn deserialize() {
        let msg = [144, 6, 0, 42, 0, 0, 0x97, 0x80];
        let expected = SubAck::new_v5(
            42,
            vec![
                SubRcV5::SuccessQos0,
                SubRcV5::QuotaExceeded,
                SubRcV5::Failure,
            ],
            None,
            vec![],
        );
        let mut reader = BufReader::new(&msg[..]);
        let header = FixedHeader::try_read_sync(&mut reader).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(SubAck::try_read_v5(header, &data[..]).unwrap(), expected);
    }

    #[test]
    fn deserialize_properties() {
        let msg = [
            144, 35, 0, 42, // properties
            29, // reason
            31, 0, 6, b'r', b'e', b'a', b's', b'o', b'n', // User property
            38, 0, 9, b'p', b'r', b'o', b'p', b'e', b'r', b't', b'y', b'1', 0, 6, b'v', b'a', b'l',
            b'u', b'e', b'1', // payload
            0, 0x97, 0x80,
        ];
        let expected = SubAck::new_v5(
            42,
            vec![
                SubRcV5::SuccessQos0,
                SubRcV5::QuotaExceeded,
                SubRcV5::Failure,
            ],
            Some("reason".to_string()),
            vec![ReceivedUserProperty {
                key: "property1".to_string(),
                value: "value1".to_string(),
            }],
        );
        let mut reader = BufReader::new(&msg[..]);
        let header = FixedHeader::try_read_sync(&mut reader).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(SubAck::try_read_v5(header, &data[..]).unwrap(), expected);
    }
}

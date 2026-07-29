use bytes::{Buf, BufMut, Bytes};

use crate::{
    util::{extract_str, read_variable_len_int, variable_len_int_size, write_variable_len_int},
    version::PacketProperties,
    Error, MalformedPacket, MqttV3_1_1, MqttV5_0_0, MqttVersion, Packet, Property,
    PropertyIdentifier, UserProperty,
};

use super::fixed_header::{ControlPacketType, FixedHeader};

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum SubRcV3 {
    SuccessQos0 = 0,
    SuccessQos1 = 1,
    SuccessQos2 = 2,
    Failure = 0x80,
}

impl PacketProperties for Vec<SubRcV3> {
    fn try_read(data: &mut Bytes) -> Result<Self, Error> {
        let mut return_codes = Vec::new();
        while data.has_remaining() {
            return_codes.push(SubRcV3::try_from(data.try_get_u8()?)?);
        }
        Ok(return_codes)
    }

    fn write_properties(&self, buf: &mut impl BufMut) {
        for rc in self {
            buf.put_u8(*rc as u8);
        }
    }

    fn properties_block_len(&self) -> usize {
        self.len()
    }

    fn properties_len(&self) -> usize {
        // V3.1.1 SUBACK has no MQTT5-style properties section; properties_block_len is
        // overridden above to skip the length-prefix entirely.
        0
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
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

#[derive(Debug, Clone, PartialEq)]
pub struct SubAckDataV5 {
    pub return_codes: Vec<SubRcV5>,
    /// UTF-8 Encoded String representing the reason associated with this response.
    /// This Reason String is a human readable string designed for diagnostics
    /// and is not intended to be parsed by the receiver
    pub reason: Option<String>,
    /// UTF-8 String Pair. This property can be used to provide additional
    /// diagnostic or other information
    pub user_property: Vec<UserProperty>,
}

impl PacketProperties for SubAckDataV5 {
    fn try_read(data: &mut Bytes) -> Result<Self, Error> {
        let len_properties = read_variable_len_int(data)? as usize;
        let mut reason = None;
        let mut user_property = Vec::new();

        if data.remaining() < len_properties {
            return Err(MalformedPacket::new("Packet too short to parse"));
        }
        let properties = &mut data.split_to(len_properties);

        while properties.has_remaining() {
            let property_identifier = crate::util::read_variable_len_int(properties)?;
            let property_identifier = PropertyIdentifier::try_from(property_identifier)?;
            match property_identifier {
                PropertyIdentifier::Reason => {
                    if reason.is_some() {
                        return Err(Error::ProtocolError("Reason specified multiple times"));
                    }
                    reason = Some(extract_str(properties)?.to_string());
                }
                PropertyIdentifier::UserProperty => {
                    let key = extract_str(properties)?.to_string();
                    let value = extract_str(properties)?.to_string();
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

        let mut return_codes = Vec::new();
        while data.has_remaining() {
            return_codes.push(SubRcV5::try_from(data.try_get_u8()?)?);
        }

        Ok(Self {
            return_codes,
            reason,
            user_property,
        })
    }

    fn write_properties(&self, buf: &mut impl BufMut) {
        let property_len = self.properties_len();
        write_variable_len_int(property_len as u64, buf);
        self.reason.serialize(PropertyIdentifier::Reason, buf);
        self.user_property
            .serialize(PropertyIdentifier::UserProperty, buf);
        for rc in &self.return_codes {
            buf.put_u8((*rc) as u8);
        }
    }

    fn properties_block_len(&self) -> usize {
        let l = self.properties_len();
        variable_len_int_size(l) + l + self.return_codes.len()
    }
    fn properties_len(&self) -> usize {
        self.user_property.property_len() + self.reason.property_len()
    }
}

#[derive(Debug, Clone, PartialEq)]
/// A SUBACK Packet is sent by the Server to the Client to confirm receipt and processing of a SUBSCRIBE Packet.
pub struct SubAck<V: MqttVersion> {
    packet_identifier: u16,
    data: V::SubAckData,
}

impl<V: MqttVersion> Packet for SubAck<V> {
    fn write_to_buf(&self, buf: &mut impl BufMut) {
        let fixed_header = FixedHeader::new(
            ControlPacketType::SubAck,
            2 + self.data.properties_block_len(),
        );
        fixed_header.write_to_buf(buf);
        buf.put_u16(self.packet_identifier);

        self.data.write_properties(buf);
    }

    fn try_read(header: FixedHeader, data: &mut Bytes) -> Result<Self, Error> {
        assert_eq!(header.control_packet_type, ControlPacketType::SubAck);
        let packet_identifier = data.try_get_u16()?;

        Ok(Self {
            packet_identifier,
            data: V::SubAckData::try_read(data)?,
        })
    }
}

impl<V: MqttVersion> SubAck<V> {
    pub fn packet_identifier(&self) -> u16 {
        self.packet_identifier
    }
    pub fn new(packet_identifier: u16, data: V::SubAckData) -> Self {
        Self {
            packet_identifier,
            data,
        }
    }
}
impl SubAck<MqttV3_1_1> {
    pub fn return_codes(&self) -> &[SubRcV3] {
        &self.data
    }
}

impl SubAck<MqttV5_0_0> {
    pub fn return_codes(&self) -> &[SubRcV5] {
        &self.data.return_codes
    }

    pub fn reason(&self) -> Option<&String> {
        self.data.reason.as_ref()
    }

    pub fn user_property(&self) -> &[UserProperty] {
        &self.data.user_property
    }
}

#[cfg(test)]
mod test_v3 {
    use bytes::BytesMut;

    use super::*;

    #[test]
    fn serialize() {
        let mut buf = Vec::new();
        let msg = SubAck::<MqttV3_1_1>::new(
            42,
            vec![SubRcV3::SuccessQos0, SubRcV3::SuccessQos1, SubRcV3::Failure],
        );
        msg.write_to_buf(&mut buf);
        assert_eq!(&buf, &[144, 5, 0, 42, 0, 1, 0x80]);
    }

    #[test]
    fn deserialize() {
        let msg = [144, 5, 0, 42, 0, 1, 0x80];
        let expected = SubAck::<MqttV3_1_1>::new(
            42,
            vec![SubRcV3::SuccessQos0, SubRcV3::SuccessQos1, SubRcV3::Failure],
        );
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(SubAck::try_read(header, &mut body).unwrap(), expected);
    }
}

#[cfg(test)]
mod test_v5 {
    use bytes::BytesMut;

    use super::*;

    #[test]
    fn serialize() {
        let mut buf = Vec::new();
        let msg = SubAck::<MqttV5_0_0>::new(
            42,
            SubAckDataV5 {
                return_codes: vec![SubRcV5::SuccessQos0, SubRcV5::SuccessQos1, SubRcV5::Failure],
                reason: None,
                user_property: vec![],
            },
        );
        msg.write_to_buf(&mut buf);
        assert_eq!(&buf, &[144, 6, 0, 42, 0, 0, 1, 0x80]);
    }

    #[test]
    fn serialize_properties() {
        let mut buf = Vec::new();
        let msg = SubAck::<MqttV5_0_0>::new(
            42,
            SubAckDataV5 {
                return_codes: vec![SubRcV5::SuccessQos0, SubRcV5::SuccessQos1, SubRcV5::Failure],
                reason: Some("reason".to_string()),
                user_property: vec![UserProperty {
                    key: "property1".to_string(),
                    value: "value1".to_string(),
                }],
            },
        );
        msg.write_to_buf(&mut buf);
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
        let expected = SubAck::<MqttV5_0_0>::new(
            42,
            SubAckDataV5 {
                return_codes: vec![
                    SubRcV5::SuccessQos0,
                    SubRcV5::QuotaExceeded,
                    SubRcV5::Failure,
                ],
                reason: None,
                user_property: vec![],
            },
        );
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(SubAck::try_read(header, &mut body).unwrap(), expected);
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
        let expected = SubAck::<MqttV5_0_0>::new(
            42,
            SubAckDataV5 {
                return_codes: vec![
                    SubRcV5::SuccessQos0,
                    SubRcV5::QuotaExceeded,
                    SubRcV5::Failure,
                ],
                reason: Some("reason".to_string()),
                user_property: vec![UserProperty {
                    key: "property1".to_string(),
                    value: "value1".to_string(),
                }],
            },
        );
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(SubAck::try_read(header, &mut body).unwrap(), expected);
    }
}

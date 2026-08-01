use bytes::{Buf, BufMut, Bytes};

use crate::{
    util::{extract_str, read_variable_len_int, variable_len_int_size, write_variable_len_int},
    version::PacketProperties,
    ControlPacketType, Error, FixedHeader, MalformedPacket, MqttVersion, Packet, Property,
    PropertyIdentifier, UserProperty,
};

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

#[derive(Debug, Clone, PartialEq)]
pub struct UnSubAckDataV5 {
    pub reason_code: Vec<UnsubAckReasonCode>,
    pub reason: Option<String>,
    pub user_property: Vec<UserProperty>,
}

impl PacketProperties for UnSubAckDataV5 {
    fn try_read(data: &mut Bytes) -> Result<Self, Error> {
        let prop_len = read_variable_len_int(data)? as usize;
        if prop_len > data.remaining() {
            return Err(MalformedPacket::new("Packet too short to parse"));
        }
        let properties = &mut data.split_to(prop_len);

        let mut out = Self {
            reason_code: Vec::new(),
            reason: None,
            user_property: Vec::new(),
        };

        while properties.has_remaining() {
            let property_identifier = crate::util::read_variable_len_int(properties)?;
            let property_identifier = PropertyIdentifier::try_from(property_identifier)?;
            match property_identifier {
                PropertyIdentifier::Reason => {
                    if out.reason.is_some() {
                        return Err(Error::ProtocolError("Reason specified multiple times"));
                    }
                    out.reason = Some(extract_str(properties)?.to_string());
                }
                PropertyIdentifier::UserProperty => {
                    let key = extract_str(properties)?.to_string();
                    let value = extract_str(properties)?.to_string();
                    let property = UserProperty { key, value };
                    out.user_property.push(property);
                }
                _ => {
                    return Err(MalformedPacket::new(
                        "Received unexpected property for connect",
                    ))
                }
            }
        }
        while data.has_remaining() {
            out.reason_code
                .push(UnsubAckReasonCode::try_from(data.try_get_u8()?)?);
        }
        Ok(out)
    }

    fn write_properties(&self, buf: &mut impl BufMut) {
        write_variable_len_int(self.properties_len() as u64, buf);
        self.reason.serialize(PropertyIdentifier::Reason, buf);
        self.user_property
            .serialize(PropertyIdentifier::UserProperty, buf);

        for code in &self.reason_code {
            buf.put_u8((*code) as u8);
        }
    }

    fn properties_block_len(&self) -> usize {
        let l = self.properties_len();
        variable_len_int_size(l) + l + self.reason_code.len()
    }

    fn properties_len(&self) -> usize {
        self.reason.property_len() + self.user_property.property_len()
    }
}

#[derive(Debug, Clone, PartialEq)]
/// The UNSUBACK Packet is sent by the Server to the Client to confirm receipt of an UNSUBSCRIBE Packet.
pub struct UnsubAck<V: MqttVersion> {
    packet_identifier: u16,
    properties: V::UnSubAckProperties,
}

impl<V: MqttVersion> Packet for UnsubAck<V> {
    fn write_to_buf(&self, buf: &mut impl BufMut) {
        let fixed_header = FixedHeader::new(
            ControlPacketType::UnsubscribeAck,
            2 + self.properties.properties_block_len(),
        );
        fixed_header.write_to_buf(buf);
        buf.put_u16(self.packet_identifier);
        self.properties.write_properties(buf);
    }

    fn try_read(header: FixedHeader, data: &mut Bytes) -> Result<Self, Error> {
        assert_eq!(
            header.control_packet_type,
            ControlPacketType::UnsubscribeAck
        );
        let packet_identifier = data.try_get_u16()?;

        let properties = V::UnSubAckProperties::try_read(data)?;

        Ok(Self {
            packet_identifier,
            properties,
        })
    }
}
impl<V: MqttVersion> UnsubAck<V> {
    pub fn packet_identifier(&self) -> u16 {
        self.packet_identifier
    }

    pub fn new(packet_identifier: u16, properties: V::UnSubAckProperties) -> Self {
        Self {
            packet_identifier,
            properties,
        }
    }
}

#[cfg(test)]
mod v3 {
    use crate::MqttV3_1_1;

    use super::*;

    use bytes::BytesMut;

    #[test]
    fn serialize() {
        let mut buf = Vec::new();
        let msg = UnsubAck::<MqttV3_1_1>::new(42, ());
        msg.write_to_buf(&mut buf);
        assert_eq!(&buf, &[176, 2, 0, 42]);
    }
    #[test]
    fn deserialize() {
        let msg = [176, 2, 0, 42];
        let expected = UnsubAck::new(42, ());
        let mut reader = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut reader, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(
            UnsubAck::<MqttV3_1_1>::try_read(header, &mut body).unwrap(),
            expected
        );
    }
}

#[cfg(test)]
mod v5 {
    use crate::MqttV5_0_0;

    use super::*;

    use bytes::BytesMut;

    #[test]
    fn serialize() {
        let mut buf = Vec::new();
        let msg = UnsubAck::<MqttV5_0_0>::new(
            42,
            UnSubAckDataV5 {
                reason_code: vec![
                    UnsubAckReasonCode::NotAuthorized,
                    UnsubAckReasonCode::Success,
                ],
                reason: None,
                user_property: Vec::new(),
            },
        );
        msg.write_to_buf(&mut buf);
        assert_eq!(&buf, &[176, 5, 0, 42, 0, 135, 0]);
    }

    #[test]
    fn serialize_reason() {
        let mut buf = Vec::new();
        let msg = UnsubAck::<MqttV5_0_0>::new(
            42,
            UnSubAckDataV5 {
                reason_code: vec![UnsubAckReasonCode::Success],
                reason: Some("test".to_string()),
                user_property: Vec::new(),
            },
        );
        msg.write_to_buf(&mut buf);
        assert_eq!(
            &buf,
            &[176, 11, 0, 42, 7, 31, 0, 4, b't', b'e', b's', b't', 0,]
        );
    }
    #[test]
    fn serialize_user_property() {
        let mut buf = Vec::new();
        let msg = UnsubAck::<MqttV5_0_0>::new(
            22,
            UnSubAckDataV5 {
                reason_code: vec![UnsubAckReasonCode::NoSubscriptionExisted],
                reason: None,
                user_property: vec![UserProperty {
                    key: "key".to_string(),
                    value: "value".to_string(),
                }],
            },
        );
        msg.write_to_buf(&mut buf);
        assert_eq!(
            &buf,
            &[
                176, 17, 0, 22, 13, 38, 0, 3, b'k', b'e', b'y', 0, 5, b'v', b'a', b'l', b'u', b'e',
                17
            ]
        );
    }
    #[test]
    fn deserialize() {
        let msg = [176, 4, 0, 42, 0, 135];
        let expected = UnsubAck::<MqttV5_0_0>::new(
            42,
            UnSubAckDataV5 {
                reason_code: vec![UnsubAckReasonCode::NotAuthorized],
                reason: None,
                user_property: Vec::new(),
            },
        );
        let mut reader = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut reader, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(
            UnsubAck::<MqttV5_0_0>::try_read(header, &mut body).unwrap(),
            expected
        );
    }

    #[test]
    fn deserialize_reason() {
        let msg = [176, 11, 0, 42, 7, 31, 0, 4, b't', b'e', b's', b't', 135];
        let expected = UnsubAck::<MqttV5_0_0>::new(
            42,
            UnSubAckDataV5 {
                reason_code: vec![UnsubAckReasonCode::NotAuthorized],
                reason: Some("test".to_string()),
                user_property: Vec::new(),
            },
        );
        let mut reader = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut reader, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(
            UnsubAck::<MqttV5_0_0>::try_read(header, &mut body).unwrap(),
            expected
        );
    }
    #[test]
    fn deserialize_user_property() {
        let msg = [
            176, 18, 0, 42, 13, 38, 0, 3, b'k', b'e', b'y', 0, 5, b'v', b'a', b'l', b'u', b'e', 0,
            0,
        ];
        let expected = UnsubAck::<MqttV5_0_0>::new(
            42,
            UnSubAckDataV5 {
                reason_code: vec![UnsubAckReasonCode::Success, UnsubAckReasonCode::Success],
                reason: None,
                user_property: vec![UserProperty {
                    key: "key".to_string(),
                    value: "value".to_string(),
                }],
            },
        );
        let mut reader = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut reader, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(UnsubAck::try_read(header, &mut body).unwrap(), expected);
    }

    /// Regression (found by the fuzzer): the v5 property-length field claims more
    /// bytes than remain in the buffer. Must return an error, not panic in
    /// `bytes::split_to`.
    #[test]
    fn deserialize_property_len_overflow_is_err() {
        // property length = 28 (0x1c), but only 2 property bytes follow.
        let msg = [176, 3, 28, 16, 24];
        let mut reader = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut reader, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert!(UnsubAck::<MqttV5_0_0>::try_read(header, &mut body).is_err());
    }
}

use std::marker::PhantomData;

use bytes::{Buf, BufMut, Bytes};

use crate::{
    util::{extract_bytes, read_variable_len_int, variable_len_int_size, write_variable_len_int},
    ControlPacketType, Error, MalformedPacket, MqttV3_1_1, MqttV5_0_0, PayloadFormat, Property,
    PropertyIdentifier, UserProperty,
};

use super::{
    fixed_header::{self, FixedHeader},
    util::{extract_str, write_str, MqttTopic, Qos, QosPacketIdentifier},
};

#[derive(Debug, PartialEq)]
pub enum PublishProperties<V> {
    V3 {
        protocol_level: PhantomData<V>,
    },
    V5 {
        protocol_level: PhantomData<V>,

        payload_format: Option<PayloadFormat>,
        /// If present, the Four Byte value is the lifetime of the Will Message in seconds and is sent as the
        /// Publication Expiry Interval when the Server publishes the Will Message.
        /// If absent, no Message Expiry Interval is sent when the Server publishes the Will Message.
        message_expiry_interval: Option<u32>,
        /// A Topic Alias is an integer value that is used to identify the Topic instead of using the Topic Name.
        /// This reduces the size of the PUBLISH packet, and is useful when the Topic Names are long and the same
        /// Topic Names are used repetitively within a Network Connection.
        ///
        /// The sender decides whether to use a Topic Alias and chooses the value. It sets a Topic Alias mapping
        /// by including a non-zero length Topic Name and a Topic Alias in the PUBLISH packet. The receiver
        /// processes the PUBLISH as normal but also sets the specified Topic Alias mapping to this Topic Name.
        ///
        /// If a Topic Alias mapping has been set at the receiver, a sender can send a PUBLISH packet that
        /// contains that Topic Alias and a zero length Topic Name. The receiver then treats the incoming
        /// PUBLISH as if it had contained the Topic Name of the Topic Alias.
        ///
        /// A sender can modify the Topic Alias mapping by sending another PUBLISH in the same Network
        /// Connection with the same Topic Alias value and a different non-zero length Topic Name.
        ///
        /// Topic Alias mappings exist only within a Network Connection and last only for the lifetime
        /// of that Network Connection. A receiver MUST NOT carry forward any Topic Alias mappings from
        /// one Network Connection to another [MQTT-3.3.2-7].
        ///
        /// A Topic Alias of 0 is not permitted. A sender MUST NOT send a PUBLISH packet containing a
        /// Topic Alias which has the value 0
        ///
        /// A Client MUST NOT send a PUBLISH packet with a Topic Alias greater than the Topic Alias
        /// Maximum value returned by the Server in the CONNACK packet. A Client MUST accept all
        /// Topic Alias values greater than 0 and less than or equal to the Topic Alias Maximum
        /// value that it sent in the CONNECT packet [MQTT-3.3.2-10].
        ///
        /// A Server MUST NOT send a PUBLISH packet with a Topic Alias greater than the Topic
        /// Alias Maximum value sent by the Client in the CONNECT packet. A Server MUST accept
        /// all Topic Alias values greater than 0 and less than or equal to the Topic Alias Maximum
        /// value that it returned in the CONNACK packet
        ///
        /// The Topic Alias mappings used by the Client and Server are independent from each other.
        /// Thus, when a Client sends a PUBLISH containing a Topic Alias value of 1 to a Server
        /// and the Server sends a PUBLISH with a Topic Alias value of 1 to that Client they will
        /// in general be referring to different Topics.
        topic_alias: Option<u16>,
        /// UTF-8 Encoded String which is used as the Topic Name for a response message
        /// The presence of a Response Topic identifies the Will Message as a Request.
        response_topic: Option<String>,
        /// The Correlation Data is used by the sender of the Request Message to identify which request
        /// the Response Message is for when it is received
        /// The value of the Correlation Data only has meaning to the sender of the Request Message
        /// and receiver of the Response Message.
        correlation_data: Bytes,
        user_property: Vec<UserProperty>,
        /// The Subscription Identifier can have the value of 1 to 268,435,455. It is a Protocol Error if
        /// the Subscription Identifier has a value of 0. Multiple Subscription Identifiers will be included
        /// if the publication is the result of a match to more than one subscription, in this case their
        /// order is not significant.
        subscription_identifier: Option<u64>,
        /// UTF-8 Encoded String describing the content of the Will Message
        /// The value of the Content Type is defined by the sending and receiving application.
        content_type: Option<String>,
    },
}

#[derive(Debug, PartialEq)]
/// A PUBLISH Control Packet is sent from a Client to a Server or from Server to a Client to transport an Application Message.
pub struct Publish<V> {
    pub fixed_header: FixedHeader,
    pub topic: String,
    pub packet_identifier: Option<u16>,
    pub payload: Bytes,
    pub properties: PublishProperties<V>,
}

impl<V> Publish<V> {
    pub fn dup(&self) -> bool {
        match self.fixed_header.control_packet_type {
            ControlPacketType::Publish { dup, .. } => dup,
            _ => unreachable!(),
        }
    }
    pub fn retain(&self) -> bool {
        match self.fixed_header.control_packet_type {
            ControlPacketType::Publish { retain, .. } => retain,
            _ => unreachable!(),
        }
    }
    pub fn qos(&self) -> Qos {
        match self.fixed_header.control_packet_type {
            ControlPacketType::Publish { qos, .. } => qos,
            _ => unreachable!(),
        }
    }

    /// Re-calculate fixed header length
    ///
    /// We initially have no properties -> property len == 0
    /// -> we have 1 byte to store it
    ///
    /// if our properties take more than 128 bytes, we need more than
    /// 1 byte for the length -> we need to modify fixed header
    ///
    /// we also need to add length of properties into fixed header and we know them
    /// only at serialization time due to builder pattern (without finalize)
    fn re_calculate_fixed_header_length(&mut self) {
        match &self.properties {
            PublishProperties::V3 { .. } => (),
            PublishProperties::V5 { .. } => {
                let remaining_length = 2
                    + self.topic.len()
                    + {
                        if self.packet_identifier.is_some() {
                            2
                        } else {
                            0
                        }
                    }
                    + self.payload.len();
                let properties_len = self.properties_len();
                self.fixed_header.remaining_length =
                    variable_len_int_size(properties_len) + properties_len + remaining_length;
            }
        }
    }

    fn properties_len(&self) -> usize {
        match &self.properties {
            PublishProperties::V3 { .. } => 0,
            PublishProperties::V5 {
                payload_format,
                message_expiry_interval,
                topic_alias,
                response_topic,
                correlation_data,
                user_property,
                subscription_identifier,
                content_type,
                ..
            } => {
                payload_format.property_len()
                    + message_expiry_interval.property_len()
                    + topic_alias.property_len()
                    + response_topic.property_len()
                    + correlation_data.property_len()
                    + user_property.property_len()
                    + subscription_identifier
                        .map(|id| 1 + variable_len_int_size(id as usize))
                        .unwrap_or_default()
                    + content_type.property_len()
            }
        }
    }

    fn write_properties(&self, buf: &mut impl BufMut) {
        if let PublishProperties::V5 {
            payload_format,
            message_expiry_interval,
            topic_alias,
            response_topic,
            correlation_data,
            user_property,
            subscription_identifier,
            content_type,
            ..
        } = &self.properties
        {
            let properties_len = self.properties_len();
            write_variable_len_int(properties_len as u64, buf);

            fn add_subscription_identifier(
                subscription_identifier: Option<u64>,
                buf: &mut impl BufMut,
            ) {
                if let Some(id) = subscription_identifier {
                    write_variable_len_int(PropertyIdentifier::SubscriptionIdentifier as u64, buf);
                    write_variable_len_int(id, buf);
                }
            }

            payload_format.serialize(PropertyIdentifier::PayloadFormatIndicator, buf);
            message_expiry_interval.serialize(PropertyIdentifier::MessageExpiryInterval, buf);
            topic_alias.serialize(PropertyIdentifier::TopicAlias, buf);
            response_topic.serialize(PropertyIdentifier::ResponseTopic, buf);
            correlation_data.serialize(PropertyIdentifier::CorrelationData, buf);
            user_property.serialize(PropertyIdentifier::UserProperty, buf);
            add_subscription_identifier(*subscription_identifier, buf);
            content_type.serialize(PropertyIdentifier::ContentType, buf);
        }
    }

    pub fn write_to_buf(&mut self, buf: &mut impl BufMut) {
        self.re_calculate_fixed_header_length();
        self.fixed_header.write_to_buf(buf);
        write_str(&self.topic, buf);
        if let Some(packet_identifier) = self.packet_identifier {
            buf.put_u16(packet_identifier);
        }
        self.write_properties(buf);
        buf.put(&self.payload[..]);
    }
}

impl Publish<MqttV3_1_1> {
    pub fn new_v3(
        dup: bool,
        qos: QosPacketIdentifier,
        retain: bool,
        topic: MqttTopic,
        payload: Bytes,
    ) -> Self {
        let topic = topic.0;
        let packet_identifier = match &qos {
            QosPacketIdentifier::AtMostOnce => None,
            QosPacketIdentifier::AtLeastOnce(id) => Some(*id),
            QosPacketIdentifier::ExactlyOnce(id) => Some(*id),
        };

        let remaining_length = 2
            + topic.len()
            + {
                if packet_identifier.is_some() {
                    2
                } else {
                    0
                }
            }
            + payload.len();
        Self {
            fixed_header: FixedHeader::new(
                super::fixed_header::ControlPacketType::Publish {
                    dup,
                    qos: qos.into(),
                    retain,
                },
                remaining_length,
            ),
            topic,
            packet_identifier,
            payload,
            properties: PublishProperties::V3 {
                protocol_level: PhantomData,
            },
        }
    }
    pub fn try_read_v3(header: FixedHeader, data: &mut Bytes) -> Result<Self, Error> {
        let topic = extract_str(data)?.to_string();

        let (_, qos, _) = match header.control_packet_type {
            fixed_header::ControlPacketType::Publish { dup, qos, retain } => (dup, qos, retain),
            _ => unreachable!(),
        };

        let packet_identifier = if qos != Qos::AtMostOnce {
            Some(data.try_get_u16()?)
        } else {
            None
        };

        let payload = data.clone();

        Ok(Self {
            fixed_header: header,
            topic,
            packet_identifier,
            payload,
            properties: PublishProperties::V3 {
                protocol_level: PhantomData,
            },
        })
    }
}

impl Publish<MqttV5_0_0> {
    pub fn new_v5(
        dup: bool,
        qos: QosPacketIdentifier,
        retain: bool,
        topic: MqttTopic,
        payload: Bytes,
    ) -> Self {
        let topic = topic.0;
        let packet_identifier = match &qos {
            QosPacketIdentifier::AtMostOnce => None,
            QosPacketIdentifier::AtLeastOnce(id) => Some(*id),
            QosPacketIdentifier::ExactlyOnce(id) => Some(*id),
        };

        let remaining_length = 2
            + topic.len()
            + 1 // properties length is static 0 before we initialize them
            + {
                if packet_identifier.is_some() {
                    2
                } else {
                    0
                }
            }
            + payload.len();
        Self {
            fixed_header: FixedHeader::new(
                super::fixed_header::ControlPacketType::Publish {
                    dup,
                    qos: qos.into(),
                    retain,
                },
                remaining_length,
            ),
            topic,
            packet_identifier,
            payload,
            properties: PublishProperties::V5 {
                protocol_level: PhantomData,
                payload_format: None,
                message_expiry_interval: None,
                topic_alias: None,
                response_topic: None,
                correlation_data: Bytes::new(),
                user_property: Vec::new(),
                subscription_identifier: None,
                content_type: None,
            },
        }
    }

    fn read_property(&mut self, data: &mut Bytes) -> Result<(), Error> {
        let property_identifier = read_variable_len_int(data)?;
        let property_identifier = PropertyIdentifier::try_from(property_identifier)?;

        match &mut self.properties {
            PublishProperties::V3 { .. } => unreachable!(),
            PublishProperties::V5 {
                payload_format,
                message_expiry_interval,
                topic_alias,
                response_topic,
                correlation_data,
                user_property,
                subscription_identifier,
                content_type,
                ..
            } => match property_identifier {
                PropertyIdentifier::PayloadFormatIndicator => {
                    if payload_format.is_some() {
                        return Err(Error::ProtocolError(
                            "PayloadFormatIndicator specified multiple times",
                        ));
                    }
                    *payload_format = Some(match data.try_get_u8()? {
                        0 => PayloadFormat::Binary,
                        1 => PayloadFormat::Utf8,
                        _ => return Err(MalformedPacket::new("Invalid payload format")),
                    });
                }
                PropertyIdentifier::MessageExpiryInterval => {
                    if message_expiry_interval.is_some() {
                        return Err(Error::ProtocolError(
                            "MessageExpiryInterval specified multiple times",
                        ));
                    }
                    *message_expiry_interval = Some(data.try_get_u32()?);
                }
                PropertyIdentifier::TopicAlias => {
                    if topic_alias.is_some() {
                        return Err(Error::ProtocolError("TopicAlias specified multiple times"));
                    }
                    *topic_alias = Some(data.try_get_u16()?);
                }
                PropertyIdentifier::ResponseTopic => {
                    if response_topic.is_some() {
                        return Err(Error::ProtocolError(
                            "ResponseTopic specified multiple times",
                        ));
                    }
                    *response_topic = Some(extract_str(data)?);
                }
                PropertyIdentifier::CorrelationData => {
                    if !correlation_data.is_empty() {
                        return Err(Error::ProtocolError(
                            "CorrelationData specified multiple times",
                        ));
                    }
                    *correlation_data = extract_bytes(data)?;
                }
                PropertyIdentifier::UserProperty => {
                    let key = extract_str(data)?;
                    let value = extract_str(data)?;
                    user_property.push(UserProperty { key, value });
                }
                PropertyIdentifier::SubscriptionIdentifier => {
                    if subscription_identifier.is_some() {
                        return Err(Error::ProtocolError(
                            "SubscriptionIdentifier specified multiple times",
                        ));
                    }
                    *subscription_identifier = Some(read_variable_len_int(data)?);
                }
                PropertyIdentifier::ContentType => {
                    if content_type.is_some() {
                        return Err(Error::ProtocolError("ContentType specified multiple times"));
                    }
                    *content_type = Some(extract_str(data)?);
                }
                _ => {
                    return Err(MalformedPacket::new(
                        "Received unexpected property for connect",
                    ))
                }
            },
        };
        Ok(())
    }

    pub fn try_read_v5(header: FixedHeader, data: &mut Bytes) -> Result<Self, Error> {
        let topic = extract_str(data)?.to_string();

        let (_, qos, _) = match header.control_packet_type {
            fixed_header::ControlPacketType::Publish { dup, qos, retain } => (dup, qos, retain),
            _ => unreachable!(),
        };

        let packet_identifier = if qos != Qos::AtMostOnce {
            Some(data.try_get_u16()?)
        } else {
            None
        };

        let properties_len = read_variable_len_int(data)? as usize;

        if data.remaining() < properties_len {
            return Err(MalformedPacket::new("Packet too short to parse"));
        }
        let properties_end = data.remaining() - properties_len;

        let mut publish = Self {
            fixed_header: header,
            topic,
            packet_identifier,
            payload: Bytes::new(),
            properties: PublishProperties::V5 {
                protocol_level: PhantomData,
                payload_format: None,
                message_expiry_interval: None,
                topic_alias: None,
                response_topic: None,
                correlation_data: Bytes::new(),
                user_property: Vec::new(),
                subscription_identifier: None,
                content_type: None,
            },
        };

        while data.remaining() > properties_end {
            publish.read_property(data)?;
        }

        publish.payload = data.clone();

        Ok(publish)
    }

    pub fn set_payload_format(mut self, value: PayloadFormat) -> Self {
        match &mut self.properties {
            PublishProperties::V3 { .. } => unreachable!(),
            PublishProperties::V5 { payload_format, .. } => *payload_format = Some(value),
        }
        self
    }
    pub fn payload_format(&self) -> Option<PayloadFormat> {
        match &self.properties {
            PublishProperties::V3 { .. } => unreachable!(),
            PublishProperties::V5 { payload_format, .. } => *payload_format,
        }
    }

    pub fn set_message_expiry_interval(mut self, interval: u32) -> Self {
        match &mut self.properties {
            PublishProperties::V3 { .. } => unreachable!(),
            PublishProperties::V5 {
                message_expiry_interval,
                ..
            } => *message_expiry_interval = Some(interval),
        }
        self
    }
    pub fn message_expiry_interval(&self) -> Option<u32> {
        match &self.properties {
            PublishProperties::V3 { .. } => unreachable!(),
            PublishProperties::V5 {
                message_expiry_interval,
                ..
            } => *message_expiry_interval,
        }
    }

    pub fn set_topic_alias(mut self, alias: u16) -> Self {
        match &mut self.properties {
            PublishProperties::V3 { .. } => unreachable!(),
            PublishProperties::V5 { topic_alias, .. } => *topic_alias = Some(alias),
        }
        self
    }
    pub fn topic_alias(&self) -> Option<u16> {
        match &self.properties {
            PublishProperties::V3 { .. } => unreachable!(),
            PublishProperties::V5 { topic_alias, .. } => *topic_alias,
        }
    }

    pub fn set_response_topic(mut self, topic: String) -> Self {
        match &mut self.properties {
            PublishProperties::V3 { .. } => unreachable!(),
            PublishProperties::V5 { response_topic, .. } => *response_topic = Some(topic),
        }
        self
    }
    pub fn response_topic(&self) -> Option<&str> {
        match &self.properties {
            PublishProperties::V3 { .. } => unreachable!(),
            PublishProperties::V5 { response_topic, .. } => response_topic.as_deref(),
        }
    }

    pub fn set_correlation_data(mut self, data: Bytes) -> Self {
        match &mut self.properties {
            PublishProperties::V3 { .. } => unreachable!(),
            PublishProperties::V5 {
                correlation_data, ..
            } => *correlation_data = data,
        }
        self
    }
    pub fn correlation_data(&self) -> &[u8] {
        match &self.properties {
            PublishProperties::V3 { .. } => unreachable!(),
            PublishProperties::V5 {
                correlation_data, ..
            } => correlation_data,
        }
    }

    pub fn set_user_property(mut self, user_properties: Vec<UserProperty>) -> Self {
        match &mut self.properties {
            PublishProperties::V3 { .. } => unreachable!(),
            PublishProperties::V5 { user_property, .. } => *user_property = user_properties,
        }
        self
    }
    pub fn user_property(&self) -> &[UserProperty] {
        match &self.properties {
            PublishProperties::V3 { .. } => unreachable!(),
            PublishProperties::V5 { user_property, .. } => user_property,
        }
    }

    pub fn set_subscription_identifier(mut self, value: u64) -> Self {
        match &mut self.properties {
            PublishProperties::V3 { .. } => unreachable!(),
            PublishProperties::V5 {
                subscription_identifier,
                ..
            } => *subscription_identifier = Some(value),
        }
        self
    }
    pub fn subscription_identifier(&self) -> Option<u64> {
        match &self.properties {
            PublishProperties::V3 { .. } => unreachable!(),
            PublishProperties::V5 {
                subscription_identifier,
                ..
            } => *subscription_identifier,
        }
    }

    pub fn set_content_type(mut self, value: String) -> Self {
        match &mut self.properties {
            PublishProperties::V3 { .. } => unreachable!(),
            PublishProperties::V5 { content_type, .. } => *content_type = Some(value),
        }
        self
    }
    pub fn content_type(&self) -> Option<&str> {
        match &self.properties {
            PublishProperties::V3 { .. } => unreachable!(),
            PublishProperties::V5 { content_type, .. } => content_type.as_deref(),
        }
    }
}

#[cfg(test)]
mod test_v3 {
    

    use bytes::BytesMut;

    use super::*;

    #[test]
    fn serialize() {
        let topic = MqttTopic::try_from("topic").unwrap();
        let mut msg = Publish::new_v3(
            false,
            QosPacketIdentifier::AtMostOnce,
            false,
            topic,
            Bytes::from_static(b"payload"),
        );
        let mut buf = Vec::new();
        msg.write_to_buf(&mut buf);
        assert_eq!(
            &buf,
            &[
                48, 14, 0, 5, b't', b'o', b'p', b'i', b'c', b'p', b'a', b'y', b'l', b'o', b'a',
                b'd'
            ]
        );
    }
    #[test]
    fn serialize2() {
        let topic = MqttTopic::try_from("foo2").unwrap();
        let mut msg = Publish::new_v3(
            false,
            QosPacketIdentifier::AtMostOnce,
            false,
            topic,
            Bytes::from_static(b"foo"),
        );
        let mut buf = Vec::new();
        msg.write_to_buf(&mut buf);
        assert_eq!(
            &buf,
            &[48, 9, 0, 4, b'f', b'o', b'o', b'2', b'f', b'o', b'o']
        );
    }
    #[test]
    fn serialize_qos() {
        let topic = MqttTopic::try_from("topic").unwrap();
        let mut msg = Publish::new_v3(
            false,
            QosPacketIdentifier::ExactlyOnce(42),
            false,
            topic,
            Bytes::from_static(b"payload"),
        );
        let mut buf = Vec::new();
        msg.write_to_buf(&mut buf);
        assert_eq!(
            &buf,
            &[
                48 | 2 << 1,
                16,
                0,
                5,
                b't',
                b'o',
                b'p',
                b'i',
                b'c',
                0,
                42,
                b'p',
                b'a',
                b'y',
                b'l',
                b'o',
                b'a',
                b'd'
            ]
        );
    }
    #[test]
    fn serialize_dup() {
        let topic = MqttTopic::try_from("topic").unwrap();
        let mut msg = Publish::new_v3(
            true,
            QosPacketIdentifier::AtMostOnce,
            false,
            topic,
            Bytes::from_static(b"payload"),
        );
        let mut buf = Vec::new();
        msg.write_to_buf(&mut buf);
        assert_eq!(
            &buf,
            &[
                48 | 1 << 3,
                14,
                0,
                5,
                b't',
                b'o',
                b'p',
                b'i',
                b'c',
                b'p',
                b'a',
                b'y',
                b'l',
                b'o',
                b'a',
                b'd'
            ]
        );
    }
    #[test]
    fn serialize_retain() {
        let topic = MqttTopic::try_from("topic").unwrap();
        let mut msg = Publish::new_v3(
            false,
            QosPacketIdentifier::AtMostOnce,
            true,
            topic,
            Bytes::from_static(b"payload"),
        );
        let mut buf = Vec::new();
        msg.write_to_buf(&mut buf);
        assert_eq!(
            &buf,
            &[
                48 | 1,
                14,
                0,
                5,
                b't',
                b'o',
                b'p',
                b'i',
                b'c',
                b'p',
                b'a',
                b'y',
                b'l',
                b'o',
                b'a',
                b'd'
            ]
        );
    }
    #[test]
    fn deserialize() {
        let expected = Publish::new_v3(
            false,
            QosPacketIdentifier::AtMostOnce,
            false,
            MqttTopic::try_from("topic").unwrap(),
            Bytes::from_static(b"payload"),
        );
        let msg = [
            48, 14, 0, 5, b't', b'o', b'p', b'i', b'c', b'p', b'a', b'y', b'l', b'o', b'a', b'd',
        ];
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(Publish::try_read_v3(header, &mut body).unwrap(), expected);
    }
    #[test]
    fn deserialize_qos() {
        let expected = Publish::new_v3(
            false,
            QosPacketIdentifier::ExactlyOnce(42),
            false,
            MqttTopic::try_from("topic").unwrap(),
            Bytes::from_static(b"payload"),
        );
        let msg = [
            48 | 2 << 1,
            16,
            0,
            5,
            b't',
            b'o',
            b'p',
            b'i',
            b'c',
            0,
            42,
            b'p',
            b'a',
            b'y',
            b'l',
            b'o',
            b'a',
            b'd',
        ];
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(Publish::try_read_v3(header, &mut body).unwrap(), expected);
    }
    #[test]
    fn deserialize_dup() {
        let expected = Publish::new_v3(
            true,
            QosPacketIdentifier::AtMostOnce,
            false,
            MqttTopic::try_from("topic").unwrap(),
            Bytes::from_static(b"payload"),
        );
        let msg = [
            48 | 1 << 3,
            14,
            0,
            5,
            b't',
            b'o',
            b'p',
            b'i',
            b'c',
            b'p',
            b'a',
            b'y',
            b'l',
            b'o',
            b'a',
            b'd',
        ];
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(Publish::try_read_v3(header, &mut body).unwrap(), expected);
    }
    #[test]
    fn deserialize_retain() {
        let expected = Publish::new_v3(
            false,
            QosPacketIdentifier::AtMostOnce,
            true,
            MqttTopic::try_from("topic").unwrap(),
            Bytes::from_static(b"payload"),
        );
        let msg = [
            48 | 1,
            14,
            0,
            5,
            b't',
            b'o',
            b'p',
            b'i',
            b'c',
            b'p',
            b'a',
            b'y',
            b'l',
            b'o',
            b'a',
            b'd',
        ];
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(Publish::try_read_v3(header, &mut body).unwrap(), expected);
    }
}

#[cfg(test)]
mod test_v5 {
    

    use bytes::BytesMut;

    use super::*;

    #[test]
    fn serialize() {
        let topic = MqttTopic::try_from("topic").unwrap();
        let mut msg = Publish::new_v5(
            false,
            QosPacketIdentifier::AtMostOnce,
            false,
            topic,
            Bytes::from_static(b"payload"),
        );
        let mut buf = Vec::new();
        msg.write_to_buf(&mut buf);
        assert_eq!(
            &buf,
            &[
                48, 15, 0, 5, b't', b'o', b'p', b'i', b'c', 0, b'p', b'a', b'y', b'l', b'o', b'a',
                b'd'
            ]
        );
    }
    #[test]
    fn serialize2() {
        let topic = MqttTopic::try_from("foo2").unwrap();
        let mut msg = Publish::new_v5(
            false,
            QosPacketIdentifier::AtMostOnce,
            false,
            topic,
            Bytes::from_static(b"foo"),
        );
        let mut buf = Vec::new();
        msg.write_to_buf(&mut buf);
        assert_eq!(
            &buf,
            &[48, 10, 0, 4, b'f', b'o', b'o', b'2', 0, b'f', b'o', b'o']
        );
    }
    #[test]
    fn serialize_qos() {
        let topic = MqttTopic::try_from("topic").unwrap();
        let mut msg = Publish::new_v5(
            false,
            QosPacketIdentifier::ExactlyOnce(42),
            false,
            topic,
            Bytes::from_static(b"payload"),
        );
        let mut buf = Vec::new();
        msg.write_to_buf(&mut buf);
        assert_eq!(
            &buf,
            &[
                48 | 2 << 1,
                17,
                0,
                5,
                b't',
                b'o',
                b'p',
                b'i',
                b'c',
                0,
                42,
                0,
                b'p',
                b'a',
                b'y',
                b'l',
                b'o',
                b'a',
                b'd'
            ]
        );
    }
    #[test]
    fn serialize_dup() {
        let topic = MqttTopic::try_from("topic").unwrap();
        let mut msg = Publish::new_v5(
            true,
            QosPacketIdentifier::AtMostOnce,
            false,
            topic,
            Bytes::from_static(b"payload"),
        );
        let mut buf = Vec::new();
        msg.write_to_buf(&mut buf);
        assert_eq!(
            &buf,
            &[
                48 | 1 << 3,
                15,
                0,
                5,
                b't',
                b'o',
                b'p',
                b'i',
                b'c',
                0,
                b'p',
                b'a',
                b'y',
                b'l',
                b'o',
                b'a',
                b'd'
            ]
        );
    }
    #[test]
    fn serialize_retain() {
        let topic = MqttTopic::try_from("topic").unwrap();
        let mut msg = Publish::new_v5(
            false,
            QosPacketIdentifier::AtMostOnce,
            true,
            topic,
            Bytes::from_static(b"payload"),
        );
        let mut buf = Vec::new();
        msg.write_to_buf(&mut buf);
        assert_eq!(
            &buf,
            &[
                48 | 1,
                15,
                0,
                5,
                b't',
                b'o',
                b'p',
                b'i',
                b'c',
                0,
                b'p',
                b'a',
                b'y',
                b'l',
                b'o',
                b'a',
                b'd'
            ]
        );
    }
    #[test]
    fn deserialize() {
        let expected = Publish::new_v5(
            false,
            QosPacketIdentifier::AtMostOnce,
            false,
            MqttTopic::try_from("topic").unwrap(),
            Bytes::from_static(b"payload"),
        );
        let msg = [
            48, 15, 0, 5, b't', b'o', b'p', b'i', b'c', 0, b'p', b'a', b'y', b'l', b'o', b'a', b'd',
        ];
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(Publish::try_read_v5(header, &mut body).unwrap(), expected);
    }
    #[test]
    fn deserialize_qos() {
        let expected = Publish::new_v5(
            false,
            QosPacketIdentifier::ExactlyOnce(42),
            false,
            MqttTopic::try_from("topic").unwrap(),
            Bytes::from_static(b"payload"),
        );
        let msg = [
            48 | 2 << 1,
            17,
            0,
            5,
            b't',
            b'o',
            b'p',
            b'i',
            b'c',
            0,
            42,
            0,
            b'p',
            b'a',
            b'y',
            b'l',
            b'o',
            b'a',
            b'd',
        ];
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(Publish::try_read_v5(header, &mut body).unwrap(), expected);
    }
    #[test]
    fn deserialize_dup() {
        let expected = Publish::new_v5(
            true,
            QosPacketIdentifier::AtMostOnce,
            false,
            MqttTopic::try_from("topic").unwrap(),
            Bytes::from_static(b"payload"),
        );
        let msg = [
            48 | 1 << 3,
            15,
            0,
            5,
            b't',
            b'o',
            b'p',
            b'i',
            b'c',
            0,
            b'p',
            b'a',
            b'y',
            b'l',
            b'o',
            b'a',
            b'd',
        ];
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(Publish::try_read_v5(header, &mut body).unwrap(), expected);
    }
    #[test]
    fn deserialize_retain() {
        let expected = Publish::new_v5(
            false,
            QosPacketIdentifier::AtMostOnce,
            true,
            MqttTopic::try_from("topic").unwrap(),
            Bytes::from_static(b"payload"),
        );
        let msg = [
            48 | 1,
            15,
            0,
            5,
            b't',
            b'o',
            b'p',
            b'i',
            b'c',
            0,
            b'p',
            b'a',
            b'y',
            b'l',
            b'o',
            b'a',
            b'd',
        ];
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(Publish::try_read_v5(header, &mut body).unwrap(), expected);
    }

    #[test]
    fn serialize_properties() {
        let topic = MqttTopic::try_from("topic").unwrap();
        let mut msg = Publish::new_v5(
            false,
            QosPacketIdentifier::AtMostOnce,
            false,
            topic,
            Bytes::from_static(b"payload"),
        )
        .set_payload_format(PayloadFormat::Binary)
        .set_message_expiry_interval(10)
        .set_topic_alias(11)
        .set_response_topic("response".to_string())
        .set_correlation_data(Bytes::from_static(b"badcafee"))
        .set_user_property(vec![
            UserProperty {
                key: "property0".to_string(),
                value: "value0".to_string(),
            },
            UserProperty {
                key: "property1".to_string(),
                value: "value1".to_string(),
            },
        ])
        .set_subscription_identifier(12)
        .set_content_type("test".to_string());
        let mut buf = Vec::new();
        msg.write_to_buf(&mut buf);
        assert_eq!(
            &buf,
            &[
                48, 96, 0, 5, b't', b'o', b'p', b'i', b'c', //
                // Properties
                81, //
                // payload format
                1, 0, //
                // Message expiry
                2, 0, 0, 0, 10, //
                // Message expiry
                35, 0, 11, //
                // Response topic
                8, 0, 8, b'r', b'e', b's', b'p', b'o', b'n', b's', b'e', //
                // correlation data
                9, 0, 8, b'b', b'a', b'd', b'c', b'a', b'f', b'e', b'e', //
                // User property
                38, 0, 9, b'p', b'r', b'o', b'p', b'e', b'r', b't', b'y', b'0', 0, 6, b'v', b'a',
                b'l', b'u', b'e', b'0', //
                // User property
                38, 0, 9, b'p', b'r', b'o', b'p', b'e', b'r', b't', b'y', b'1', 0, 6, b'v', b'a',
                b'l', b'u', b'e', b'1', //
                // Subscription identifier
                11, 12, //
                // Content type
                3, 0, 4, b't', b'e', b's', b't', //
                // payload
                b'p', b'a', b'y', b'l', b'o', b'a', b'd'
            ]
        );
    }
    #[test]
    fn deserialize_properties() {
        let mut expected = Publish::new_v5(
            false,
            QosPacketIdentifier::AtMostOnce,
            false,
            MqttTopic::try_from("topic").unwrap(),
            Bytes::from_static(b"payload"),
        );

        match &mut expected.properties {
            PublishProperties::V3 { .. } => unreachable!(),
            PublishProperties::V5 {
                payload_format,
                message_expiry_interval,
                topic_alias,
                response_topic,
                correlation_data,
                user_property,
                subscription_identifier,
                content_type,
                ..
            } => {
                *payload_format = Some(PayloadFormat::Binary);
                *message_expiry_interval = Some(10);
                *topic_alias = Some(11);
                *response_topic = Some("response".to_string());
                *correlation_data = Bytes::from_static(b"badcafee");
                *user_property = vec![
                    UserProperty {
                        key: "property0".to_string(),
                        value: "value0".to_string(),
                    },
                    UserProperty {
                        key: "property1".to_string(),
                        value: "value1".to_string(),
                    },
                ];
                *subscription_identifier = Some(12);
                *content_type = Some("test".to_string());
            }
        }
        let msg = [
            48, 96, 0, 5, b't', b'o', b'p', b'i', b'c', //
            // Properties
            81, //
            // payload format
            1, 0, //
            // Message expiry
            2, 0, 0, 0, 10, //
            // Message expiry
            35, 0, 11, //
            // Response topic
            8, 0, 8, b'r', b'e', b's', b'p', b'o', b'n', b's', b'e', //
            // correlation data
            9, 0, 8, b'b', b'a', b'd', b'c', b'a', b'f', b'e', b'e', //
            // User property
            38, 0, 9, b'p', b'r', b'o', b'p', b'e', b'r', b't', b'y', b'0', 0, 6, b'v', b'a', b'l',
            b'u', b'e', b'0', //
            // User property
            38, 0, 9, b'p', b'r', b'o', b'p', b'e', b'r', b't', b'y', b'1', 0, 6, b'v', b'a', b'l',
            b'u', b'e', b'1', //
            // Subscription identifier
            11, 12, //
            // Content type
            3, 0, 4, b't', b'e', b's', b't', //
            // payload
            b'p', b'a', b'y', b'l', b'o', b'a', b'd',
        ];
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        expected.re_calculate_fixed_header_length();
        assert_eq!(Publish::try_read_v5(header, &mut body).unwrap(), expected);
    }
}

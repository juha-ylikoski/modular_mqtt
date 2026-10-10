use bytes::{Buf, BufMut, Bytes};

use crate::{
    util::{extract_bytes, read_variable_len_int, variable_len_int_size, write_variable_len_int},
    version::PacketProperties,
    ControlPacketType, Error, IntoPayload, MalformedPacket, MqttV5_0_0, MqttVersion, Packet,
    PayloadFormat, Property, PropertyIdentifier, UserProperty,
};

use super::{
    fixed_header::{self, FixedHeader},
    util::{extract_str, write_str, MqttTopic, Qos, QosPacketIdentifier},
};

#[derive(Debug, Clone, PartialEq, Default)]
pub struct PublishProperties {
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
    subscription_identifier: Vec<u64>,
    /// UTF-8 Encoded String describing the content of the Will Message
    /// The value of the Content Type is defined by the sending and receiving application.
    content_type: Option<String>,
}

impl PacketProperties for PublishProperties {
    fn try_read(data: &mut Bytes) -> Result<Self, Error> {
        let mut properties = Self::default();
        let properties_len = read_variable_len_int(data)? as usize;

        if data.remaining() < properties_len {
            return Err(MalformedPacket::new("Packet too short to parse"));
        }

        let data = &mut data.split_to(properties_len);

        while data.has_remaining() {
            let property_identifier = read_variable_len_int(data)?;
            let property_identifier = PropertyIdentifier::try_from(property_identifier)?;
            match property_identifier {
                PropertyIdentifier::PayloadFormatIndicator => {
                    if properties.payload_format.is_some() {
                        return Err(Error::ProtocolError(
                            "PayloadFormatIndicator specified multiple times",
                        ));
                    }
                    properties.payload_format = Some(match data.try_get_u8()? {
                        0 => PayloadFormat::Binary,
                        1 => PayloadFormat::Utf8,
                        _ => return Err(MalformedPacket::new("Invalid payload format")),
                    });
                }
                PropertyIdentifier::MessageExpiryInterval => {
                    if properties.message_expiry_interval.is_some() {
                        return Err(Error::ProtocolError(
                            "MessageExpiryInterval specified multiple times",
                        ));
                    }
                    properties.message_expiry_interval = Some(data.try_get_u32()?);
                }
                PropertyIdentifier::TopicAlias => {
                    if properties.topic_alias.is_some() {
                        return Err(Error::ProtocolError("TopicAlias specified multiple times"));
                    }
                    properties.topic_alias = Some(data.try_get_u16()?);
                }
                PropertyIdentifier::ResponseTopic => {
                    if properties.response_topic.is_some() {
                        return Err(Error::ProtocolError(
                            "ResponseTopic specified multiple times",
                        ));
                    }
                    properties.response_topic = Some(extract_str(data)?);
                }
                PropertyIdentifier::CorrelationData => {
                    if !properties.correlation_data.is_empty() {
                        return Err(Error::ProtocolError(
                            "CorrelationData specified multiple times",
                        ));
                    }
                    properties.correlation_data = extract_bytes(data)?;
                }
                PropertyIdentifier::UserProperty => {
                    let key = extract_str(data)?;
                    let value = extract_str(data)?;
                    properties.user_property.push(UserProperty { key, value });
                }
                PropertyIdentifier::SubscriptionIdentifier => {
                    let id = read_variable_len_int(data)?;
                    if id == 0 {
                        return Err(Error::ProtocolError("Subscription identifier cannot be 0"));
                    }

                    properties.subscription_identifier.push(id);
                }
                PropertyIdentifier::ContentType => {
                    if properties.content_type.is_some() {
                        return Err(Error::ProtocolError("ContentType specified multiple times"));
                    }
                    properties.content_type = Some(extract_str(data)?);
                }
                _ => {
                    return Err(MalformedPacket::new(
                        "Received unexpected property for connect",
                    ))
                }
            }
        }
        properties.subscription_identifier.sort();
        properties.subscription_identifier.dedup();
        Ok(properties)
    }

    fn write_properties(&self, buf: &mut impl BufMut) {
        let properties_len = self.properties_len();
        write_variable_len_int(properties_len as u64, buf);

        self.payload_format
            .serialize(PropertyIdentifier::PayloadFormatIndicator, buf);
        self.message_expiry_interval
            .serialize(PropertyIdentifier::MessageExpiryInterval, buf);
        self.topic_alias
            .serialize(PropertyIdentifier::TopicAlias, buf);
        self.response_topic
            .serialize(PropertyIdentifier::ResponseTopic, buf);
        self.correlation_data
            .serialize(PropertyIdentifier::CorrelationData, buf);
        self.user_property
            .serialize(PropertyIdentifier::UserProperty, buf);
        for id in &self.subscription_identifier {
            write_variable_len_int(PropertyIdentifier::SubscriptionIdentifier as u64, buf);
            write_variable_len_int(*id, buf);
        }
        self.content_type
            .serialize(PropertyIdentifier::ContentType, buf);
    }

    fn properties_len(&self) -> usize {
        self.payload_format.property_len()
            + self.message_expiry_interval.property_len()
            + self.topic_alias.property_len()
            + self.response_topic.property_len()
            + self.correlation_data.property_len()
            + self.user_property.property_len()
            + self
                .subscription_identifier
                .iter()
                .map(|id| 1 + variable_len_int_size(*id as usize))
                .sum::<usize>()
            + self.content_type.property_len()
    }
}

#[derive(Debug, Clone, PartialEq)]
/// A PUBLISH Control Packet is sent from a Client to a Server or from Server to a Client to transport an Application Message.
pub struct Publish<V: MqttVersion, Q> {
    qos: Q,
    retain: bool,
    dup: bool,
    topic: String,
    payload: Bytes,
    properties: V::PublishProperties,
}

impl<V: MqttVersion, Q> Publish<V, Q> {
    pub fn dup(&self) -> bool {
        self.dup
    }
    pub fn retain(&self) -> bool {
        self.retain
    }
}

impl<V: MqttVersion> Publish<V, Qos> {
    pub fn new(topic: MqttTopic, payload: impl IntoPayload, qos: Qos, retain: bool) -> Self {
        let payload = payload.into_payload();
        let topic = topic.0;

        Self {
            qos,
            retain,
            dup: false,
            topic,
            payload,
            properties: V::PublishProperties::default(),
        }
    }
    pub fn assign_packet_identifier(
        self,
        id: impl FnOnce() -> u16,
        dup: bool,
    ) -> Publish<V, QosPacketIdentifier> {
        Publish {
            qos: match self.qos {
                Qos::AtMostOnce => QosPacketIdentifier::AtMostOnce,
                Qos::AtLeastOnce => QosPacketIdentifier::AtLeastOnce(id()),
                Qos::ExactlyOnce => QosPacketIdentifier::ExactlyOnce(id()),
            },
            retain: self.retain,
            dup,
            topic: self.topic,
            payload: self.payload,
            properties: self.properties,
        }
    }
    pub fn qos(&self) -> Qos {
        self.qos
    }
}

impl<V: MqttVersion> Packet for Publish<V, QosPacketIdentifier> {
    fn write_to_buf(&self, buf: &mut impl BufMut) {
        let remaining_length = 2
            + self.topic.len()
            + {
                if matches!(
                    self.qos,
                    QosPacketIdentifier::AtLeastOnce(_) | QosPacketIdentifier::ExactlyOnce(_)
                ) {
                    assert!(self.packet_identifier().is_some());
                    2
                } else {
                    assert!(self.packet_identifier().is_none());
                    0
                }
            }
            + self.properties.properties_block_len()
            + self.payload.len();

        let fixed_header = FixedHeader::new(
            super::fixed_header::ControlPacketType::Publish {
                dup: self.dup,
                qos: self.qos.into(),
                retain: self.retain,
            },
            remaining_length,
        );
        fixed_header.write_to_buf(buf);

        write_str(&self.topic, buf);
        match self.qos {
            QosPacketIdentifier::AtMostOnce => (),
            QosPacketIdentifier::AtLeastOnce(packet_identifier)
            | QosPacketIdentifier::ExactlyOnce(packet_identifier) => buf.put_u16(packet_identifier),
        }
        self.properties.write_properties(buf);
        buf.put(&self.payload[..]);
    }

    fn try_read(header: FixedHeader, data: &mut Bytes) -> Result<Self, Error> {
        assert!(matches!(
            header.control_packet_type,
            ControlPacketType::Publish { .. }
        ));

        let topic = extract_str(data)?.to_string();

        let (dup, qos, retain) = match header.control_packet_type {
            fixed_header::ControlPacketType::Publish { dup, qos, retain } => (dup, qos, retain),
            _ => unreachable!(),
        };

        let qos = match qos {
            Qos::AtMostOnce => QosPacketIdentifier::AtMostOnce,
            Qos::AtLeastOnce => QosPacketIdentifier::AtLeastOnce(data.try_get_u16()?),
            Qos::ExactlyOnce => QosPacketIdentifier::ExactlyOnce(data.try_get_u16()?),
        };

        let properties = V::PublishProperties::try_read(data)?;

        let payload = data.clone();
        data.advance(payload.len());

        Ok(Self {
            topic,
            qos,
            retain,
            dup,
            payload,
            properties,
        })
    }
}

impl<V: MqttVersion> Publish<V, QosPacketIdentifier> {
    pub fn qos(&self) -> Qos {
        self.qos.into()
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn packet_identifier(&self) -> Option<u16> {
        match self.qos {
            QosPacketIdentifier::AtMostOnce => None,
            QosPacketIdentifier::AtLeastOnce(packet_identifier)
            | QosPacketIdentifier::ExactlyOnce(packet_identifier) => Some(packet_identifier),
        }
    }

    pub fn payload(&self) -> &Bytes {
        &self.payload
    }
}

impl<Q> Publish<MqttV5_0_0, Q> {
    pub fn payload_format(&self) -> Option<PayloadFormat> {
        self.properties.payload_format
    }

    pub fn message_expiry_interval(&self) -> Option<u32> {
        self.properties.message_expiry_interval
    }

    pub fn topic_alias(&self) -> Option<u16> {
        self.properties.topic_alias
    }

    pub fn response_topic(&self) -> Option<&str> {
        self.properties.response_topic.as_deref()
    }

    pub fn correlation_data(&self) -> Bytes {
        self.properties.correlation_data.clone()
    }

    pub fn user_property(&self) -> &[UserProperty] {
        &self.properties.user_property
    }

    pub fn subscription_identifier(&self) -> &[u64] {
        &self.properties.subscription_identifier
    }

    pub fn content_type(&self) -> Option<&str> {
        self.properties.content_type.as_deref()
    }
}

impl Publish<MqttV5_0_0, Qos> {
    pub fn set_payload_format(mut self, value: PayloadFormat) -> Self {
        self.properties.payload_format = Some(value);
        self
    }

    pub fn set_message_expiry_interval(mut self, interval: u32) -> Self {
        self.properties.message_expiry_interval = Some(interval);
        self
    }

    pub fn set_response_topic(mut self, topic: String) -> Self {
        self.properties.response_topic = Some(topic);
        self
    }
    pub fn set_topic_alias(mut self, alias: u16) -> Self {
        self.properties.topic_alias = Some(alias);
        self
    }
    pub fn set_correlation_data(mut self, data: Bytes) -> Self {
        self.properties.correlation_data = data;
        self
    }
    pub fn set_user_property(mut self, user_properties: Vec<UserProperty>) -> Self {
        self.properties.user_property = user_properties;
        self
    }
    pub fn set_subscription_identifier(mut self, mut identifiers: Vec<u64>) -> Result<Self, Error> {
        identifiers.sort();
        identifiers.dedup();
        if identifiers.contains(&0) {
            return Err(Error::ProtocolError(
                "Publish packet subscription identifier cannot be 0",
            ));
        }
        if identifiers.iter().any(|id| *id > 268435455) {
            return Err(Error::ProtocolError(
                "Publish packet cannot contain subscription id > 268435455",
            ));
        }
        self.properties.subscription_identifier = identifiers;
        Ok(self)
    }
    pub fn set_content_type(mut self, content_type: String) -> Self {
        self.properties.content_type = Some(content_type);
        self
    }
}

#[cfg(test)]
mod test_v3 {

    use bytes::BytesMut;

    use crate::MqttV3_1_1;

    use super::*;

    #[test]
    fn serialize() {
        let topic = MqttTopic::try_from("topic").unwrap();
        let msg = Publish::<MqttV3_1_1, Qos>::new(
            topic,
            Bytes::from_static(b"payload"),
            Qos::AtMostOnce,
            false,
        )
        .assign_packet_identifier(|| 0, false);
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
        let msg = Publish::<MqttV3_1_1, Qos>::new(
            topic,
            Bytes::from_static(b"foo"),
            Qos::AtMostOnce,
            false,
        )
        .assign_packet_identifier(|| 0, false);
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
        let msg = Publish::<MqttV3_1_1, Qos>::new(
            topic,
            Bytes::from_static(b"payload"),
            Qos::ExactlyOnce,
            false,
        )
        .assign_packet_identifier(|| 42, false);
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
        let msg = Publish::<MqttV3_1_1, Qos>::new(
            topic,
            Bytes::from_static(b"payload"),
            Qos::AtMostOnce,
            false,
        )
        .assign_packet_identifier(|| 0, true);
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
        let msg = Publish::<MqttV3_1_1, Qos>::new(
            topic,
            Bytes::from_static(b"payload"),
            Qos::AtMostOnce,
            true,
        )
        .assign_packet_identifier(|| 0, false);
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
        let expected = Publish::<MqttV3_1_1, Qos>::new(
            MqttTopic::try_from("topic").unwrap(),
            Bytes::from_static(b"payload"),
            Qos::AtMostOnce,
            false,
        )
        .assign_packet_identifier(|| 0, false);
        let msg = [
            48, 14, 0, 5, b't', b'o', b'p', b'i', b'c', b'p', b'a', b'y', b'l', b'o', b'a', b'd',
        ];
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(Publish::try_read(header, &mut body).unwrap(), expected);
    }
    #[test]
    fn deserialize_qos() {
        let expected = Publish::<MqttV3_1_1, Qos>::new(
            MqttTopic::try_from("topic").unwrap(),
            Bytes::from_static(b"payload"),
            Qos::ExactlyOnce,
            false,
        )
        .assign_packet_identifier(|| 42, false);
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
        assert_eq!(Publish::try_read(header, &mut body).unwrap(), expected);
    }
    #[test]
    fn deserialize_dup() {
        let expected = Publish::<MqttV3_1_1, Qos>::new(
            MqttTopic::try_from("topic").unwrap(),
            Bytes::from_static(b"payload"),
            Qos::AtMostOnce,
            false,
        )
        .assign_packet_identifier(|| 0, true);
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
        assert_eq!(Publish::try_read(header, &mut body).unwrap(), expected);
    }
    #[test]
    fn deserialize_retain() {
        let expected = Publish::<MqttV3_1_1, Qos>::new(
            MqttTopic::try_from("topic").unwrap(),
            Bytes::from_static(b"payload"),
            Qos::AtMostOnce,
            true,
        )
        .assign_packet_identifier(|| 0, false);
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
        assert_eq!(Publish::try_read(header, &mut body).unwrap(), expected);
    }
}

#[cfg(test)]
mod test_v5 {

    use bytes::BytesMut;

    use super::*;

    #[test]
    fn serialize() {
        let topic = MqttTopic::try_from("topic").unwrap();
        let msg = Publish::<MqttV5_0_0, Qos>::new(
            topic,
            Bytes::from_static(b"payload"),
            Qos::AtMostOnce,
            false,
        )
        .assign_packet_identifier(|| 0, false);
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
        let msg = Publish::<MqttV5_0_0, Qos>::new(
            topic,
            Bytes::from_static(b"foo"),
            Qos::AtMostOnce,
            false,
        )
        .assign_packet_identifier(|| 0, false);
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
        let msg = Publish::<MqttV5_0_0, Qos>::new(
            topic,
            Bytes::from_static(b"payload"),
            Qos::ExactlyOnce,
            false,
        )
        .assign_packet_identifier(|| 42, false);
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
        let msg = Publish::<MqttV5_0_0, Qos>::new(
            topic,
            Bytes::from_static(b"payload"),
            Qos::AtMostOnce,
            false,
        )
        .assign_packet_identifier(|| 0, true);
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
        let msg = Publish::<MqttV5_0_0, Qos>::new(
            topic,
            Bytes::from_static(b"payload"),
            Qos::AtMostOnce,
            true,
        )
        .assign_packet_identifier(|| 0, false);
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
        let expected = Publish::<MqttV5_0_0, Qos>::new(
            MqttTopic::try_from("topic").unwrap(),
            Bytes::from_static(b"payload"),
            Qos::AtMostOnce,
            false,
        )
        .assign_packet_identifier(|| 0, false);
        let msg = [
            48, 15, 0, 5, b't', b'o', b'p', b'i', b'c', 0, b'p', b'a', b'y', b'l', b'o', b'a', b'd',
        ];
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(Publish::try_read(header, &mut body).unwrap(), expected);
    }
    #[test]
    fn deserialize_qos() {
        let expected = Publish::<MqttV5_0_0, Qos>::new(
            MqttTopic::try_from("topic").unwrap(),
            Bytes::from_static(b"payload"),
            Qos::ExactlyOnce,
            false,
        )
        .assign_packet_identifier(|| 42, false);
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
        assert_eq!(Publish::try_read(header, &mut body).unwrap(), expected);
    }
    #[test]
    fn deserialize_dup() {
        let expected = Publish::<MqttV5_0_0, Qos>::new(
            MqttTopic::try_from("topic").unwrap(),
            Bytes::from_static(b"payload"),
            Qos::AtMostOnce,
            false,
        )
        .assign_packet_identifier(|| 0, true);
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
        assert_eq!(Publish::try_read(header, &mut body).unwrap(), expected);
    }
    #[test]
    fn deserialize_retain() {
        let expected = Publish::<MqttV5_0_0, Qos>::new(
            MqttTopic::try_from("topic").unwrap(),
            Bytes::from_static(b"payload"),
            Qos::AtMostOnce,
            true,
        )
        .assign_packet_identifier(|| 42, false);
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
        assert_eq!(Publish::try_read(header, &mut body).unwrap(), expected);
    }

    #[test]
    fn serialize_properties() {
        let topic = MqttTopic::try_from("topic").unwrap();
        let msg = Publish::new(
            topic,
            Bytes::from_static(b"payload"),
            Qos::AtMostOnce,
            false,
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
        .set_subscription_identifier(vec![12])
        .unwrap()
        .set_content_type("test".to_string())
        .assign_packet_identifier(|| 0, false);
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
        let mut expected = Publish::<MqttV5_0_0, Qos>::new(
            MqttTopic::try_from("topic").unwrap(),
            Bytes::from_static(b"payload"),
            Qos::AtMostOnce,
            false,
        )
        .assign_packet_identifier(|| 0, false);

        expected.properties.payload_format = Some(PayloadFormat::Binary);
        expected.properties.message_expiry_interval = Some(10);
        expected.properties.topic_alias = Some(11);
        expected.properties.response_topic = Some("response".to_string());
        expected.properties.correlation_data = Bytes::from_static(b"badcafee");
        expected.properties.user_property = vec![
            UserProperty {
                key: "property0".to_string(),
                value: "value0".to_string(),
            },
            UserProperty {
                key: "property1".to_string(),
                value: "value1".to_string(),
            },
        ];
        expected.properties.subscription_identifier = vec![12, 13];
        expected.properties.content_type = Some("test".to_string());

        let msg = [
            48, 98, 0, 5, b't', b'o', b'p', b'i', b'c', //
            // Properties
            83, //
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
            // Subscription identifier
            11, 13, //
            // Content type
            3, 0, 4, b't', b'e', b's', b't', //
            // payload
            b'p', b'a', b'y', b'l', b'o', b'a', b'd',
        ];
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(Publish::try_read(header, &mut body).unwrap(), expected);
    }

    #[test]
    fn deserialize_duplicate_subscription_ids() {
        let mut expected = Publish::<MqttV5_0_0, Qos>::new(
            MqttTopic::try_from("topic").unwrap(),
            Bytes::from_static(b"payload"),
            Qos::AtMostOnce,
            false,
        )
        .assign_packet_identifier(|| 0, false);

        expected.properties.subscription_identifier = vec![12];

        let msg = [
            48, 19, 0, 5, b't', b'o', b'p', b'i', b'c', //
            // Properties
            4, //
            // Subscription identifier
            11, 12, //
            // Subscription identifier
            11, 12, //
            // payload
            b'p', b'a', b'y', b'l', b'o', b'a', b'd',
        ];
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(
            Publish::<MqttV5_0_0, QosPacketIdentifier>::try_read(header, &mut body).unwrap(),
            expected
        );
    }

    #[test]
    fn deserialize_subscription_id_zero() {
        let msg = [
            48, 17, 0, 5, b't', b'o', b'p', b'i', b'c', //
            // Properties
            2, //
            // Subscription identifier
            11, 0, //
            // payload
            b'p', b'a', b'y', b'l', b'o', b'a', b'd',
        ];
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        match Publish::<MqttV5_0_0, QosPacketIdentifier>::try_read(header, &mut body) {
            Ok(_) => panic!("Should be protocol error"),
            Err(Error::ProtocolError(error)) => {
                assert_eq!(error, "Subscription identifier cannot be 0")
            }
            _ => panic!("Should be protocol error"),
        }
    }

    #[test]
    fn serialize_subscription_id_zero() {
        match Publish::<MqttV5_0_0, Qos>::new(
            MqttTopic::try_from("topic").unwrap(),
            Bytes::from_static(b"payload"),
            Qos::AtMostOnce,
            false,
        )
        .set_subscription_identifier(vec![0, 1])
        {
            Ok(_) => panic!("Should be protocol error"),
            Err(Error::ProtocolError(error)) => {
                assert_eq!(error, "Publish packet subscription identifier cannot be 0")
            }
            _ => panic!("Should be protocol error"),
        };
    }

    #[test]
    fn serialize_subscription_id_too_large() {
        match Publish::<MqttV5_0_0, Qos>::new(
            MqttTopic::try_from("topic").unwrap(),
            Bytes::from_static(b"payload"),
            Qos::AtMostOnce,
            false,
        )
        .set_subscription_identifier(vec![0xffffffffffffffff])
        {
            Ok(_) => panic!("Should be protocol error"),
            Err(Error::ProtocolError(error)) => {
                assert_eq!(
                    error,
                    "Publish packet cannot contain subscription id > 268435455"
                )
            }
            _ => panic!("Should be protocol error"),
        };
    }

    #[test]
    fn subscription_ids_round_trip() {
        let msg = Publish::<MqttV5_0_0, Qos>::new(
            MqttTopic::try_from("topic").unwrap(),
            Bytes::from_static(b"payload"),
            Qos::AtMostOnce,
            false,
        )
        .set_subscription_identifier(vec![12, 13, 12])
        .unwrap() // duplicate on purpose
        .assign_packet_identifier(|| 0, false);

        let mut wire = Vec::new();
        msg.write_to_buf(&mut wire);

        let mut buf = BytesMut::from(&wire[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(
            Publish::<MqttV5_0_0, QosPacketIdentifier>::try_read(header, &mut body).unwrap(),
            msg
        );
    }
}

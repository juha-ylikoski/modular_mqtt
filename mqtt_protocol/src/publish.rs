use std::{io::Write, marker::PhantomData};

use crate::{
    util::{extract_bytes, read_variable_len_int, variable_len_int_size, write_variable_len_int},
    ControlPacketType, Error, MalformedPacket, MqttV3_1_1, MqttV5_0_0, PayloadFormat, Property,
    PropertyIdentifier, ReceivedUserProperty, UserProperty,
};

use super::{
    fixed_header::{self, FixedHeader},
    util::{extract_str, write_str, MqttTopic, Qos, QosPacketIdentifier},
};

#[derive(Debug, PartialEq, Default)]
pub struct Properties<'a> {
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
    response_topic: Option<&'a str>,
    /// The Correlation Data is used by the sender of the Request Message to identify which request
    /// the Response Message is for when it is received
    /// The value of the Correlation Data only has meaning to the sender of the Request Message
    /// and receiver of the Response Message.
    correlation_data: &'a [u8],
    user_property: &'a [UserProperty<'a>],
    /// The Subscription Identifier can have the value of 1 to 268,435,455. It is a Protocol Error if
    /// the Subscription Identifier has a value of 0. Multiple Subscription Identifiers will be included
    /// if the publication is the result of a match to more than one subscription, in this case their
    /// order is not significant.
    subscription_identifier: Option<u64>,
    /// UTF-8 Encoded String describing the content of the Will Message
    /// The value of the Content Type is defined by the sending and receiving application.
    content_type: Option<&'a str>,
}

#[derive(Debug, PartialEq, Default)]
pub struct ReceivedProperties {
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
    correlation_data: Vec<u8>,
    user_property: Vec<ReceivedUserProperty>,
    /// The Subscription Identifier can have the value of 1 to 268,435,455. It is a Protocol Error if
    /// the Subscription Identifier has a value of 0. Multiple Subscription Identifiers will be included
    /// if the publication is the result of a match to more than one subscription, in this case their
    /// order is not significant.
    subscription_identifier: Option<u64>,
    /// UTF-8 Encoded String describing the content of the Will Message
    /// The value of the Content Type is defined by the sending and receiving application.
    content_type: Option<String>,
}

#[derive(Debug, PartialEq)]
/// A PUBLISH Control Packet is sent from a Client to a Server or from Server to a Client to transport an Application Message.
pub struct Publish<'a, V> {
    pub fixed_header: FixedHeader,
    pub topic: &'a str,
    pub packet_identifier: Option<u16>,
    pub payload: &'a [u8],
    version: PhantomData<V>,
    /// Mqtt v5 properties
    ///
    /// if message is v3, this is None
    pub properties: Option<Properties<'a>>,
}

#[derive(Debug, PartialEq)]
/// A PUBLISH Control Packet is sent from a Client to a Server or from Server to a Client to transport an Application Message.
pub struct ReceivedMessage<V> {
    pub flags: u8,
    pub topic: String,
    pub packet_identifier: Option<u16>,
    pub payload: Vec<u8>,
    version: PhantomData<V>,

    /// Mqtt v5 properties
    ///
    /// if message is v3, this is None
    pub properties: Option<ReceivedProperties>,
}

impl<'a, V> Publish<'a, V> {
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
        if self.properties.is_some() {
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

    fn properties_len(&self) -> usize {
        if let Some(properties) = self.properties.as_ref() {
            properties.payload_format.property_len()
                + properties.message_expiry_interval.property_len()
                + properties.topic_alias.property_len()
                + properties.response_topic.property_len()
                + properties.correlation_data.property_len()
                + properties.user_property.property_len()
                + properties
                    .subscription_identifier
                    .map(|id| 1 + variable_len_int_size(id as usize))
                    .unwrap_or_default()
                + properties.content_type.property_len()
        } else {
            0
        }
    }

    fn write_properties(&self, writer: &mut impl Write) -> Result<usize, std::io::Error> {
        if let Some(properties) = self.properties.as_ref() {
            let mut length = 0;
            let properties_len = self.properties_len();
            println!("properties len: {properties_len}");
            length += write_variable_len_int(properties_len as u64, writer)?;

            fn add_subscription_identifier(
                subscription_identifier: Option<u64>,
                writer: &mut impl Write,
            ) -> Result<usize, std::io::Error> {
                if let Some(id) = subscription_identifier {
                    Ok(write_variable_len_int(
                        PropertyIdentifier::SubscriptionIdentifier as u64,
                        writer,
                    )? + write_variable_len_int(id, writer)?)
                } else {
                    Ok(0)
                }
            }

            length += properties
                .payload_format
                .serialize(PropertyIdentifier::PayloadFormatIndicator, writer)?
                + properties
                    .message_expiry_interval
                    .serialize(PropertyIdentifier::MessageExpiryInterval, writer)?
                + properties
                    .topic_alias
                    .serialize(PropertyIdentifier::TopicAlias, writer)?
                + properties
                    .response_topic
                    .serialize(PropertyIdentifier::ResponseTopic, writer)?
                + properties
                    .correlation_data
                    .serialize(PropertyIdentifier::CorrelationData, writer)?
                + properties
                    .user_property
                    .serialize(PropertyIdentifier::UserProperty, writer)?
                + add_subscription_identifier(properties.subscription_identifier, writer)?
                + properties
                    .content_type
                    .serialize(PropertyIdentifier::ContentType, writer)?;

            Ok(length)
        } else {
            Ok(0)
        }
    }

    pub fn write_to_stream(mut self, writer: &mut impl Write) -> Result<usize, std::io::Error> {
        self.re_calculate_fixed_header_length();
        let mut length = self.fixed_header.write_to_stream(writer)?;
        length += write_str(self.topic, writer)?;
        if let Some(packet_identifier) = self.packet_identifier {
            writer.write_all(&[
                ((packet_identifier & 0xff00) >> 8) as u8,
                (packet_identifier & 0xff) as u8,
            ])?;
            length += 2;
        }
        length += self.write_properties(writer)?;
        length += self.payload.len();
        writer.write_all(self.payload)?;
        writer.flush()?;

        Ok(length)
    }
}

impl<'a> Publish<'a, MqttV3_1_1> {
    pub fn new_v3(
        dup: bool,
        qos: QosPacketIdentifier,
        retain: bool,
        topic: &'a MqttTopic,
        payload: &'a [u8],
    ) -> Self {
        let topic = topic.0.as_str();
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
            version: PhantomData,
            properties: None,
        }
    }
}

impl<'a> Publish<'a, MqttV5_0_0> {
    pub fn new_v5(
        dup: bool,
        qos: QosPacketIdentifier,
        retain: bool,
        topic: &'a MqttTopic,
        payload: &'a [u8],
    ) -> Self {
        let topic = topic.0.as_str();
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
            version: PhantomData,
            properties: Some(Properties::default()),
        }
    }
    pub fn set_payload_format(mut self, value: PayloadFormat) -> Self {
        self.properties.as_mut().unwrap().payload_format = Some(value);
        self
    }
    pub fn payload_format(&self) -> Option<PayloadFormat> {
        self.properties.as_ref().unwrap().payload_format
    }

    pub fn set_message_expiry_interval(mut self, interval: u32) -> Self {
        self.properties.as_mut().unwrap().message_expiry_interval = Some(interval);
        self
    }
    pub fn message_expiry_interval(&self) -> Option<u32> {
        self.properties.as_ref().unwrap().message_expiry_interval
    }

    pub fn set_topic_alias(mut self, alias: u16) -> Self {
        self.properties.as_mut().unwrap().topic_alias = Some(alias);
        self
    }
    pub fn topic_alias(&self) -> Option<u16> {
        self.properties.as_ref().unwrap().topic_alias
    }

    pub fn set_response_topic(mut self, response_topic: &'a str) -> Self {
        self.properties.as_mut().unwrap().response_topic = Some(response_topic);
        self
    }
    pub fn response_topic(&self) -> Option<&str> {
        self.properties.as_ref().unwrap().response_topic
    }

    pub fn set_correlation_data(mut self, correlation_data: &'a [u8]) -> Self {
        self.properties.as_mut().unwrap().correlation_data = correlation_data;
        self
    }
    pub fn correlation_data(&self) -> &[u8] {
        self.properties.as_ref().unwrap().correlation_data
    }

    pub fn set_user_property(mut self, user_property: &'a [UserProperty<'a>]) -> Self {
        self.properties.as_mut().unwrap().user_property = user_property;
        self
    }
    pub fn user_property(&self) -> &[UserProperty<'a>] {
        self.properties.as_ref().unwrap().user_property
    }

    pub fn set_subscription_identifier(mut self, value: u64) -> Self {
        self.properties.as_mut().unwrap().subscription_identifier = Some(value);
        self
    }
    pub fn subscription_identifier(&self) -> Option<u64> {
        self.properties.as_ref().unwrap().subscription_identifier
    }

    pub fn set_content_type(mut self, content_type: &'a str) -> Self {
        self.properties.as_mut().unwrap().content_type = Some(content_type);
        self
    }
    pub fn content_type(&self) -> Option<&str> {
        self.properties.as_ref().unwrap().content_type
    }
}

impl<V> ReceivedMessage<V> {
    pub fn dup(&self) -> bool {
        ControlPacketType::flags_dup(self.flags)
    }
    pub fn retain(&self) -> bool {
        ControlPacketType::flags_retain(self.flags)
    }
    pub fn qos(&self) -> Qos {
        ControlPacketType::flags_qos(self.flags)
            .expect("Internal flags were not what they were supposed to be. This is a bug.")
    }
}

impl ReceivedMessage<MqttV3_1_1> {
    pub fn new_v3(
        dup: bool,
        qos: Qos,
        retain: bool,
        topic: String,
        packet_identifier: Option<u16>,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            flags: ControlPacketType::Publish { dup, qos, retain }.flags(),
            topic,
            packet_identifier,
            payload,
            version: PhantomData,
            properties: None,
        }
    }
    pub fn try_read_v3(header: FixedHeader, mut data: Vec<u8>) -> Result<Self, Error> {
        let flags = header.control_packet_type.flags();
        if data.len() < 2 {
            return Err(MalformedPacket::new("Packet too short to parse"));
        }
        let topic = extract_str(&data[..])?.to_string();
        let mut pos = 2 + topic.len();

        let (_, qos, _) = match header.control_packet_type {
            fixed_header::ControlPacketType::Publish { dup, qos, retain } => (dup, qos, retain),
            _ => panic!("Library had internal error. This is bug!"),
        };

        let packet_identifier = if qos != Qos::AtMostOnce {
            let id = Some(u16::from_be_bytes([data[pos], data[pos + 1]]));
            pos += 2;
            id
        } else {
            None
        };

        // Split data before to be able to do split_off which requires &mut
        data.drain(0..pos);
        let payload = data;

        Ok(Self {
            flags,
            topic,
            packet_identifier,
            payload,
            version: PhantomData,
            properties: None,
        })
    }
}

impl ReceivedMessage<MqttV5_0_0> {
    pub fn new_v5(
        dup: bool,
        qos: Qos,
        retain: bool,
        topic: String,
        packet_identifier: Option<u16>,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            flags: ControlPacketType::Publish { dup, qos, retain }.flags(),
            topic,
            packet_identifier,
            payload,
            version: PhantomData,
            properties: Some(ReceivedProperties::default()),
        }
    }

    fn read_property(&mut self, data: &[u8]) -> Result<usize, Error> {
        println!("self: {self:#?}");
        let (i, property_identifier) = read_variable_len_int(data)?;
        let property_identifier = PropertyIdentifier::try_from(property_identifier)?;

        let properties = self.properties.as_mut().unwrap();

        println!("property: {property_identifier:?}");
        match property_identifier {
            PropertyIdentifier::PayloadFormatIndicator => {
                if properties.payload_format.is_some() {
                    return Err(Error::ProtocolError(
                        "PayloadFormatIndicator specified multiple times",
                    ));
                }
                if data[i..i + 1].is_empty() {
                    Err(MalformedPacket::new("Packet too short to read property"))
                } else {
                    properties.payload_format = Some(match data[i] {
                        0 => PayloadFormat::Binary,
                        1 => PayloadFormat::Utf8,
                        _ => return Err(MalformedPacket::new("Invalid payload format")),
                    });
                    Ok(1)
                }
            }
            PropertyIdentifier::MessageExpiryInterval => {
                if properties.message_expiry_interval.is_some() {
                    return Err(Error::ProtocolError(
                        "MessageExpiryInterval specified multiple times",
                    ));
                }
                properties.message_expiry_interval =
                    Some(u32::from_be_bytes(data[i..i + 4].try_into().map_err(
                        |_| MalformedPacket::new("Packet too short to read property"),
                    )?));
                Ok(4)
            }
            PropertyIdentifier::TopicAlias => {
                if properties.topic_alias.is_some() {
                    return Err(Error::ProtocolError("TopicAlias specified multiple times"));
                }
                properties.topic_alias =
                    Some(u16::from_be_bytes(data[i..i + 2].try_into().map_err(
                        |_| MalformedPacket::new("Packet too short to read property"),
                    )?));
                Ok(2)
            }
            PropertyIdentifier::ResponseTopic => {
                if properties.response_topic.is_some() {
                    return Err(Error::ProtocolError(
                        "ResponseTopic specified multiple times",
                    ));
                }
                let response_topic = extract_str(&data[i..])?;
                let len = 2 + response_topic.len();
                properties.response_topic = Some(response_topic.to_string());
                Ok(len)
            }
            PropertyIdentifier::CorrelationData => {
                if !properties.correlation_data.is_empty() {
                    return Err(Error::ProtocolError(
                        "CorrelationData specified multiple times",
                    ));
                }
                let correlation_data = extract_bytes(&data[i..])?;
                let len = 2 + correlation_data.len();
                properties.correlation_data = correlation_data.to_vec();
                Ok(len)
            }
            PropertyIdentifier::UserProperty => {
                let key = extract_str(&data[i..])?.to_string();
                let value = extract_str(&data[i + 2 + key.len()..])?.to_string();
                let len = 2 + key.len() + 2 + value.len();
                properties
                    .user_property
                    .push(ReceivedUserProperty { key, value });
                Ok(len)
            }
            PropertyIdentifier::SubscriptionIdentifier => {
                if properties.subscription_identifier.is_some() {
                    return Err(Error::ProtocolError(
                        "SubscriptionIdentifier specified multiple times",
                    ));
                }
                let (l, v) = read_variable_len_int(&data[i..])?;
                properties.subscription_identifier = Some(v);
                Ok(l)
            }
            PropertyIdentifier::ContentType => {
                if properties.content_type.is_some() {
                    return Err(Error::ProtocolError("ContentType specified multiple times"));
                }
                let content_type = extract_str(&data[i..])?;
                let len = 2 + content_type.len();
                properties.content_type = Some(content_type.to_string());
                Ok(len)
            }
            _ => Err(MalformedPacket::new(
                "Received unexpected property for connect",
            )),
        }
        .map(|len| i + len)
    }

    pub fn try_read_v5(header: FixedHeader, mut data: Vec<u8>) -> Result<Self, Error> {
        let flags = header.control_packet_type.flags();
        if data.len() < 2 {
            return Err(MalformedPacket::new("Packet too short to parse"));
        }
        let topic = extract_str(&data[..])?.to_string();
        let mut pos = 2 + topic.len();

        let (_, qos, _) = match header.control_packet_type {
            fixed_header::ControlPacketType::Publish { dup, qos, retain } => (dup, qos, retain),
            _ => panic!("Library had internal error. This is bug!"),
        };

        let packet_identifier = if qos != Qos::AtMostOnce {
            let id = Some(u16::from_be_bytes([data[pos], data[pos + 1]]));
            pos += 2;
            id
        } else {
            None
        };

        let (int_len, properties_len) = read_variable_len_int(&data[pos..])?;
        pos += int_len;
        let properties_end = pos + properties_len as usize;

        let mut publish = Self {
            flags,
            topic,
            packet_identifier,
            payload: Vec::new(),
            version: PhantomData,
            properties: Some(ReceivedProperties::default()),
        };

        while pos < properties_end {
            pos += publish.read_property(&data[pos..])?;
        }

        // Split data before to be able to do split_off which requires &mut
        data.drain(0..pos);
        publish.payload = data;

        Ok(publish)
    }

    pub fn payload_format(&self) -> Option<PayloadFormat> {
        self.properties.as_ref().unwrap().payload_format
    }

    pub fn message_expiry_interval(&self) -> Option<u32> {
        self.properties.as_ref().unwrap().message_expiry_interval
    }

    pub fn topic_alias(&self) -> Option<u16> {
        self.properties.as_ref().unwrap().topic_alias
    }

    pub fn response_topic(&self) -> Option<&str> {
        self.properties.as_ref().unwrap().response_topic.as_deref()
    }

    pub fn correlation_data(&self) -> &[u8] {
        self.properties.as_ref().unwrap().correlation_data.as_ref()
    }

    pub fn user_property(&self) -> &[ReceivedUserProperty] {
        self.properties.as_ref().unwrap().user_property.as_ref()
    }

    pub fn subscription_identifier(&self) -> Option<u64> {
        self.properties.as_ref().unwrap().subscription_identifier
    }

    pub fn content_type(&self) -> Option<&str> {
        self.properties.as_ref().unwrap().content_type.as_deref()
    }
}

#[cfg(test)]
mod test_v3 {
    use std::io::{BufReader, BufWriter, Read};

    use super::*;

    #[test]
    fn serialize() {
        let topic = MqttTopic::try_from("topic").unwrap();
        let msg = Publish::new_v3(
            false,
            QosPacketIdentifier::AtMostOnce,
            false,
            &topic,
            b"payload",
        );
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
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
        let msg = Publish::new_v3(
            false,
            QosPacketIdentifier::AtMostOnce,
            false,
            &topic,
            b"foo",
        );
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
        assert_eq!(
            &buf,
            &[48, 9, 0, 4, b'f', b'o', b'o', b'2', b'f', b'o', b'o']
        );
    }
    #[test]
    fn serialize_qos() {
        let topic = MqttTopic::try_from("topic").unwrap();
        let msg = Publish::new_v3(
            false,
            QosPacketIdentifier::ExactlyOnce(42),
            false,
            &topic,
            b"payload",
        );
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
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
        let msg = Publish::new_v3(
            true,
            QosPacketIdentifier::AtMostOnce,
            false,
            &topic,
            b"payload",
        );
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
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
        let msg = Publish::new_v3(
            false,
            QosPacketIdentifier::AtMostOnce,
            true,
            &topic,
            b"payload",
        );
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
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
        let expected = ReceivedMessage::new_v3(
            false,
            Qos::AtMostOnce,
            false,
            "topic".to_string(),
            None,
            b"payload".to_vec(),
        );
        let msg = [
            48, 14, 0, 5, b't', b'o', b'p', b'i', b'c', b'p', b'a', b'y', b'l', b'o', b'a', b'd',
        ];
        let mut reader = BufReader::new(&msg[..]);
        let header = FixedHeader::try_read_sync(&mut reader).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(
            ReceivedMessage::try_read_v3(header, data).unwrap(),
            expected
        );
    }
    #[test]
    fn deserialize_qos() {
        let expected = ReceivedMessage::new_v3(
            false,
            Qos::ExactlyOnce,
            false,
            "topic".to_string(),
            Some(42),
            b"payload".to_vec(),
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
        let mut reader = BufReader::new(&msg[..]);
        let header = FixedHeader::try_read_sync(&mut reader).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(
            ReceivedMessage::try_read_v3(header, data).unwrap(),
            expected
        );
    }
    #[test]
    fn deserialize_dup() {
        let expected = ReceivedMessage::new_v3(
            true,
            Qos::AtMostOnce,
            false,
            "topic".to_string(),
            None,
            b"payload".to_vec(),
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
        let mut reader = BufReader::new(&msg[..]);
        let header = FixedHeader::try_read_sync(&mut reader).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(
            ReceivedMessage::try_read_v3(header, data).unwrap(),
            expected
        );
    }
    #[test]
    fn deserialize_retain() {
        let expected = ReceivedMessage::new_v3(
            false,
            Qos::AtMostOnce,
            true,
            "topic".to_string(),
            None,
            b"payload".to_vec(),
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
        let mut reader = BufReader::new(&msg[..]);
        let header = FixedHeader::try_read_sync(&mut reader).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(
            ReceivedMessage::try_read_v3(header, data).unwrap(),
            expected
        );
    }
}

#[cfg(test)]
mod test_v5 {
    use std::io::{BufReader, BufWriter, Read};

    use super::*;

    #[test]
    fn serialize() {
        let topic = MqttTopic::try_from("topic").unwrap();
        let msg = Publish::new_v5(
            false,
            QosPacketIdentifier::AtMostOnce,
            false,
            &topic,
            b"payload",
        );
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
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
        let msg = Publish::new_v5(
            false,
            QosPacketIdentifier::AtMostOnce,
            false,
            &topic,
            b"foo",
        );
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
        assert_eq!(
            &buf,
            &[48, 10, 0, 4, b'f', b'o', b'o', b'2', 0, b'f', b'o', b'o']
        );
    }
    #[test]
    fn serialize_qos() {
        let topic = MqttTopic::try_from("topic").unwrap();
        let msg = Publish::new_v5(
            false,
            QosPacketIdentifier::ExactlyOnce(42),
            false,
            &topic,
            b"payload",
        );
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
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
        let msg = Publish::new_v5(
            true,
            QosPacketIdentifier::AtMostOnce,
            false,
            &topic,
            b"payload",
        );
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
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
        let msg = Publish::new_v5(
            false,
            QosPacketIdentifier::AtMostOnce,
            true,
            &topic,
            b"payload",
        );
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
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
        let expected = ReceivedMessage::new_v5(
            false,
            Qos::AtMostOnce,
            false,
            "topic".to_string(),
            None,
            b"payload".to_vec(),
        );
        let msg = [
            48, 15, 0, 5, b't', b'o', b'p', b'i', b'c', 0, b'p', b'a', b'y', b'l', b'o', b'a', b'd',
        ];
        let mut reader = BufReader::new(&msg[..]);
        let header = FixedHeader::try_read_sync(&mut reader).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(
            ReceivedMessage::try_read_v5(header, data).unwrap(),
            expected
        );
    }
    #[test]
    fn deserialize_qos() {
        let expected = ReceivedMessage::new_v5(
            false,
            Qos::ExactlyOnce,
            false,
            "topic".to_string(),
            Some(42),
            b"payload".to_vec(),
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
        let mut reader = BufReader::new(&msg[..]);
        let header = FixedHeader::try_read_sync(&mut reader).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(
            ReceivedMessage::try_read_v5(header, data).unwrap(),
            expected
        );
    }
    #[test]
    fn deserialize_dup() {
        let expected = ReceivedMessage::new_v5(
            true,
            Qos::AtMostOnce,
            false,
            "topic".to_string(),
            None,
            b"payload".to_vec(),
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
        let mut reader = BufReader::new(&msg[..]);
        let header = FixedHeader::try_read_sync(&mut reader).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(
            ReceivedMessage::try_read_v5(header, data).unwrap(),
            expected
        );
    }
    #[test]
    fn deserialize_retain() {
        let expected = ReceivedMessage::new_v5(
            false,
            Qos::AtMostOnce,
            true,
            "topic".to_string(),
            None,
            b"payload".to_vec(),
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
        let mut reader = BufReader::new(&msg[..]);
        let header = FixedHeader::try_read_sync(&mut reader).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(
            ReceivedMessage::try_read_v5(header, data).unwrap(),
            expected
        );
    }

    #[test]
    fn serialize_properties() {
        let topic = MqttTopic::try_from("topic").unwrap();
        let msg = Publish::new_v5(
            false,
            QosPacketIdentifier::AtMostOnce,
            false,
            &topic,
            b"payload",
        )
        .set_payload_format(PayloadFormat::Binary)
        .set_message_expiry_interval(10)
        .set_topic_alias(11)
        .set_response_topic("response")
        .set_correlation_data(b"badcafee")
        .set_user_property(&[
            UserProperty {
                key: "property0",
                value: "value0",
            },
            UserProperty {
                key: "property1",
                value: "value1",
            },
        ])
        .set_subscription_identifier(12)
        .set_content_type("test");
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
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
        let mut expected = ReceivedMessage::new_v5(
            false,
            Qos::AtMostOnce,
            false,
            "topic".to_string(),
            None,
            b"payload".to_vec(),
        );

        expected.properties.as_mut().unwrap().payload_format = Some(PayloadFormat::Binary);
        expected
            .properties
            .as_mut()
            .unwrap()
            .message_expiry_interval = Some(10);
        expected.properties.as_mut().unwrap().topic_alias = Some(11);
        expected.properties.as_mut().unwrap().response_topic = Some("response".to_string());
        expected.properties.as_mut().unwrap().correlation_data = b"badcafee".to_vec();
        expected.properties.as_mut().unwrap().user_property = vec![
            ReceivedUserProperty {
                key: "property0".to_string(),
                value: "value0".to_string(),
            },
            ReceivedUserProperty {
                key: "property1".to_string(),
                value: "value1".to_string(),
            },
        ];
        expected
            .properties
            .as_mut()
            .unwrap()
            .subscription_identifier = Some(12);
        expected.properties.as_mut().unwrap().content_type = Some("test".to_string());
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
        let mut reader = BufReader::new(&msg[..]);
        let header = FixedHeader::try_read_sync(&mut reader).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(
            ReceivedMessage::try_read_v5(header, data).unwrap(),
            expected
        );
    }
}

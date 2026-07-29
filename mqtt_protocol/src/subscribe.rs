use bytes::{Buf, BufMut, Bytes};

use crate::{
    util::{read_variable_len_int, variable_len_int_size, write_variable_len_int},
    version::PacketProperties,
    ControlPacketType, Error, MalformedPacket, MqttTopic, MqttV5_0_0, MqttVersion, Packet,
    Property, PropertyIdentifier, UserProperty,
};

use super::{
    fixed_header::FixedHeader,
    util::{extract_str, write_str, Qos},
};

pub trait TopicSubscription: Sized + std::fmt::Debug + PartialEq + Send + Sync + Clone {
    fn new(topic: MqttTopic, qos: Qos) -> Self;
    fn try_from_byte(topic: String, options: u8) -> Result<Self, Error>;
    fn topic(&self) -> &str;
    fn options(&self) -> u8;
}

#[derive(Debug, Clone, PartialEq)]
pub struct TopicSubscriptionV3 {
    topic: MqttTopic,
    qos: Qos,
}

impl TopicSubscription for TopicSubscriptionV3 {
    fn new(topic: MqttTopic, qos: Qos) -> Self {
        Self { topic, qos }
    }
    fn try_from_byte(topic: String, options: u8) -> Result<Self, Error> {
        let qos = Qos::try_from(options)?;
        let topic = MqttTopic::try_from(topic)?;
        Ok(TopicSubscriptionV3::new(topic, qos))
    }
    fn topic(&self) -> &str {
        &self.topic.0
    }
    fn options(&self) -> u8 {
        self.qos as u8
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TopicSubscriptionV5 {
    qos: Qos,
    /// Bit 2 of the Subscription Options represents the No Local option. If the value is 1, Application Messages MUST NOT be forwarded to a connection with a ClientID equal to the ClientID of the publishing connection [MQTT-3.8.3-3]. It is a Protocol Error to set the No Local bit to 1 on a Shared Subscription [MQTT-3.8.3-4].
    no_local: bool,
    /// Bit 3 of the Subscription Options represents the Retain As Published option. If 1, Application Messages forwarded using this subscription keep the RETAIN flag they were published with. If 0, Application Messages forwarded using this subscription have the RETAIN flag set to 0. Retained messages sent when the subscription is established have the RETAIN flag set to 1.
    keep_retain: bool,
    retain_handling: RetainHandling,
    topic: MqttTopic,
}

impl TopicSubscriptionV5 {
    pub fn new(
        topic: MqttTopic,
        qos: Qos,
        no_local: bool,
        keep_retain: bool,
        retain_handling: RetainHandling,
    ) -> Self {
        Self {
            qos,
            no_local,
            keep_retain,
            retain_handling,
            topic,
        }
    }
}

impl TopicSubscription for TopicSubscriptionV5 {
    fn new(topic: MqttTopic, qos: Qos) -> Self {
        Self {
            qos,
            no_local: true,
            keep_retain: true,
            retain_handling: RetainHandling::SendAtSubscribe,
            topic,
        }
    }
    fn try_from_byte(topic: String, options: u8) -> Result<Self, Error> {
        let topic = MqttTopic::try_from(topic)?;
        let qos = Qos::try_from(options & 0b11)?;
        let no_local = (options & 0x4) == 0x4;
        let keep_retain = (options & 0x8) == 0x8;
        let retain_handling = RetainHandling::try_from((options & 0x30) >> 4)?;
        if options & 0xc0 != 0 {
            return Err(MalformedPacket::new("Invalid subscribe options"));
        }
        Ok(Self {
            qos,
            no_local,
            keep_retain,
            retain_handling,
            topic,
        })
    }
    fn topic(&self) -> &str {
        &self.topic.0
    }
    fn options(&self) -> u8 {
        self.qos as u8
            | ((self.no_local as u8) << 2)
            | ((self.keep_retain as u8) << 3)
            | ((self.retain_handling as u8) << 4)
    }
}

impl<T: TopicSubscription> PacketProperties for Vec<T> {
    fn try_read(data: &mut Bytes) -> Result<Self, Error> {
        let mut subscriptions = Vec::new();
        while data.has_remaining() {
            let topic = extract_str(data)?;
            let options = data.try_get_u8()?;
            subscriptions.push(T::try_from_byte(topic, options)?);
        }

        Ok(subscriptions)
    }

    fn write_properties(&self, buf: &mut impl BufMut) {
        for sub in self {
            write_str(sub.topic(), buf);
            buf.put_u8(sub.options());
        }
    }

    fn properties_block_len(&self) -> usize {
        self.iter().map(|sub| sub.topic().len() + 2 + 1).sum()
    }
    fn properties_len(&self) -> usize {
        unimplemented!()
    }
}
/// Bits 4 and 5 of the Subscription Options represent the Retain Handling option. This option specifies whether retained messages are sent when the subscription is established. This does not affect the sending of retained messages at any point after the subscribe. If there are no retained messages matching the Topic Filter, all of these values act the same. The values are:
///
/// 0 = Send retained messages at the time of the subscribe
/// 1 = Send retained messages at subscribe only if the subscription does not currently exist
/// 2 = Do not send retained messages at the time of the subscribe
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum RetainHandling {
    SendAtSubscribe = 0,
    SendIfSubDoesNotExist = 1,
    DontSendRetained = 2,
}

impl TryFrom<u8> for RetainHandling {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::SendAtSubscribe),
            1 => Ok(Self::SendIfSubDoesNotExist),
            2 => Ok(Self::DontSendRetained),
            _ => Err(Error::ProtocolError("Invalid RetainHandling")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct SubscribeOptions {
    /// Integer representing the identifier of the subscription. The Subscription Identifier can have the value of 1 to 268,435,455. It is a Protocol Error if the Subscription Identifier has a value of
    /// The Subscription Identifier is associated with any subscription created or modified as the result of this SUBSCRIBE packet. If there is a Subscription Identifier, it is stored with the subscription. If this property is not specified, then the absence of a Subscription Identifier is stored with the subscription.
    subscription_identifier: Option<u64>,
    user_property: Vec<UserProperty>,
}

impl PacketProperties for SubscribeOptions {
    fn try_read(data: &mut Bytes) -> Result<Self, Error> {
        let properties_len = read_variable_len_int(data)? as usize;

        let mut properties = Self::default();

        if data.remaining() < properties_len {
            return Err(MalformedPacket::new("Packet too short to parse"));
        }
        let data = &mut data.split_to(properties_len);

        while data.has_remaining() {
            let property_identifier = read_variable_len_int(data)?;
            let property_identifier = PropertyIdentifier::try_from(property_identifier)?;
            match property_identifier {
                PropertyIdentifier::SubscriptionIdentifier => {
                    if properties.subscription_identifier.is_some() {
                        return Err(Error::ProtocolError(
                            "SubscriptionIdentifier specified multiple times",
                        ));
                    }
                    properties.subscription_identifier = Some(read_variable_len_int(data)?);
                }
                PropertyIdentifier::UserProperty => {
                    let key = extract_str(data)?.to_string();
                    let value = extract_str(data)?.to_string();
                    properties.user_property.push(UserProperty { key, value });
                }
                _ => {
                    return Err(MalformedPacket::new(
                        "Received unexpected property for connect",
                    ))
                }
            }
        }
        Ok(properties)
    }

    fn write_properties(&self, buf: &mut impl BufMut) {
        let properties_len = self.properties_len();
        write_variable_len_int(properties_len as u64, buf);

        if let Some(id) = self.subscription_identifier {
            write_variable_len_int(PropertyIdentifier::SubscriptionIdentifier as u64, buf);
            write_variable_len_int(id, buf);
        }
        self.user_property
            .serialize(crate::PropertyIdentifier::UserProperty, buf);
    }

    fn properties_len(&self) -> usize {
        self.subscription_identifier
            .map(|v| variable_len_int_size(v as usize) + 1)
            .unwrap_or_default()
            + self.user_property.property_len()
    }
}

#[derive(Debug, Clone, PartialEq)]
/// The SUBSCRIBE Packet is sent from the Client to the Server to create one or more Subscriptions. Each Subscription registers a Client’s interest in one or more Topics. The Server sends PUBLISH Packets to the Client in order to forward Application Messages that were published to Topics that match these Subscriptions. The SUBSCRIBE Packet also specifies (for each Subscription) the maximum QoS with which the Server can send Application Messages to the Client.
pub struct Subscribe<V: MqttVersion> {
    packet_identifier: u16,
    subscriptions: Vec<V::TopicSubscription>,

    options: V::SubscribeData,
}

impl<V: MqttVersion> Packet for Subscribe<V> {
    fn write_to_buf(&self, buf: &mut impl BufMut) {
        let fixed_header = FixedHeader::new(
            super::fixed_header::ControlPacketType::Subscribe,
            2 + self.options.properties_block_len() + self.subscriptions.properties_block_len(),
        );
        fixed_header.write_to_buf(buf);
        buf.put_u16(self.packet_identifier);

        self.options.write_properties(buf);
        self.subscriptions.write_properties(buf);
    }

    fn try_read(header: FixedHeader, data: &mut Bytes) -> Result<Self, Error> {
        assert_eq!(header.control_packet_type, ControlPacketType::Subscribe);

        if data.len() < 3 {
            return Err(MalformedPacket::new("Packet too short to parse"));
        }
        let packet_identifier = data.try_get_u16()?;

        if !data.has_remaining() {
            return Err(MalformedPacket::new(
                "Subscribe packet cannot have 0 subscriptions",
            ));
        }

        let options = V::SubscribeData::try_read(data)?;
        let subscriptions = Vec::<V::TopicSubscription>::try_read(data)?;

        Ok(Self {
            packet_identifier,
            subscriptions,
            options,
        })
    }
}

impl<V: MqttVersion> Subscribe<V> {
    /// Create new subscribe package
    ///
    /// # Panics
    /// - If `subscriptions.len() == 0`
    pub fn new(packet_identifier: u16, subscriptions: Vec<V::TopicSubscription>) -> Self {
        // Protocol violation if 0
        if subscriptions.is_empty() {
            panic!("Protocol violation. Cannot create MQTT subscribe-packet with 0 subscriptions.");
        }
        Self {
            packet_identifier,
            subscriptions,
            options: V::SubscribeData::default(),
        }
    }
    pub fn packet_identifier(&self) -> u16 {
        self.packet_identifier
    }

    pub fn subscriptions(&self) -> &[V::TopicSubscription] {
        &self.subscriptions
    }

    pub fn subscriptions_mut(&mut self) -> &mut Vec<V::TopicSubscription> {
        &mut self.subscriptions
    }

    pub fn packet_identifier_mut(&mut self) -> &mut u16 {
        &mut self.packet_identifier
    }
}

impl Subscribe<MqttV5_0_0> {
    /// Create new subscribe package
    ///
    /// # Panics
    /// - If `subscriptions.len() == 0`
    pub fn new_with_options(
        packet_identifier: u16,
        subscriptions: Vec<TopicSubscriptionV5>,
        subscription_identifier: Option<u64>,
        user_property: Vec<UserProperty>,
    ) -> Self {
        // Protocol violation if 0
        if subscriptions.is_empty() {
            panic!("Protocol violation. Cannot create MQTT subscribe-packet with 0 subscriptions.");
        }

        Self {
            packet_identifier,
            subscriptions,
            options: SubscribeOptions {
                subscription_identifier,
                user_property,
            },
        }
    }

    pub fn subscription_identifier(&self) -> Option<u64> {
        self.options.subscription_identifier
    }
    pub fn user_property(&self) -> &[UserProperty] {
        &self.options.user_property
    }
}

#[cfg(test)]
mod test_v3 {

    use bytes::BytesMut;

    use crate::MqttV3_1_1;

    use super::*;

    #[test]
    fn serialize() {
        let mut buf = Vec::new();
        let msg = Subscribe::<MqttV3_1_1>::new(
            42,
            vec![
                TopicSubscriptionV3::new(MqttTopic::try_from("topic1").unwrap(), Qos::ExactlyOnce),
                TopicSubscriptionV3::new(MqttTopic::try_from("topic2").unwrap(), Qos::AtMostOnce),
            ],
        );
        msg.write_to_buf(&mut buf);
        assert_eq!(
            &buf,
            &[
                128 | 2,
                20,
                0,
                42,
                0,
                6,
                b't',
                b'o',
                b'p',
                b'i',
                b'c',
                b'1',
                2,
                0,
                6,
                b't',
                b'o',
                b'p',
                b'i',
                b'c',
                b'2',
                0,
            ]
        );
    }
    #[test]
    fn deserialize() {
        let msg = [
            128 | 2,
            20,
            0,
            42,
            0,
            6,
            b't',
            b'o',
            b'p',
            b'i',
            b'c',
            b'1',
            2,
            0,
            6,
            b't',
            b'o',
            b'p',
            b'i',
            b'c',
            b'2',
            0,
        ];
        let expected = Subscribe::<MqttV3_1_1>::new(
            42,
            vec![
                TopicSubscriptionV3::new(MqttTopic::try_from("topic1").unwrap(), Qos::ExactlyOnce),
                TopicSubscriptionV3::new(MqttTopic::try_from("topic2").unwrap(), Qos::AtMostOnce),
            ],
        );
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(Subscribe::try_read(header, &mut body).unwrap(), expected);
    }
}

#[cfg(test)]
mod test_v5 {

    use bytes::BytesMut;

    use super::*;

    #[test]
    fn serialize_v3_like() {
        let mut buf = Vec::new();
        let msg = Subscribe::new_with_options(
            42,
            vec![
                TopicSubscriptionV5::new(
                    MqttTopic::try_from("topic1").unwrap(),
                    Qos::ExactlyOnce,
                    false,
                    false,
                    RetainHandling::SendAtSubscribe,
                ),
                TopicSubscriptionV5::new(
                    MqttTopic::try_from("topic2").unwrap(),
                    Qos::AtMostOnce,
                    false,
                    false,
                    RetainHandling::SendAtSubscribe,
                ),
            ],
            None,
            Vec::new(),
        );
        msg.write_to_buf(&mut buf);
        assert_eq!(
            &buf,
            &[
                128 | 2,
                21,
                0,
                42,
                0,
                // Properties len
                0,
                6,
                b't',
                b'o',
                b'p',
                b'i',
                b'c',
                b'1',
                2,
                0,
                6,
                b't',
                b'o',
                b'p',
                b'i',
                b'c',
                b'2',
                0,
            ]
        );
    }

    #[test]
    fn serialize_properties() {
        let mut buf = Vec::new();
        let msg = Subscribe::new_with_options(
            42,
            vec![
                TopicSubscriptionV5::new(
                    MqttTopic::try_from("topic1").unwrap(),
                    Qos::ExactlyOnce,
                    false,
                    false,
                    RetainHandling::SendAtSubscribe,
                ),
                TopicSubscriptionV5::new(
                    MqttTopic::try_from("topic2").unwrap(),
                    Qos::AtMostOnce,
                    false,
                    false,
                    RetainHandling::SendAtSubscribe,
                ),
            ],
            Some(42),
            vec![UserProperty {
                key: "property1".into(),
                value: "value1".into(),
            }],
        );
        msg.write_to_buf(&mut buf);
        assert_eq!(
            &buf,
            &[
                128 | 2,
                43,
                0,
                42,
                // Properties len
                22,
                //  subscription id
                11,
                42,
                // User property
                38,
                0,
                9,
                b'p',
                b'r',
                b'o',
                b'p',
                b'e',
                b'r',
                b't',
                b'y',
                b'1',
                0,
                6,
                b'v',
                b'a',
                b'l',
                b'u',
                b'e',
                b'1',
                0,
                6,
                b't',
                b'o',
                b'p',
                b'i',
                b'c',
                b'1',
                2,
                0,
                6,
                b't',
                b'o',
                b'p',
                b'i',
                b'c',
                b'2',
                0,
            ]
        );
    }

    #[test]
    fn serialize_sub_options() {
        let mut buf = Vec::new();
        let msg = Subscribe::new_with_options(
            42,
            vec![
                TopicSubscriptionV5::new(
                    MqttTopic::try_from("topic1").unwrap(),
                    Qos::ExactlyOnce,
                    true,
                    false,
                    RetainHandling::SendIfSubDoesNotExist,
                ),
                TopicSubscriptionV5::new(
                    MqttTopic::try_from("topic2").unwrap(),
                    Qos::AtMostOnce,
                    false,
                    true,
                    RetainHandling::DontSendRetained,
                ),
            ],
            None,
            Vec::new(),
        );
        msg.write_to_buf(&mut buf);
        assert_eq!(
            &buf,
            &[
                128 | 2,
                21,
                0,
                42,
                0,
                // Properties len
                0,
                6,
                b't',
                b'o',
                b'p',
                b'i',
                b'c',
                b'1',
                22,
                0,
                6,
                b't',
                b'o',
                b'p',
                b'i',
                b'c',
                b'2',
                40,
            ]
        );
    }

    #[test]
    fn deserialize_v3_like() {
        let msg = [
            128 | 2,
            21,
            0,
            42,
            // Properties len
            0,
            0,
            6,
            b't',
            b'o',
            b'p',
            b'i',
            b'c',
            b'1',
            2,
            0,
            6,
            b't',
            b'o',
            b'p',
            b'i',
            b'c',
            b'2',
            0,
        ];
        let expected = Subscribe::new_with_options(
            42,
            vec![
                TopicSubscriptionV5::new(
                    MqttTopic::try_from("topic1").unwrap(),
                    Qos::ExactlyOnce,
                    false,
                    false,
                    RetainHandling::SendAtSubscribe,
                ),
                TopicSubscriptionV5::new(
                    MqttTopic::try_from("topic2").unwrap(),
                    Qos::AtMostOnce,
                    false,
                    false,
                    RetainHandling::SendAtSubscribe,
                ),
            ],
            None,
            Vec::new(),
        );
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(Subscribe::try_read(header, &mut body).unwrap(), expected);
    }

    #[test]
    fn deserialize_properties() {
        let msg = [
            128 | 2,
            43,
            0,
            42,
            // Properties len
            22,
            //  subscription id
            11,
            42,
            // User property
            38,
            0,
            9,
            b'p',
            b'r',
            b'o',
            b'p',
            b'e',
            b'r',
            b't',
            b'y',
            b'1',
            0,
            6,
            b'v',
            b'a',
            b'l',
            b'u',
            b'e',
            b'1',
            0,
            6,
            b't',
            b'o',
            b'p',
            b'i',
            b'c',
            b'1',
            2,
            0,
            6,
            b't',
            b'o',
            b'p',
            b'i',
            b'c',
            b'2',
            0,
        ];
        let expected = Subscribe::new_with_options(
            42,
            vec![
                TopicSubscriptionV5::new(
                    MqttTopic::try_from("topic1").unwrap(),
                    Qos::ExactlyOnce,
                    false,
                    false,
                    RetainHandling::SendAtSubscribe,
                ),
                TopicSubscriptionV5::new(
                    MqttTopic::try_from("topic2").unwrap(),
                    Qos::AtMostOnce,
                    false,
                    false,
                    RetainHandling::SendAtSubscribe,
                ),
            ],
            Some(42),
            vec![UserProperty {
                key: "property1".into(),
                value: "value1".into(),
            }],
        );
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(Subscribe::try_read(header, &mut body).unwrap(), expected);
    }

    #[test]
    fn deserialize_sub_options() {
        let msg = [
            128 | 2,
            21,
            0,
            42,
            0,
            // Properties len
            0,
            6,
            b't',
            b'o',
            b'p',
            b'i',
            b'c',
            b'1',
            22,
            0,
            6,
            b't',
            b'o',
            b'p',
            b'i',
            b'c',
            b'2',
            40,
        ];
        let expected = Subscribe::new_with_options(
            42,
            vec![
                TopicSubscriptionV5::new(
                    MqttTopic::try_from("topic1").unwrap(),
                    Qos::ExactlyOnce,
                    true,
                    false,
                    RetainHandling::SendIfSubDoesNotExist,
                ),
                TopicSubscriptionV5::new(
                    MqttTopic::try_from("topic2").unwrap(),
                    Qos::AtMostOnce,
                    false,
                    true,
                    RetainHandling::DontSendRetained,
                ),
            ],
            None,
            Vec::new(),
        );
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(Subscribe::try_read(header, &mut body).unwrap(), expected);
    }
}

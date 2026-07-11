use std::marker::PhantomData;

use bytes::{Buf, BufMut, Bytes};

use crate::util::{read_variable_len_int, variable_len_int_size, write_variable_len_int};
use crate::{
    Error, MalformedPacket, MqttV3_1_1, MqttV5_0_0, Property, PropertyIdentifier, UserProperty,
};

use crate::{
    fixed_header::{ControlPacketType, FixedHeader},
    util::{extract_str, write_str, MqttTopic},
};

#[derive(Debug, PartialEq)]
pub enum UnsubscribeOptions<V> {
    V3 {
        protocol_level: PhantomData<V>,
    },
    V5 {
        protocol_level: PhantomData<V>,
        user_property: Vec<UserProperty>,
    },
}

#[derive(Debug, PartialEq)]
/// An UNSUBSCRIBE Packet is sent by the Client to the Server, to unsubscribe from topics.
pub struct Unsubscribe<V> {
    fixed_header: FixedHeader,
    packet_identifier: u16,
    options: UnsubscribeOptions<V>,
    topics: Vec<String>,
}

impl<V> Unsubscribe<V> {
    pub fn write_to_buf(&self, buf: &mut impl BufMut) {
        self.fixed_header.write_to_buf(buf);
        buf.put_u16(self.packet_identifier);

        if let UnsubscribeOptions::V5 { user_property, .. } = &self.options {
            let properties_len = user_property.property_len();
            write_variable_len_int(properties_len as u64, buf);
            user_property.serialize(crate::PropertyIdentifier::UserProperty, buf);
        }

        for topic in &self.topics {
            write_str(topic, buf);
        }
    }

    pub fn packet_identifier(&self) -> u16 {
        self.packet_identifier
    }

    pub fn topics(&self) -> &[String] {
        &self.topics
    }
}
impl Unsubscribe<MqttV3_1_1> {
    pub fn new_v3(packet_identifier: u16, topics: Vec<MqttTopic>) -> Self {
        Self {
            fixed_header: FixedHeader::new(
                ControlPacketType::Unsubscribe,
                2 + topics.len() * 2 + topics.iter().map(|topic| topic.0.len()).sum::<usize>(),
            ),
            packet_identifier,
            topics: topics
                .into_iter()
                .map(|topic| topic.0.to_string())
                .collect(),
            options: UnsubscribeOptions::V3 {
                protocol_level: PhantomData,
            },
        }
    }
    pub fn try_read_v3(header: FixedHeader, data: &mut Bytes) -> Result<Self, Error> {
        let packet_identifier = data.try_get_u16()?;
        let mut topics = Vec::new();
        while data.has_remaining() {
            let topic = extract_str(data)?;
            topics.push(topic.to_string());
        }
        Ok(Self {
            fixed_header: header,
            packet_identifier,
            topics,
            options: UnsubscribeOptions::V3 {
                protocol_level: PhantomData,
            },
        })
    }
}

impl Unsubscribe<MqttV5_0_0> {
    pub fn new_v5(
        packet_identifier: u16,
        topics: Vec<MqttTopic>,
        user_property: Vec<UserProperty>,
    ) -> Self {
        let properties_len = user_property.property_len();
        Self {
            fixed_header: FixedHeader::new(
                ControlPacketType::Unsubscribe,
                2 + variable_len_int_size(properties_len)
                    + properties_len
                    + topics.len() * 2
                    + topics.iter().map(|topic| topic.0.len()).sum::<usize>(),
            ),
            packet_identifier,
            topics: topics
                .into_iter()
                .map(|topic| topic.0.to_string())
                .collect(),
            options: UnsubscribeOptions::V5 {
                protocol_level: PhantomData,
                user_property,
            },
        }
    }
    pub fn try_read_v5(header: FixedHeader, data: &mut Bytes) -> Result<Self, Error> {
        let packet_identifier = data.try_get_u16()?;
        let properties_len = read_variable_len_int(data)? as usize;

        let mut user_property: Vec<UserProperty> = Vec::new();

        if data.remaining() < properties_len {
            return Err(MalformedPacket::new("Packet too short to read property"));
        }
        let end_properties = data.remaining() - properties_len;

        while data.remaining() > end_properties {
            let property_identifier = read_variable_len_int(data)?;
            let property_identifier = PropertyIdentifier::try_from(property_identifier)?;
            if property_identifier == PropertyIdentifier::UserProperty {
                let key = extract_str(data)?.to_string();
                let value = extract_str(data)?.to_string();
                user_property.push(UserProperty { key, value });
            } else {
                return Err(MalformedPacket::new(
                    "Received unexpected property for connect",
                ));
            }
        }

        let mut topics = Vec::new();
        while data.has_remaining() {
            let topic = extract_str(data)?;
            topics.push(topic.to_string());
        }
        Ok(Self {
            fixed_header: header,
            packet_identifier,
            topics,
            options: UnsubscribeOptions::V5 {
                protocol_level: PhantomData,
                user_property,
            },
        })
    }

    pub fn user_property(&self) -> &[UserProperty] {
        match &self.options {
            UnsubscribeOptions::V3 { .. } => unreachable!(),
            UnsubscribeOptions::V5 { user_property, .. } => &user_property,
        }
    }
}

#[cfg(test)]
mod test_v3 {

    use bytes::BytesMut;

    use super::*;

    #[test]
    fn serialize() {
        let mut buf = Vec::new();
        let msg = Unsubscribe::new_v3(
            42,
            vec!["topic1".try_into().unwrap(), "topic2".try_into().unwrap()],
        );
        msg.write_to_buf(&mut buf);
        assert_eq!(
            &buf,
            &[
                160 | 2,
                18,
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
                0,
                6,
                b't',
                b'o',
                b'p',
                b'i',
                b'c',
                b'2',
            ]
        );
    }
    #[test]
    fn deserialize() {
        let msg = [
            160 | 2,
            18,
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
            0,
            6,
            b't',
            b'o',
            b'p',
            b'i',
            b'c',
            b'2',
        ];
        let expected = Unsubscribe::new_v3(
            42,
            vec!["topic1".try_into().unwrap(), "topic2".try_into().unwrap()],
        );
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(
            Unsubscribe::try_read_v3(header, &mut body).unwrap(),
            expected
        );
    }
}

#[cfg(test)]
mod test_v5 {

    use bytes::BytesMut;

    use super::*;

    #[test]
    fn serialize_normal() {
        let mut buf = Vec::new();
        let msg = Unsubscribe::new_v5(
            42,
            vec!["topic1".try_into().unwrap(), "topic2".try_into().unwrap()],
            vec![],
        );
        msg.write_to_buf(&mut buf);
        assert_eq!(
            &buf,
            &[
                160 | 2,
                19,
                0,
                42,
                0,
                0,
                6,
                b't',
                b'o',
                b'p',
                b'i',
                b'c',
                b'1',
                0,
                6,
                b't',
                b'o',
                b'p',
                b'i',
                b'c',
                b'2',
            ]
        );
    }
    #[test]
    fn serialize_user_property() {
        let mut buf = Vec::new();
        let msg = Unsubscribe::new_v5(
            42,
            vec!["topic1".try_into().unwrap(), "topic2".try_into().unwrap()],
            vec![UserProperty {
                key: "property1".into(),
                value: "value1".into(),
            }],
        );
        msg.write_to_buf(&mut buf);
        assert_eq!(
            &buf,
            &[
                160 | 2,
                39,
                0,
                42,
                20,
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
                0,
                6,
                b't',
                b'o',
                b'p',
                b'i',
                b'c',
                b'2',
            ]
        );
    }

    #[test]
    fn deserialize_normal() {
        let msg = [
            160 | 2,
            19,
            0,
            42,
            0,
            0,
            6,
            b't',
            b'o',
            b'p',
            b'i',
            b'c',
            b'1',
            0,
            6,
            b't',
            b'o',
            b'p',
            b'i',
            b'c',
            b'2',
        ];
        let expected = Unsubscribe::new_v5(
            42,
            vec!["topic1".try_into().unwrap(), "topic2".try_into().unwrap()],
            vec![],
        );
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(
            Unsubscribe::try_read_v5(header, &mut body).unwrap(),
            expected
        );
    }

    #[test]
    fn deserialize_user_property() {
        let msg = [
            160 | 2,
            39,
            0,
            42,
            20,
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
            0,
            6,
            b't',
            b'o',
            b'p',
            b'i',
            b'c',
            b'2',
        ];
        let expected = Unsubscribe::new_v5(
            42,
            vec!["topic1".try_into().unwrap(), "topic2".try_into().unwrap()],
            vec![UserProperty {
                key: "property1".into(),
                value: "value1".into(),
            }],
        );
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(
            Unsubscribe::try_read_v5(header, &mut body).unwrap(),
            expected
        );
    }
}

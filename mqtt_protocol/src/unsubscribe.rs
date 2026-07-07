use std::io::Write;
use std::marker::PhantomData;

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
    pub fn write_to_stream(self, writer: &mut impl Write) -> Result<usize, std::io::Error> {
        let mut length = self.fixed_header.write_to_stream(writer)?;
        writer.write_all(&[
            ((self.packet_identifier & 0xff00) >> 8) as u8,
            (self.packet_identifier & 0xff) as u8,
        ])?;
        length += 2;

        if let UnsubscribeOptions::V5 { user_property, .. } = self.options {
            let properties_len = user_property.property_len();
            length += write_variable_len_int(properties_len as u64, writer)?;
            length += user_property.serialize(crate::PropertyIdentifier::UserProperty, writer)?;
        }

        for topic in self.topics {
            write_str(&topic, writer)?;
            length += topic.len() + 2;
        }
        writer.flush()?;
        Ok(length)
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
    pub fn try_read_v3(header: FixedHeader, data: &[u8]) -> Result<Self, Error> {
        if data.len() < 4 {
            return Err(MalformedPacket::new("Packet too short to parse"));
        }
        let packet_identifier = u16::from_be_bytes([data[0], data[1]]);
        let mut remaining = header.remaining_length - 2;
        let mut index = 2;
        let mut topics = Vec::new();
        while remaining > 0 {
            let topic = extract_str(&data[index..])?;
            index += 2 + topic.len();
            remaining -= 2 + topic.len();
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
    pub fn try_read_v5(header: FixedHeader, data: &[u8]) -> Result<Self, Error> {
        if data.len() < 4 {
            return Err(MalformedPacket::new("Packet too short to parse"));
        }
        let packet_identifier = u16::from_be_bytes([data[0], data[1]]);
        let (property_length_length, properties_len) = read_variable_len_int(&data[2..])?;
        let properties_len = properties_len as usize;

        let mut index = 2 + property_length_length;
        let mut user_property: Vec<UserProperty> = Vec::new();
        while index - 2 - property_length_length < properties_len {
            let (i, property_identifier) = read_variable_len_int(&data[index..])?;
            let property_identifier = PropertyIdentifier::try_from(property_identifier)?;
            if property_identifier == PropertyIdentifier::UserProperty {
                let key = extract_str(&data[index + i..])?.to_string();
                let value = extract_str(&data[index + i + 2 + key.len()..])?.to_string();
                index += i + 2 + key.len() + 2 + value.len();
                user_property.push(UserProperty { key, value });
            } else {
                return Err(MalformedPacket::new(
                    "Received unexpected property for connect",
                ));
            }
        }

        let mut remaining = header.remaining_length - 2 - property_length_length - properties_len;
        let mut topics = Vec::new();
        while remaining > 0 {
            let topic = extract_str(&data[index..])?;
            index += 2 + topic.len();
            remaining -= 2 + topic.len();
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
}

#[cfg(test)]
mod test_v3 {
    use std::io::{BufReader, BufWriter, Read};

    use super::*;

    #[test]
    fn serialize() {
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        let msg = Unsubscribe::new_v3(
            42,
            vec!["topic1".try_into().unwrap(), "topic2".try_into().unwrap()],
        );
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
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
        let mut reader = BufReader::new(&msg[..]);
        let header = FixedHeader::try_read_sync(&mut reader).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(
            Unsubscribe::try_read_v3(header, &data[..]).unwrap(),
            expected
        );
    }
}

#[cfg(test)]
mod test_v5 {
    use std::io::{BufReader, BufWriter, Read};

    use super::*;

    #[test]
    fn serialize_normal() {
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        let msg = Unsubscribe::new_v5(
            42,
            vec!["topic1".try_into().unwrap(), "topic2".try_into().unwrap()],
            vec![],
        );
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
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
        let mut writer = BufWriter::new(&mut buf);
        let msg = Unsubscribe::new_v5(
            42,
            vec!["topic1".try_into().unwrap(), "topic2".try_into().unwrap()],
            vec![UserProperty {
                key: "property1".into(),
                value: "value1".into(),
            }],
        );
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
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
        let mut reader = BufReader::new(&msg[..]);
        let header = FixedHeader::try_read_sync(&mut reader).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(
            Unsubscribe::try_read_v5(header, &data[..]).unwrap(),
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
        let mut reader = BufReader::new(&msg[..]);
        let header = FixedHeader::try_read_sync(&mut reader).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(
            Unsubscribe::try_read_v5(header, &data[..]).unwrap(),
            expected
        );
    }
}

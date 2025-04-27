use std::io::Write;

use crate::ControlPacketType;

use super::{
    fixed_header::{self, FixedHeader},
    util::{extract_str, write_str, MqttTopic, PacketError, Qos, QosPacketIdentifier},
};

#[derive(Debug, PartialEq)]
/// A PUBLISH Control Packet is sent from a Client to a Server or from Server to a Client to transport an Application Message.
pub struct Publish<'a> {
    pub fixed_header: FixedHeader,
    pub topic: &'a str,
    pub packet_identifier: Option<u16>,
    pub payload: &'a [u8],
}

#[derive(Debug, PartialEq)]
/// A PUBLISH Control Packet is sent from a Client to a Server or from Server to a Client to transport an Application Message.
pub struct ReceivedMessage {
    pub flags: u8,
    pub topic: String,
    pub packet_identifier: Option<u16>,
    pub payload: Vec<u8>,
}

impl<'a> Publish<'a> {
    pub fn new(
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
        }
    }

    pub fn write_to_stream(self, writer: &mut impl Write) -> Result<usize, std::io::Error> {
        let mut length = self.fixed_header.write_to_stream(writer)?;
        length += write_str(self.topic, writer)?;
        if let Some(packet_identifier) = self.packet_identifier {
            writer.write_all(&[
                ((packet_identifier & 0xff00) >> 8) as u8,
                (packet_identifier & 0xff) as u8,
            ])?;
            length += 2;
        }
        length += self.payload.len();
        writer.write_all(&self.payload)?;
        writer.flush()?;

        Ok(length)
    }
}

impl ReceivedMessage {
    pub fn try_read(header: FixedHeader, mut data: Vec<u8>) -> Result<Self, PacketError> {
        let flags = header.control_packet_type.flags();
        if data.len() < 2 {
            return Err(PacketError::MissingBytes {
                expected: 2,
                got: data.len(),
            });
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
        })
    }
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

#[cfg(test)]
mod test {
    use std::io::{BufReader, BufWriter, Read};

    use super::*;

    #[test]
    fn serialize() {
        let topic = MqttTopic::try_from("topic").unwrap();
        let msg = Publish::new(
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
        let msg = Publish::new(
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
        let msg = Publish::new(
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
        let msg = Publish::new(
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
        let msg = Publish::new(
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
        let expected = ReceivedMessage {
            flags: ControlPacketType::Publish {
                dup: false,
                qos: Qos::AtMostOnce,
                retain: false,
            }
            .flags(),
            topic: "topic".to_string(),
            packet_identifier: None,
            payload: b"payload".to_vec(),
        };
        let msg = [
            48, 14, 0, 5, b't', b'o', b'p', b'i', b'c', b'p', b'a', b'y', b'l', b'o', b'a', b'd',
        ];
        let mut reader = BufReader::new(&msg[..]);
        let header = FixedHeader::try_read(&mut reader).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(ReceivedMessage::try_read(header, data).unwrap(), expected);
    }
    #[test]
    fn deserialize_qos() {
        let expected = ReceivedMessage {
            flags: ControlPacketType::Publish {
                dup: false,
                qos: Qos::ExactlyOnce,
                retain: false,
            }
            .flags(),
            topic: "topic".to_string(),
            packet_identifier: Some(42),
            payload: b"payload".to_vec(),
        };
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
        let header = FixedHeader::try_read(&mut reader).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(ReceivedMessage::try_read(header, data).unwrap(), expected);
    }
    #[test]
    fn deserialize_dup() {
        let expected = ReceivedMessage {
            flags: ControlPacketType::Publish {
                dup: true,
                qos: Qos::AtMostOnce,
                retain: false,
            }
            .flags(),
            topic: "topic".to_string(),
            packet_identifier: None,
            payload: b"payload".to_vec(),
        };
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
        let header = FixedHeader::try_read(&mut reader).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(ReceivedMessage::try_read(header, data).unwrap(), expected);
    }
    #[test]
    fn deserialize_retain() {
        let expected = ReceivedMessage {
            flags: ControlPacketType::Publish {
                dup: false,
                qos: Qos::AtMostOnce,
                retain: true,
            }
            .flags(),
            topic: "topic".to_string(),
            packet_identifier: None,
            payload: b"payload".to_vec(),
        };
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
        let header = FixedHeader::try_read(&mut reader).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(ReceivedMessage::try_read(header, data).unwrap(), expected);
    }
}

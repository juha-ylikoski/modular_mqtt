use std::io::Write;

use super::{
    fixed_header::{self, FixedHeader},
    util::{
        extract_bytes, extract_str, write_bytes, write_str, MqttTopic, PacketError, Qos,
        QosPacketIdentifier,
    },
};

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
/// A PUBLISH Control Packet is sent from a Client to a Server or from Server to a Client to transport an Application Message.
pub struct Publish<'a> {
    fixed_header: FixedHeader,
    topic: &'a str,
    packet_identifier: Option<u16>,
    payload: &'a [u8],
}

impl<'a> Publish<'a> {
    pub fn new(
        dup: bool,
        qos: QosPacketIdentifier,
        retain: bool,
        topic: MqttTopic<'a>,
        payload: &'a [u8],
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
        }
    }
    pub fn try_read(header: FixedHeader, data: &'a [u8]) -> Result<Self, PacketError> {
        if data.len() < 2 {
            return Err(PacketError::MissingBytes(2, data.len()));
        }
        let topic = extract_str(data)?;
        let (_, qos, _) = match header.control_packet_type {
            fixed_header::ControlPacketType::Publish { dup, qos, retain } => (dup, qos, retain),
            _ => panic!("Library had internal error. This is bug!"),
        };
        let mut pos = 2 + topic.len();
        let packet_identifier = if qos != Qos::AtMostOnce {
            let id = Some(u16::from_be_bytes([data[pos], data[pos + 1]]));
            pos += 2;
            id
        } else {
            None
        };

        let payload = extract_bytes(&data[pos..])?;

        Ok(Self {
            fixed_header: header,
            topic,
            packet_identifier,
            payload,
        })
    }
    #[cfg_attr(feature = "async", mqtt_protocol_derive::impl_async)]
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
        writer.flush()?;
        Ok(length + write_bytes(self.payload, writer)?)
    }
}

#[cfg(test)]
mod test {
    use std::io::{BufReader, BufWriter, Read};

    use super::*;

    #[test]
    fn serialize() {
        let msg = Publish::new(
            false,
            QosPacketIdentifier::AtMostOnce,
            false,
            "topic".try_into().unwrap(),
            b"payload",
        );
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
        assert_eq!(
            &buf,
            &[
                48, 14, 0, 5, b't', b'o', b'p', b'i', b'c', 0, 7, b'p', b'a', b'y', b'l', b'o',
                b'a', b'd'
            ]
        );
    }
    #[test]
    fn serialize_qos() {
        let msg = Publish::new(
            false,
            QosPacketIdentifier::ExactlyOnce(42),
            false,
            "topic".try_into().unwrap(),
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
                0,
                7,
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
        let msg = Publish::new(
            true,
            QosPacketIdentifier::AtMostOnce,
            false,
            "topic".try_into().unwrap(),
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
                0,
                7,
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
        let msg = Publish::new(
            false,
            QosPacketIdentifier::AtMostOnce,
            true,
            "topic".try_into().unwrap(),
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
                0,
                7,
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
        let expected = Publish::new(
            false,
            QosPacketIdentifier::AtMostOnce,
            false,
            "topic".try_into().unwrap(),
            b"payload",
        );
        let msg = [
            48, 14, 0, 5, b't', b'o', b'p', b'i', b'c', 0, 7, b'p', b'a', b'y', b'l', b'o', b'a',
            b'd',
        ];
        let mut reader = BufReader::new(&msg[..]);
        let header = FixedHeader::try_read(&mut reader).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(Publish::try_read(header, &data[..]).unwrap(), expected);
    }
    #[test]
    fn deserialize_qos() {
        let expected = Publish::new(
            false,
            QosPacketIdentifier::ExactlyOnce(42),
            false,
            "topic".try_into().unwrap(),
            b"payload",
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
            0,
            7,
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
        assert_eq!(Publish::try_read(header, &data[..]).unwrap(), expected);
    }
    #[test]
    fn deserialize_dup() {
        let expected = Publish::new(
            true,
            QosPacketIdentifier::AtMostOnce,
            false,
            "topic".try_into().unwrap(),
            b"payload",
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
            0,
            7,
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
        assert_eq!(Publish::try_read(header, &data[..]).unwrap(), expected);
    }
    #[test]
    fn deserialize_retain() {
        let expected = Publish::new(
            false,
            QosPacketIdentifier::AtMostOnce,
            true,
            "topic".try_into().unwrap(),
            b"payload",
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
            0,
            7,
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
        assert_eq!(Publish::try_read(header, &data[..]).unwrap(), expected);
    }
}

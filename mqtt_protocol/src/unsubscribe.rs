use std::io::Write;

use crate::util::PacketError;

use crate::{
    fixed_header::{ControlPacketType, FixedHeader},
    util::{extract_str, write_str, MqttTopic},
};

#[derive(Debug, PartialEq)]
/// An UNSUBSCRIBE Packet is sent by the Client to the Server, to unsubscribe from topics.
pub struct Unsubscribe {
    fixed_header: FixedHeader,
    packet_identifier: u16,
    topics: Vec<String>,
}

impl Unsubscribe {
    pub fn new(packet_identifier: u16, topics: Vec<MqttTopic>) -> Self {
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
        }
    }
    pub fn try_read(header: FixedHeader, data: &[u8]) -> Result<Self, PacketError> {
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
        })
    }
    pub fn write_to_stream(self, writer: &mut impl Write) -> Result<usize, std::io::Error> {
        let mut length = self.fixed_header.write_to_stream(writer)?;
        writer.write_all(&[
            ((self.packet_identifier & 0xff00) >> 8) as u8,
            (self.packet_identifier & 0xff) as u8,
        ])?;
        length += 2;
        for topic in self.topics {
            write_str(&topic, writer)?;
            length += topic.len() + 2;
        }
        writer.flush()?;
        Ok(length)
    }
}

#[cfg(test)]
mod test {
    use std::io::{BufReader, BufWriter, Read};

    use super::*;

    #[test]
    fn serialize() {
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        let msg = Unsubscribe::new(
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
        let expected = Unsubscribe::new(
            42,
            vec!["topic1".try_into().unwrap(), "topic2".try_into().unwrap()],
        );
        let mut reader = BufReader::new(&msg[..]);
        let header = FixedHeader::try_read(&mut reader).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(Unsubscribe::try_read(header, &data[..]).unwrap(), expected);
    }
}

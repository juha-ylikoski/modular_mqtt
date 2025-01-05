use core::panic;
use std::io::Write;

use crate::mqtt_protocol::util::PacketError;

use super::{
    fixed_header::FixedHeader,
    util::{extract_str, write_str, Qos},
};

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct TopicSubscription {
    topic: String,
    qos: Qos,
}

impl TopicSubscription {
    pub fn new(topic: String, qos: Qos) -> Self {
        Self { topic, qos }
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
/// The SUBSCRIBE Packet is sent from the Client to the Server to create one or more Subscriptions. Each Subscription registers a Client’s interest in one or more Topics. The Server sends PUBLISH Packets to the Client in order to forward Application Messages that were published to Topics that match these Subscriptions. The SUBSCRIBE Packet also specifies (for each Subscription) the maximum QoS with which the Server can send Application Messages to the Client.
pub struct Subscribe {
    fixed_header: FixedHeader,
    packet_identifier: u16,
    subscriptions: Vec<TopicSubscription>,
}

impl Subscribe {
    /// Create new subscribe package
    ///
    /// # Panics
    /// - If `subscriptions.len() == 0`
    pub fn new(packet_identifier: u16, subscriptions: Vec<TopicSubscription>) -> Self {
        // Protocol violation if 0
        if subscriptions.len() == 0 {
            panic!("Protocol violation. Cannot create MQTT subscribe-packet with 0 subscriptions.");
        }
        Self {
            fixed_header: FixedHeader::new(
                super::fixed_header::ControlPacketType::Subscribe,
                2 + subscriptions
                    .iter()
                    .map(|sub| sub.topic.len() as u64 + 3)
                    .sum::<u64>(),
            ),
            packet_identifier,
            subscriptions,
        }
    }

    pub fn try_read(header: FixedHeader, data: &[u8]) -> Result<Self, PacketError> {
        if data.len() < 2 {
            return Err(PacketError::MissingBytes(2, data.len()));
        }
        let packet_identifier = u16::from_be_bytes([data[0], data[1]]);
        let mut remaining = header.remaining_length - 2;
        let mut index = 2;

        if remaining == 0 {
            return Err(PacketError::MalformedPacket(
                "Subscribe packet cannot have 0 subscriptions",
            ));
        }

        let mut subscriptions = Vec::new();
        while remaining != 0 {
            let topic = extract_str(&data[index..])?;
            let qos = data[index + topic.len() + 2];
            let qos = Qos::try_from(qos)?;
            subscriptions.push(TopicSubscription::new(topic.to_string(), qos));
            remaining -= 2 + (topic.len() as u64) + 1;
            index += 2 + topic.len() + 1;
        }

        Ok(Self {
            fixed_header: header,
            packet_identifier,
            subscriptions,
        })
    }
    pub fn write_to_stream(self, writer: &mut impl Write) -> Result<usize, std::io::Error> {
        let mut length = self.fixed_header.write_to_stream(writer)?;
        writer.write_all(&[
            ((self.packet_identifier & 0xff00) >> 8) as u8,
            (self.packet_identifier & 0xff) as u8,
        ])?;
        length += 2;
        for sub in self.subscriptions {
            length += write_str(&sub.topic, writer)? + 1;
            writer.write_all(&[sub.qos as u8])?;
        }
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
        let msg = Subscribe::new(
            42,
            vec![
                TopicSubscription::new("topic1".to_string(), Qos::ExactlyOnce),
                TopicSubscription::new("topic2".to_string(), Qos::AtMostOnce),
            ],
        );
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
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
        let expected = Subscribe::new(
            42,
            vec![
                TopicSubscription::new("topic1".to_string(), Qos::ExactlyOnce),
                TopicSubscription::new("topic2".to_string(), Qos::AtMostOnce),
            ],
        );
        let mut reader = BufReader::new(&msg[..]);
        let header = FixedHeader::try_read(&mut reader).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(Subscribe::try_read(header, &data[..]).unwrap(), expected);
    }
}

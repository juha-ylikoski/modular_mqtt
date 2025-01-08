use std::io::Write;

use super::fixed_header::{ControlPacketType, FixedHeader};

macro_rules! create_packet_type {
    (#[doc = $doc:expr] $name:ident, $packet_type:expr, $test_mod:ident,$test_packet_type:expr) => {
        #[derive(Debug)]
        #[cfg_attr(test, derive(PartialEq))]
        #[doc = $doc]
        pub struct $name {
            fixed_header: FixedHeader,
            packet_identifier: u16,
        }

        impl $name {
            pub fn new(packet_identifier: u16) -> Self {
                Self {
                    fixed_header: FixedHeader::new($packet_type, 2),
                    packet_identifier,
                }
            }
            pub fn try_read(header: FixedHeader, data: &[u8]) -> Self {
                Self {
                    fixed_header: header,
                    packet_identifier: u16::from_be_bytes([data[0], data[1]]),
                }
            }
            #[cfg_attr(feature = "async", mqtt_protocol_derive::impl_async)]
            pub fn write_to_stream(self, writer: &mut impl Write) -> Result<usize, std::io::Error> {
                let len = self.fixed_header.write_to_stream(writer)?;
                writer.write_all(&[
                    ((self.packet_identifier & 0xff00) >> 8) as u8,
                    (self.packet_identifier & 0xff) as u8,
                ])?;
                Ok(len + 2)
            }
        }

        #[cfg(test)]
        mod $test_mod {
            use std::io::{BufReader, BufWriter, Read};

            use super::*;

            #[test]
            fn serialize() {
                let mut buf = Vec::new();
                let mut writer = BufWriter::new(&mut buf);
                let msg = $name::new(42);
                msg.write_to_stream(&mut writer).unwrap();
                drop(writer);
                assert_eq!(&buf, &[$test_packet_type, 2, 0, 42]);
            }
            #[test]
            fn deserialize() {
                let msg = [$test_packet_type, 2, 0, 42];
                let expected = $name::new(42);
                let mut reader = BufReader::new(&msg[..]);
                let header = FixedHeader::try_read(&mut reader).unwrap();
                let mut data = Vec::new();
                reader.read_to_end(&mut data).unwrap();
                assert_eq!($name::try_read(header, &data[..]), expected);
            }
        }
    };
}

create_packet_type!(
/// A PUBACK Packet is the response to a PUBLISH Packet with QoS level 1.
    PubAck, ControlPacketType::PubAck, test_puback, 64
);
create_packet_type!(
    /// A PUBREC Packet is the response to a PUBLISH Packet with QoS 2. It is the second packet of the QoS 2 protocol exchange.
    PubRec, ControlPacketType::PubRec, test_pubreck, 80
);
create_packet_type!(
    /// A PUBREL Packet is the response to a PUBREC Packet. It is the third packet of the QoS 2 protocol exchange.
    PubRel, ControlPacketType::PubRel, test_pubrec, 96 | 2
);
create_packet_type!(
    /// The PUBCOMP Packet is the response to a PUBREL Packet. It is the fourth and final packet of the QoS 2 protocol exchange.
    PubComp, ControlPacketType::PubComp, test_pubcomp, 112
);

create_packet_type!(
    /// The UNSUBACK Packet is sent by the Server to the Client to confirm receipt of an UNSUBSCRIBE Packet.
    UnsubscribeAck, ControlPacketType::UnsubscribeAck, test_unsubscribeack, 176
);

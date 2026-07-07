use std::io::Write;

use crate::fixed_header::{ControlPacketType, FixedHeader};
use crate::util::PacketError;

macro_rules! create_ping_package {
    (#[doc = $doc:expr] $name:ident, $packet_type:expr, $test_mod:ident,$test_packet_type:expr) => {
        #[derive(Debug, PartialEq)]
        #[doc = $doc]
        pub struct $name {
            #[allow(unused)]
            fixed_header: FixedHeader,
        }

        impl Default for $name {
            fn default() -> Self {
                Self {
                    fixed_header: FixedHeader::new($packet_type, 0),
                }
            }
        }

        impl $name {
            pub fn try_read(header: FixedHeader) -> Result<Self, PacketError> {
                if header.control_packet_type == $packet_type {
                    Ok(Self {
                        fixed_header: header,
                    })
                } else {
                    Err(PacketError::MalformedPacket(
                        "Invalid packet type when trying to create packet.",
                    ))
                }
            }
            pub fn write_to_stream(writer: &mut impl Write) -> Result<usize, std::io::Error> {
                let fixed_header = FixedHeader::new($packet_type, 0);
                let len = fixed_header.write_to_stream(writer)?;
                writer.flush()?;
                Ok(len)
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
                $name::write_to_stream(&mut writer).unwrap();
                drop(writer);
                assert_eq!(&buf, &[$test_packet_type, 0]);
            }
            #[test]
            fn deserialize() {
                let msg = [$test_packet_type, 0];
                let expected = $name::default();
                let mut reader = BufReader::new(&msg[..]);
                let header = FixedHeader::try_read_sync(&mut reader).unwrap();
                let mut data = Vec::new();
                reader.read_to_end(&mut data).unwrap();
                assert_eq!($name::try_read(header).unwrap(), expected);
            }
        }
    };
}

create_ping_package!(
    #[doc = "The PINGREQ Packet is sent from a Client to the Server. It can be used to:\n 1. Indicate to the Server that the Client is alive in the absence of any other Control Packets being sent from the Client to the Server.\n 2. Request that the Server responds to confirm that it is alive.\n 3. Exercise the network to indicate that the Network Connection is active."]
    PingReq,
    ControlPacketType::PingReq,
    test_pingreq,
    192
);
create_ping_package!(
    /// A PINGRESP Packet is sent by the Server to the Client in response to a PINGREQ Packet. It indicates that the Server is alive.
    PingResp,
    ControlPacketType::PingResp,
    test_pingresp,
    208
);

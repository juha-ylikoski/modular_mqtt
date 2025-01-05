use std::io::Write;

use super::fixed_header::{ControlPacketType, FixedHeader};

macro_rules! create_ping_package {
    (#[doc = $doc:expr] $name:ident, $packet_type:expr, $test_mod:ident,$test_packet_type:expr) => {
        #[derive(Debug)]
        #[cfg_attr(test, derive(PartialEq))]
        #[doc = $doc]
        pub struct $name {
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
            pub fn try_read(header: FixedHeader) -> Self {
                Self {
                    fixed_header: header,
                }
            }
            pub fn write_to_stream(self, writer: &mut impl Write) -> Result<usize, std::io::Error> {
                self.fixed_header.write_to_stream(writer)
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
                let msg = $name::default();
                msg.write_to_stream(&mut writer).unwrap();
                drop(writer);
                assert_eq!(&buf, &[$test_packet_type, 0]);
            }
            #[test]
            fn deserialize() {
                let msg = [$test_packet_type, 0];
                let expected = $name::default();
                let mut reader = BufReader::new(&msg[..]);
                let header = FixedHeader::try_read(&mut reader).unwrap();
                let mut data = Vec::new();
                reader.read_to_end(&mut data).unwrap();
                assert_eq!($name::try_read(header), expected);
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

create_ping_package!(
    /// The DISCONNECT Packet is the final Control Packet sent from the Client to the Server. It indicates that the Client is disconnecting cleanly.
    Disconnect, ControlPacketType::Disconnect, test_disconnect, 224
);

use bytes::BufMut;
use bytes::Bytes;

use crate::fixed_header::{ControlPacketType, FixedHeader};
use crate::Error;

macro_rules! create_ping_package {
    (#[doc = $doc:expr] $name:ident, $packet_type:expr, $test_mod:ident,$test_packet_type:expr) => {
        #[derive(Debug, PartialEq)]
        #[doc = $doc]
        pub struct $name;

        impl Default for $name {
            fn default() -> Self {
                Self
            }
        }

        impl $name {
            pub fn try_read(header: FixedHeader, _buf: &mut Bytes) -> Result<Self, Error> {
                assert_eq!(header.control_packet_type, $packet_type);
                Ok(Self)
            }
            pub fn write_to_buf(buf: &mut impl BufMut) {
                let fixed_header = FixedHeader::new($packet_type, 0);
                fixed_header.write_to_buf(buf);
            }
        }

        #[cfg(test)]
        mod $test_mod {
            use bytes::BytesMut;

            use super::*;

            #[test]
            fn serialize() {
                let mut buf = Vec::new();
                $name::write_to_buf(&mut buf);
                assert_eq!(&buf, &[$test_packet_type, 0]);
            }
            #[test]
            fn deserialize() {
                let msg = [$test_packet_type, 0];
                let expected = $name::default();
                let mut reader = BytesMut::from(&msg[..]);
                let (header, mut body) =
                    FixedHeader::parse(&mut reader, crate::MAX_MQTT_PACKET_SIZE)
                        .unwrap()
                        .unwrap();
                assert_eq!($name::try_read(header, &mut body).unwrap(), expected);
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

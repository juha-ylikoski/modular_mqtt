use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::{Error, MalformedPacket};

use super::util::Qos;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ControlPacketType {
    /// Client request to connect to Server
    Connect,
    /// Connect acknowledgment
    ConnAck,
    /// Publish message
    Publish {
        /// Duplicate delivery of PUBLISH Control packet
        dup: bool,
        /// PUBLISH Quality of service
        qos: Qos,
        /// Retain message on broker
        retain: bool,
    },
    /// Publish acknowledgment
    PubAck,
    /// Publish received (assured delivery part 1)
    PubRec,
    /// Publish release (assured delivery part 2)
    PubRel,
    /// Publish complete (assured delivery part 3)
    PubComp,
    /// Client subscribe request
    Subscribe,
    /// Subscribe acknowledgment
    SubAck,
    /// Unsubscribe request
    Unsubscribe,
    /// Unsubscribe acknowledgment
    UnsubscribeAck,
    /// PING request
    PingReq,
    /// PING response
    PingResp,
    /// Client is disconnecting
    Disconnect,
    /// Authentication exchange
    Auth,
}
impl ControlPacketType {
    pub fn value(&self) -> u8 {
        match self {
            Self::Connect => 1,
            Self::ConnAck => 2,
            Self::Publish { .. } => 3,
            Self::PubAck => 4,
            Self::PubRec => 5,
            Self::PubRel => 6,
            Self::PubComp => 7,
            Self::Subscribe => 8,
            Self::SubAck => 9,
            Self::Unsubscribe => 10,
            Self::UnsubscribeAck => 11,
            Self::PingReq => 12,
            Self::PingResp => 13,
            Self::Disconnect => 14,
            Self::Auth => 15,
        }
    }
    pub fn flags(&self) -> u8 {
        match self {
            ControlPacketType::Connect => 0,
            ControlPacketType::ConnAck => 0,
            ControlPacketType::Publish { dup, qos, retain } => {
                ((*dup as u8) << 3) | ((*qos as u8) << 1) | (*retain as u8)
            }
            ControlPacketType::PubAck => 0,
            ControlPacketType::PubRec => 0,
            ControlPacketType::PubRel => 0b0010,
            ControlPacketType::PubComp => 0,
            ControlPacketType::Subscribe => 0b0010,
            ControlPacketType::SubAck => 0,
            ControlPacketType::Unsubscribe => 0b0010,
            ControlPacketType::UnsubscribeAck => 0,
            ControlPacketType::PingReq => 0,
            ControlPacketType::PingResp => 0,
            ControlPacketType::Disconnect => 0,
            ControlPacketType::Auth => 0,
        }
    }
    pub fn flags_dup(flags: u8) -> bool {
        ((flags & 0b1000) >> 3) == 1
    }
    pub fn flags_qos(flags: u8) -> Result<Qos, Error> {
        Qos::try_from((flags & 0b110) >> 1)
            .map_err(|_| MalformedPacket::InvalidQos((flags & 0b110) >> 1).into())
    }
    pub fn flags_retain(flags: u8) -> bool {
        (flags & 1) == 1
    }
    pub fn try_from_byte(byte: u8) -> Result<Self, Error> {
        let packet_type = (byte & 0b11110000) >> 4;
        let flags = byte & 0b1111;

        macro_rules! verify {
            ($packet_type:expr) => {{
                let t = $packet_type;
                if $packet_type.flags() != flags {
                    Err(MalformedPacket::InvalidFlags(flags, t).into())
                } else {
                    Ok(t)
                }
            }};
        }

        match packet_type {
            1 => verify!(Self::Connect),
            2 => verify!(Self::ConnAck),
            3 => verify!(Self::Publish {
                dup: Self::flags_dup(flags),
                qos: Self::flags_qos(flags)?,
                retain: Self::flags_retain(flags),
            }),
            4 => verify!(Self::PubAck),
            5 => verify!(Self::PubRec),
            6 => verify!(Self::PubRel),
            7 => verify!(Self::PubComp),
            8 => verify!(Self::Subscribe),
            9 => verify!(Self::SubAck),
            10 => verify!(Self::Unsubscribe),
            11 => verify!(Self::UnsubscribeAck),
            12 => verify!(Self::PingReq),
            13 => verify!(Self::PingResp),
            14 => verify!(Self::Disconnect),
            15 => verify!(Self::Auth),
            _ => Err(MalformedPacket::new("ControlPacketType had reserved value")),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct FixedHeader {
    pub control_packet_type: ControlPacketType,
    pub remaining_length: usize,
}

impl FixedHeader {
    /// Createe a new fixed header.
    ///
    /// # Panics
    ///
    /// Will panic if remaining length is larger than 268 435 455.
    pub fn new(control_packet_type: ControlPacketType, remaining_length: usize) -> Self {
        if remaining_length > 268_435_455 {
            panic!("MQTT packet payload (dynamic header + payload) > 268 435 455")
        }
        Self {
            control_packet_type,
            remaining_length,
        }
    }

    /// Parse mqtt fixed header and return it along as well as variable header and payload for
    /// the packet
    ///
    /// `max_packet_size` parameter has to be lesser or equal to [`crate::MAX_MQTT_PACKET_SIZE`]
    ///
    /// If this function returns Err(Error::PacketTooLarge), have you tried to parse a packet
    /// with larger than configured maximum size. In this case, buffer is poisoned and not
    /// recoverable and user should disconnect the connection and attempt to re-create it
    pub fn parse(
        buf: &mut BytesMut,
        max_packet_size: usize,
    ) -> Result<Option<(Self, Bytes)>, Error> {
        if max_packet_size > crate::MAX_MQTT_PACKET_SIZE {
            return Err(Error::Generic(
                "max_packet_size is configured to be larger than it's maximum value",
            ));
        }

        let packet_len = buf.len();
        if packet_len < 2 {
            return Ok(None);
        }

        let orig = buf;
        let buf: &[u8] = &*orig;

        let control_packet_type = ControlPacketType::try_from_byte(buf[0])?;

        let mut peek: &[u8] = &buf[1..];
        let peek_length = peek.remaining();
        let remaining_length = match crate::util::read_variable_len_int(&mut peek) {
            Ok(length) => length as usize,
            Err(Error::NotEnoughData) => return Ok(None),
            Err(e) => return Err(e),
        };
        let variable_int_len = peek_length - peek.remaining();

        if 1 + variable_int_len + remaining_length > max_packet_size {
            return Err(Error::PacketTooLarge {
                packet_size: 1 + variable_int_len + remaining_length,
                max_configured_size: max_packet_size,
            });
        }

        if buf.remaining() < 1 + variable_int_len + remaining_length {
            return Ok(None);
        }

        orig.advance(1 + variable_int_len);
        let body = orig.split_to(remaining_length).freeze();

        Ok(Some((
            Self::new(control_packet_type, remaining_length),
            body,
        )))
    }

    pub fn write_to_buf(&self, buf: &mut impl BufMut) {
        buf.put_u8(self.control_packet_type.value() << 4 | self.control_packet_type.flags());
        crate::util::write_variable_len_int(self.remaining_length as u64, buf);
    }
}
#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn serialize_remaining_length() {
        const H: u8 = 1 << 4;

        fn cmp(header: FixedHeader, expected: &[u8]) {
            let mut buf = Vec::new();
            header.write_to_buf(&mut buf);
            assert_eq!(&buf[0..buf.len()], expected);
        }

        // 1 Byte
        cmp(FixedHeader::new(ControlPacketType::Connect, 1), &[H, 1]);
        cmp(FixedHeader::new(ControlPacketType::Connect, 127), &[H, 127]);

        // 2 Bytes
        cmp(
            FixedHeader::new(ControlPacketType::Connect, 128),
            &[H, 0x80, 0x01],
        );
        cmp(
            FixedHeader::new(ControlPacketType::Connect, 16383),
            &[H, 0xff, 0x7f],
        );

        // 3 Bytes
        cmp(
            FixedHeader::new(ControlPacketType::Connect, 16384),
            &[H, 0x80, 0x80, 0x01],
        );
        cmp(
            FixedHeader::new(ControlPacketType::Connect, 2097151),
            &[H, 0xff, 0xff, 0x7f],
        );

        // 4 Bytes
        cmp(
            FixedHeader::new(ControlPacketType::Connect, 2097152),
            &[H, 0x80, 0x80, 0x80, 0x01],
        );
        cmp(
            FixedHeader::new(ControlPacketType::Connect, 268435455),
            &[H, 0xff, 0xff, 0xff, 0x7f],
        );
    }

    #[test]
    fn serialize_header() {
        fn cmp(packet_type: ControlPacketType, type_value: u8, flags: u8) {
            let expected = [type_value << 4 | flags, 1];
            let mut buf = Vec::new();
            FixedHeader::new(packet_type, 1).write_to_buf(&mut buf);
            assert_eq!(&buf[0..buf.len()], expected);
        }

        cmp(ControlPacketType::Connect, 1, 0);
        cmp(ControlPacketType::ConnAck, 2, 0);

        cmp(
            ControlPacketType::Publish {
                dup: false,
                qos: Qos::AtMostOnce,
                retain: false,
            },
            3,
            0,
        );
        cmp(
            ControlPacketType::Publish {
                dup: true,
                qos: Qos::AtMostOnce,
                retain: false,
            },
            3,
            1 << 3,
        );
        cmp(
            ControlPacketType::Publish {
                dup: false,
                qos: Qos::AtLeastOnce,
                retain: false,
            },
            3,
            1 << 1,
        );
        cmp(
            ControlPacketType::Publish {
                dup: false,
                qos: Qos::ExactlyOnce,
                retain: false,
            },
            3,
            1 << 2,
        );
        cmp(
            ControlPacketType::Publish {
                dup: false,
                qos: Qos::AtMostOnce,
                retain: true,
            },
            3,
            1,
        );

        cmp(ControlPacketType::PubAck, 4, 0);
        cmp(ControlPacketType::PubRec, 5, 0);
        cmp(ControlPacketType::PubRel, 6, 1 << 1);
        cmp(ControlPacketType::PubComp, 7, 0);
        cmp(ControlPacketType::Subscribe, 8, 1 << 1);
        cmp(ControlPacketType::SubAck, 9, 0);
        cmp(ControlPacketType::Unsubscribe, 10, 1 << 1);
        cmp(ControlPacketType::UnsubscribeAck, 11, 0);
        cmp(ControlPacketType::PingReq, 12, 0);
        cmp(ControlPacketType::PingResp, 13, 0);
        cmp(ControlPacketType::Disconnect, 14, 0);
    }

    #[test]
    fn deserialize_header() {
        fn cmp(input: &[u8], expected_header: FixedHeader, expected_body: &[u8]) {
            let mut buf = BytesMut::from(input);
            let (header, body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
                .unwrap()
                .unwrap();
            assert_eq!(
                header, expected_header,
                "input={input:?} expected={expected_header:?} expected_body={body:?}"
            );
            assert_eq!(body, expected_body);
        }

        cmp(
            &[1 << 4, 1, 42],
            FixedHeader::new(ControlPacketType::Connect, 1),
            &[42],
        );
        cmp(
            &[2 << 4, 2, 1, 2],
            FixedHeader::new(ControlPacketType::ConnAck, 2),
            &[1, 2],
        );

        cmp(
            &[3 << 4, 3, 3, 4, 5],
            FixedHeader::new(
                ControlPacketType::Publish {
                    dup: false,
                    qos: Qos::AtMostOnce,
                    retain: false,
                },
                3,
            ),
            &[3, 4, 5],
        );
        cmp(
            &[3 << 4 | 1 << 3, 5, 0, 0, 0, 0, 0],
            FixedHeader::new(
                ControlPacketType::Publish {
                    dup: true,
                    qos: Qos::AtMostOnce,
                    retain: false,
                },
                5,
            ),
            &[0; 5],
        );

        cmp(
            &[3 << 4 | 1 << 1, 5, 0, 0, 0, 0, 0],
            FixedHeader::new(
                ControlPacketType::Publish {
                    dup: false,
                    qos: Qos::AtLeastOnce,
                    retain: false,
                },
                5,
            ),
            &[0; 5],
        );

        cmp(
            &[3 << 4 | 1 << 2, 5, 0, 0, 0, 0, 0],
            FixedHeader::new(
                ControlPacketType::Publish {
                    dup: false,
                    qos: Qos::ExactlyOnce,
                    retain: false,
                },
                5,
            ),
            &[0; 5],
        );

        cmp(
            &[3 << 4 | 1, 5, 0, 0, 0, 0, 0],
            FixedHeader::new(
                ControlPacketType::Publish {
                    dup: false,
                    qos: Qos::AtMostOnce,
                    retain: true,
                },
                5,
            ),
            &[0; 5],
        );

        cmp(
            &[4 << 4, 5, 0, 0, 0, 0, 0],
            FixedHeader::new(ControlPacketType::PubAck, 5),
            &[0; 5],
        );
        cmp(
            &[5 << 4, 5, 0, 0, 0, 0, 0],
            FixedHeader::new(ControlPacketType::PubRec, 5),
            &[0; 5],
        );
        cmp(
            &[6 << 4 | 1 << 1, 5, 0, 0, 0, 0, 0],
            FixedHeader::new(ControlPacketType::PubRel, 5),
            &[0; 5],
        );
        cmp(
            &[7 << 4, 5, 0, 0, 0, 0, 0],
            FixedHeader::new(ControlPacketType::PubComp, 5),
            &[0; 5],
        );
        cmp(
            &[8 << 4 | 1 << 1, 5, 0, 0, 0, 0, 0],
            FixedHeader::new(ControlPacketType::Subscribe, 5),
            &[0; 5],
        );
        cmp(
            &[9 << 4, 5, 0, 0, 0, 0, 0],
            FixedHeader::new(ControlPacketType::SubAck, 5),
            &[0; 5],
        );
        cmp(
            &[10 << 4 | 1 << 1, 5, 0, 0, 0, 0, 0],
            FixedHeader::new(ControlPacketType::Unsubscribe, 5),
            &[0; 5],
        );
        cmp(
            &[11 << 4, 5, 0, 0, 0, 0, 0],
            FixedHeader::new(ControlPacketType::UnsubscribeAck, 5),
            &[0; 5],
        );
        cmp(
            &[12 << 4, 5, 0, 0, 0, 0, 0],
            FixedHeader::new(ControlPacketType::PingReq, 5),
            &[0; 5],
        );
        cmp(
            &[13 << 4, 5, 0, 0, 0, 0, 0],
            FixedHeader::new(ControlPacketType::PingResp, 5),
            &[0; 5],
        );
        cmp(
            &[14 << 4, 5, 0, 0, 0, 0, 0],
            FixedHeader::new(ControlPacketType::Disconnect, 5),
            &[0; 5],
        );
    }

    #[test]
    fn partial_delivery() {
        let mut buf = BytesMut::from(&[1 << 4, 10][..]);
        assert!(FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .is_none())
    }

    /// PUBLISH, flags 0, remaining_length = 128 -> 2-byte VBI, 131 bytes total.
    fn two_byte_vbi_packet() -> Vec<u8> {
        let mut v = vec![0b0011_0000, 0x80, 0x01];
        v.extend((0..128).map(|i| i as u8));
        v
    }
    #[test]
    fn every_proper_prefix_is_incomplete_and_leaves_buffer_untouched() {
        let packet = two_byte_vbi_packet();
        for len in 0..packet.len() {
            let mut buf = BytesMut::from(&packet[..len]);
            let before = buf.clone();
            assert_eq!(
                FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE).unwrap(),
                None,
                "{len}-byte prefix must be reported as incomplete"
            );
            assert_eq!(
                buf, before,
                "parse consumed bytes on an incomplete packet (len={len})"
            );
        }
    }

    #[test]
    fn parse_is_independent_of_read_chunk_size() {
        // The invariant M1 depends on: a streaming reader must produce the same
        // result regardless of how the bytes were split across reads.
        let packet = two_byte_vbi_packet();
        for chunk in [1usize, 2, 3, 5, 64, 4096] {
            let mut buf = BytesMut::new();
            let mut got = None;
            for piece in packet.chunks(chunk) {
                buf.extend_from_slice(piece);
                if got.is_none() {
                    got = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE).unwrap();
                }
            }
            let (header, body) = got.unwrap_or_else(|| panic!("never completed at chunk={chunk}"));
            assert_eq!(header.remaining_length, 128);
            assert_eq!(body.len(), 128);
        }
    }
    #[test]
    fn truncated_variable_byte_integer_is_incomplete() {
        for prefix in [
            &[0x30u8][..],           // no VBI byte yet
            &[0x30, 0x80][..],       // 1 continuation byte
            &[0x30, 0x80, 0x80][..], // 2 continuation bytes
        ] {
            let mut buf = BytesMut::from(prefix);
            assert!(
                FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
                    .unwrap()
                    .is_none(),
                "{prefix:?} should be incomplete"
            );
        }
    }

    #[test]
    fn five_continuation_bytes_is_malformed() {
        // Guards against widening the Ok(None) net to cover real protocol errors.
        let mut buf = BytesMut::from(&[0x30, 0x80, 0x80, 0x80, 0x80][..]);
        assert!(FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE).is_err());
    }

    #[test]
    fn errors_decidable_from_the_first_byte_survive_truncation() {
        for bytes in [
            &[0x00u8, 0x00][..], // reserved packet type 0
            &[0x36, 0x00][..],   // PUBLISH with QoS = 3
            &[0x60, 0x00][..],   // PUBREL with flags 0 (must be 0b0010)
        ] {
            let mut buf = BytesMut::from(bytes);
            assert!(
                FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE).is_err(),
                "{bytes:?} should be a protocol error, not 'incomplete'"
            );
        }
    }

    #[test]
    fn oversized_declared_length_errors_even_when_incomplete() {
        // Pins that PacketTooLarge is checked before the completeness guard,
        // so nobody "fixes" it into Ok(None).
        // 120 = 0x78, so this is a single-byte VBI: declared packet size is 122.
        let mut buf = BytesMut::from(&[0x30, 120, 0x00][..]);
        assert!(matches!(
            FixedHeader::parse(&mut buf, 100),
            Err(Error::PacketTooLarge {
                packet_size: 122,
                max_configured_size: 100
            })
        ));
    }

    #[test]
    fn leftover_bytes_are_preserved_for_the_next_packet() {
        let mut buf = BytesMut::from(&[0x40, 0x02, 0xAA, 0xBB, 0xD0, 0x00][..]);

        let (h1, b1) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(h1.control_packet_type, ControlPacketType::PubAck);
        assert_eq!(&b1[..], &[0xAA, 0xBB]);
        assert_eq!(buf.len(), 2, "second packet must stay in the buffer");

        let (h2, b2) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(h2.control_packet_type, ControlPacketType::PingResp);
        assert!(b2.is_empty());
        assert!(buf.is_empty());
    }
}

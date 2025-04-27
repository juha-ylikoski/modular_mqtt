use std::io::{Read, Write};
use thiserror::Error;

use super::util::Qos;

#[derive(Debug, Error)]
pub enum FixedHeaderError {
    #[error("Not enough bytes to extract FixedHeader.")]
    NotEnoughBytes,
    #[error("ControlPacketType had reserved value {0}.")]
    ReservedControlPacketType(u8),
    #[error("Invalid Quality of service {0}.")]
    InvalidQos(u8),
    #[error("Flags did not follow specification {0} for type {1:?}.")]
    InvalidFlags(u8, ControlPacketType),
    #[error("IoError")]
    IoError(#[from] std::io::Error),
}

#[derive(Debug, PartialEq)]
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
        }
    }
    pub fn flags_dup(flags: u8) -> bool {
        ((flags & 0b1000) >> 3) == 1
    }
    pub fn flags_qos(flags: u8) -> Result<Qos, FixedHeaderError> {
        Qos::try_from((flags & 0b110) >> 1)
            .map_err(|_| FixedHeaderError::InvalidQos((flags & 0b110) >> 1))
    }
    pub fn flags_retain(flags: u8) -> bool {
        (flags & 1) == 1
    }
    pub fn try_from_byte(byte: u8) -> Result<Self, FixedHeaderError> {
        let packet_type = (byte & 0b11110000) >> 4;
        let flags = byte & 0b1111;

        macro_rules! verify {
            ($packet_type:expr) => {{
                let t = $packet_type;
                if $packet_type.flags() != flags {
                    Err(FixedHeaderError::InvalidFlags(flags, t))
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
            _ => Err(FixedHeaderError::ReservedControlPacketType(packet_type)),
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
    /// Will panic if remaining length is 0 or larger than 268 435 455.
    pub fn new(control_packet_type: ControlPacketType, remaining_length: usize) -> Self {
        if remaining_length > 268_435_455 {
            panic!("MQTT packet payload (dynamic header + payload) 268 435 455")
        }
        Self {
            control_packet_type,
            remaining_length,
        }
    }

    pub fn try_read(reader: &mut impl Read) -> Result<Self, FixedHeaderError> {
        let mut buf = [0u8; 1];
        reader.read_exact(&mut buf)?;
        let control_packet_type = ControlPacketType::try_from_byte(buf[0])?;

        // // Algorith based on http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718023
        let mut multiplier = 1;
        let mut remaining_length = 0;
        loop {
            reader.read_exact(&mut buf)?;
            let byte = buf[0];
            remaining_length += ((byte & 127) * multiplier) as usize;
            multiplier *= 128;
            if byte & 128 == 0 {
                break;
            }
        }
        Ok(Self::new(control_packet_type, remaining_length))
    }

    pub fn write_to_stream(mut self, writer: &mut impl Write) -> Result<usize, std::io::Error> {
        let mut length = 1;
        writer.write_all(&[
            self.control_packet_type.value() << 4 | self.control_packet_type.flags()
        ])?;

        if self.remaining_length == 0 {
            writer.write_all(&[0])?;
            return Ok(length + 1);
        }

        while self.remaining_length != 0 {
            let byte = (self.remaining_length % 128) as u8;
            self.remaining_length /= 128;
            if self.remaining_length > 0 {
                writer.write_all(&[byte | 128])?;
            } else {
                writer.write_all(&[byte])?;
            }
            length += 1;
        }

        Ok(length)
    }
}
#[cfg(test)]
mod test {
    use std::io::{BufReader, BufWriter};

    use super::*;

    #[test]
    fn serialize_remaining_length() {
        const H: u8 = 1 << 4;

        fn cmp(header: FixedHeader, expected: &[u8]) {
            let mut buf = Vec::new();
            let mut writer = BufWriter::new(&mut buf);
            let len = header.write_to_stream(&mut writer).unwrap();
            drop(writer);
            assert_eq!(&buf[0..len], expected);
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
            let mut writer = BufWriter::new(&mut buf);
            let len = FixedHeader::new(packet_type, 1)
                .write_to_stream(&mut writer)
                .unwrap();
            drop(writer);
            assert_eq!(&buf[0..len], expected);
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
        fn cmp(input: &[u8], expected: FixedHeader) {
            let mut reader = BufReader::new(input);
            let header = FixedHeader::try_read(&mut reader).unwrap();
            assert_eq!(header, expected);
        }

        cmp(
            &[1 << 4, 1],
            FixedHeader::new(ControlPacketType::Connect, 1),
        );
        cmp(
            &[2 << 4, 1],
            FixedHeader::new(ControlPacketType::ConnAck, 1),
        );

        cmp(
            &[3 << 4, 1],
            FixedHeader::new(
                ControlPacketType::Publish {
                    dup: false,
                    qos: Qos::AtMostOnce,
                    retain: false,
                },
                1,
            ),
        );
        cmp(
            &[3 << 4 | 1 << 3, 1],
            FixedHeader::new(
                ControlPacketType::Publish {
                    dup: true,
                    qos: Qos::AtMostOnce,
                    retain: false,
                },
                1,
            ),
        );

        cmp(
            &[3 << 4 | 1 << 1, 1],
            FixedHeader::new(
                ControlPacketType::Publish {
                    dup: false,
                    qos: Qos::AtLeastOnce,
                    retain: false,
                },
                1,
            ),
        );

        cmp(
            &[3 << 4 | 1 << 2, 1],
            FixedHeader::new(
                ControlPacketType::Publish {
                    dup: false,
                    qos: Qos::ExactlyOnce,
                    retain: false,
                },
                1,
            ),
        );

        cmp(
            &[3 << 4 | 1, 1],
            FixedHeader::new(
                ControlPacketType::Publish {
                    dup: false,
                    qos: Qos::AtMostOnce,
                    retain: true,
                },
                1,
            ),
        );

        cmp(&[4 << 4, 1], FixedHeader::new(ControlPacketType::PubAck, 1));
        cmp(&[5 << 4, 1], FixedHeader::new(ControlPacketType::PubRec, 1));
        cmp(
            &[6 << 4 | 1 << 1, 1],
            FixedHeader::new(ControlPacketType::PubRel, 1),
        );
        cmp(
            &[7 << 4, 1],
            FixedHeader::new(ControlPacketType::PubComp, 1),
        );
        cmp(
            &[8 << 4 | 1 << 1, 1],
            FixedHeader::new(ControlPacketType::Subscribe, 1),
        );
        cmp(&[9 << 4, 1], FixedHeader::new(ControlPacketType::SubAck, 1));
        cmp(
            &[10 << 4 | 1 << 1, 1],
            FixedHeader::new(ControlPacketType::Unsubscribe, 1),
        );
        cmp(
            &[11 << 4, 1],
            FixedHeader::new(ControlPacketType::UnsubscribeAck, 1),
        );
        cmp(
            &[12 << 4, 1],
            FixedHeader::new(ControlPacketType::PingReq, 1),
        );
        cmp(
            &[13 << 4, 1],
            FixedHeader::new(ControlPacketType::PingResp, 1),
        );
        cmp(
            &[14 << 4, 1],
            FixedHeader::new(ControlPacketType::Disconnect, 1),
        );
    }
}

use std::io::Write;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum PacketError {
    #[error("Received packet was malformed: {0}")]
    MalformedPacket(&'static str),

    #[error("IoError")]
    IoError(#[from] std::io::Error),

    #[error("Was unable to extract utf8 string from packet")]
    Utf8Error(#[from] std::str::Utf8Error),

    #[error("Could not read expected {0} bytes. Read only {1}")]
    MissingBytes(usize, usize),

    #[error("Mqtt topic cannot contain wildcard characters '#' or '+'")]
    InvalidMqttTopic,

    #[error("Invalid Quality of service {0}.")]
    InvalidQos(u8),
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum Qos {
    AtMostOnce = 0,
    AtLeastOnce = 1,
    ExactlyOnce = 2,
}

impl TryFrom<u8> for Qos {
    type Error = PacketError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Qos::AtMostOnce),
            1 => Ok(Qos::AtLeastOnce),
            2 => Ok(Qos::ExactlyOnce),
            _ => Err(PacketError::InvalidQos(value)),
        }
    }
}
impl From<QosPacketIdentifier> for Qos {
    fn from(value: QosPacketIdentifier) -> Self {
        match value {
            QosPacketIdentifier::AtMostOnce => Self::AtMostOnce,
            QosPacketIdentifier::AtLeastOnce(_) => Self::AtLeastOnce,
            QosPacketIdentifier::ExactlyOnce(_) => Self::ExactlyOnce,
        }
    }
}

/// Type for type safe construction of packet identifier
#[derive(Debug, PartialEq)]
pub enum QosPacketIdentifier {
    AtMostOnce,
    AtLeastOnce(u16),
    ExactlyOnce(u16),
}

/// Mqtt topic which does not contain invalid characters
#[derive(Debug, Clone, Copy)]
pub struct MqttTopic<'a>(pub(crate) &'a str);

impl<'a> TryFrom<&'a str> for MqttTopic<'a> {
    type Error = PacketError;

    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        if value.contains('#') || value.contains('+') {
            Err(PacketError::InvalidMqttTopic)
        } else {
            Ok(Self(value))
        }
    }
}

pub fn extract_str(data: &[u8]) -> Result<&str, PacketError> {
    let length = u16::from_be_bytes([data[0], data[1]]) as usize;
    if length == 0 {
        Ok("")
    } else {
        if data.len() < length + 2 {
            Err(PacketError::MissingBytes(length + 2, data.len()))
        } else {
            Ok(std::str::from_utf8(&data[2..2 + length])?)
        }
    }
}

pub fn write_str(string: &str, writer: &mut impl Write) -> Result<usize, std::io::Error> {
    let length = string.len();
    writer.write_all(&(length as u16).to_be_bytes())?;
    writer.write_all(string.as_bytes())?;
    Ok(length + 2)
}

pub fn extract_bytes(data: &[u8]) -> Result<&[u8], PacketError> {
    let length = u16::from_be_bytes([data[0], data[1]]) as usize;
    if length == 0 {
        Err(PacketError::MalformedPacket(
            "Packet bytes cannot have length of 0.",
        ))
    } else {
        if data.len() < length + 2 {
            Err(PacketError::MissingBytes(length + 2, data.len()))
        } else {
            Ok(&data[2..2 + length])
        }
    }
}

pub fn write_bytes(bytes: &[u8], writer: &mut impl Write) -> Result<usize, std::io::Error> {
    let length = bytes.len();
    writer.write_all(&(length as u16).to_be_bytes())?;
    writer.write_all(bytes)?;
    Ok(length + 2)
}
#[cfg(test)]
mod test {
    use std::io::BufWriter;

    use crate::mqtt_protocol::util::extract_str;

    use super::*;

    #[test]
    fn serialize_str() {
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        assert_eq!(write_str("foo", &mut writer).unwrap(), 5);
        drop(writer);
        assert_eq!(&buf, &[0, 3, b'f', b'o', b'o']);
    }
    #[test]
    fn serialize_str_long() {
        let input = [b'f'; 1000];
        let input = std::str::from_utf8(&input).unwrap();
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        assert_eq!(write_str(input, &mut writer).unwrap(), 1002);
        drop(writer);

        let mut expected = [b'f'; 1002];
        expected[0] = 0x03;
        expected[1] = 0xe8;
        assert_eq!(&buf, &expected);
    }
    #[test]
    fn deserialize_str() {
        let buf = [0, 3, b'f', b'o', b'o'];
        let out = extract_str(&buf[..]).unwrap();
        assert_eq!(out.len(), 3);
        assert_eq!(out, "foo");
    }
    #[test]
    fn deserialize_str_long() {
        let mut buf = [b'f'; 1002];
        buf[0] = 0x03;
        buf[1] = 0xe8;
        let expected = [b'f'; 1000];
        let expected = std::str::from_utf8(&expected).unwrap();
        let out = extract_str(&buf[..]).unwrap();
        assert_eq!(out.len(), 1000);
        assert_eq!(out, expected);
    }

    #[test]
    fn deserialize_malformed_str() {
        let buf = [0, 100, b'f'];
        if let Err(e) = extract_str(&buf[..]) {
            match e {
                PacketError::MissingBytes(_, _) => (),
                _ => panic!("Failed to detect bad input data!"),
            }
        } else {
            panic!("Should not get here");
        }
    }

    #[test]
    fn serialize_bytes() {
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        assert_eq!(write_bytes(&b"1234"[..], &mut writer).unwrap(), 6);
        drop(writer);
        assert_eq!(&buf, &[0, 4, b'1', b'2', b'3', b'4']);
    }
    #[test]
    fn serialize_bytes_long() {
        let input = [b'f'; 1000];
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        assert_eq!(write_bytes(&input[..], &mut writer).unwrap(), 1002);
        drop(writer);

        let mut expected = [b'f'; 1002];
        expected[0] = 0x03;
        expected[1] = 0xe8;
        assert_eq!(&buf, &expected);
    }
    #[test]
    fn deserialize_bytes() {
        let buf = [0, 3, b'f', b'o', b'o'];
        let out = extract_bytes(&buf[..]).unwrap();
        assert_eq!(out.len(), 3);
        assert_eq!(out, b"foo");
    }
    #[test]
    fn deserialize_bytes_long() {
        let mut buf = [b'f'; 1002];
        buf[0] = 0x03;
        buf[1] = 0xe8;
        let expected = [b'f'; 1000];
        let out = extract_bytes(&buf[..]).unwrap();
        assert_eq!(out.len(), 1000);
        assert_eq!(out, expected);
    }

    #[test]
    fn deserialize_malformed_bytes() {
        let buf = [0, 100, b'f'];
        if let Err(e) = extract_bytes(&buf[..]) {
            match e {
                PacketError::MissingBytes(_, _) => (),
                _ => panic!("Failed to detect bad input data!"),
            }
        } else {
            panic!("Should not get here");
        }
    }
}

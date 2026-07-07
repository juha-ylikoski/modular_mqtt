use std::io::Write;

use crate::{Error, MalformedPacket};

#[derive(Debug)]
pub enum PacketError {
    MalformedPacket(&'static str),
    Utf8Error(std::str::Utf8Error),
    MissingBytes { expected: usize, got: usize },
    InvalidMqttTopic,
    InvalidQos(u8),
    InvalidFixedHeader(super::FixedHeaderError),
}

impl std::fmt::Display for PacketError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PacketError::MalformedPacket(packet) => {
                f.write_fmt(format_args!("Received packet was malformed: {packet}"))
            }
            PacketError::Utf8Error(utf8_error) => f.write_fmt(format_args!(
                "Was unable to extract utf8 string from packet: {utf8_error}"
            )),
            PacketError::MissingBytes { expected, got } => f.write_fmt(format_args!(
                "Could not read expected {expected} bytes. Read only {got}"
            )),
            PacketError::InvalidMqttTopic => {
                f.write_str("Mqtt topic cannot contain wildcard characters '#' or '+'")
            }
            PacketError::InvalidQos(qos) => {
                f.write_fmt(format_args!("Invalid Quality of service {qos}."))
            }
            PacketError::InvalidFixedHeader(error) => f.write_fmt(format_args!(
                "Invalid fixed header read from stream: {error}"
            )),
        }
    }
}

impl std::error::Error for PacketError {}

#[derive(Copy, Clone, Debug, PartialEq, Default)]
pub enum Qos {
    #[default]
    AtMostOnce = 0,
    AtLeastOnce = 1,
    ExactlyOnce = 2,
}

impl TryFrom<u8> for Qos {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Qos::AtMostOnce),
            1 => Ok(Qos::AtLeastOnce),
            2 => Ok(Qos::ExactlyOnce),
            _ => Err(MalformedPacket::InvalidQos(value).into()),
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
#[derive(Debug, Clone, PartialEq)]
pub struct MqttTopic(pub(crate) String);

impl<'a> TryFrom<&'a str> for MqttTopic {
    type Error = Error;

    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        if value.contains('#') || value.contains('+') {
            Err(MalformedPacket::InvalidMqttTopic.into())
        } else {
            Ok(Self(value.to_string()))
        }
    }
}

pub fn extract_str(data: &[u8]) -> Result<&str, Error> {
    if data.len() < 2 {
        return Err(MalformedPacket::new("Packet too short to read string"));
    }
    let length = u16::from_be_bytes([data[0], data[1]]) as usize;
    if length == 0 {
        Ok("")
    } else if data.len() < length + 2 {
        Err(MalformedPacket::new("Packet too short to read string"))
    } else {
        std::str::from_utf8(&data[2..2 + length]).map_err(|e| MalformedPacket::Utf8Error(e).into())
    }
}

pub fn write_str(string: &str, writer: &mut impl Write) -> Result<usize, std::io::Error> {
    let length = string.len();
    writer.write_all(&(length as u16).to_be_bytes())?;
    writer.write_all(string.as_bytes())?;
    Ok(length + 2)
}

pub fn extract_bytes(data: &[u8]) -> Result<&[u8], Error> {
    let length = u16::from_be_bytes([data[0], data[1]]) as usize;
    if length == 0 {
        Err(MalformedPacket::new(
            "Packet bytes cannot have length of 0.",
        ))
    } else if data.len() < length + 2 {
        Err(MalformedPacket::new("Packet too short to read bytes"))
    } else {
        Ok(&data[2..2 + length])
    }
}

/// Write mqtt variable length integer
pub fn write_variable_len_int(
    mut value: u64,
    writer: &mut impl Write,
) -> Result<usize, std::io::Error> {
    let mut length = 0;
    if value == 0 {
        writer.write_all(&[0])?;
        return Ok(1);
    }

    while value != 0 {
        let byte = (value % 128) as u8;
        value /= 128;
        if value > 0 {
            writer.write_all(&[byte | 128])?;
        } else {
            writer.write_all(&[byte])?;
        }
        length += 1;
    }
    Ok(length)
}

/// Write mqtt variable length integer
pub fn variable_len_int_size(mut value: usize) -> usize {
    let mut length = 0;
    if value == 0 {
        return 1;
    }

    while value != 0 {
        value /= 128;
        length += 1;
    }
    length
}

/// Read mqtt variable length integer
///
/// Algorithm based on http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718023
pub fn read_variable_len_int(packet: &[u8]) -> Result<(usize, u64), crate::Error> {
    let packet_len = packet.len();
    let mut multiplier: u64 = 1;
    let mut value = 0;
    let mut i = 0;
    loop {
        if packet_len <= i {
            return Err(Error::NotEnoughData);
        }
        let byte = packet[i];
        value += (byte & 127) as u64 * multiplier;
        multiplier *= 128;
        i += 1;
        if byte & 128 == 0 {
            break;
        }
    }
    Ok((i, value))
}

#[cfg(test)]
mod test {
    use std::io::BufWriter;

    use super::extract_str;

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
                Error::MalformedPacket(_) => (),
                _ => panic!("Failed to detect bad input data!"),
            }
        } else {
            panic!("Should not get here");
        }
    }

    #[test]
    fn deserialize_malformed_bytes() {
        let buf = [0, 100, b'f'];
        if let Err(e) = extract_bytes(&buf[..]) {
            match e {
                Error::MalformedPacket(_) => (),
                _ => panic!("Failed to detect bad input data!"),
            }
        } else {
            panic!("Should not get here");
        }
    }
}

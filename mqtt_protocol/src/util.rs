use std::io::Write;

use bytes::{Buf, Bytes};

use crate::{Error, MalformedPacket};

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

impl TryFrom<String> for MqttTopic {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.contains('#') || value.contains('+') {
            Err(MalformedPacket::InvalidMqttTopic.into())
        } else {
            Ok(Self(value))
        }
    }
}

pub fn extract_str(data: &mut Bytes) -> Result<String, Error> {
    if data.remaining() < 2 {
        return Err(MalformedPacket::new("Packet too short to read string"));
    }
    let length = data.try_get_u16()? as usize;
    if length == 0 {
        Ok(String::new())
    } else if data.remaining() < length {
        Err(MalformedPacket::new("Packet too short to read string"))
    } else {
        let out = match std::str::from_utf8(&data[0..length]) {
            Ok(s) => Ok(s.to_string()),
            Err(e) => Err(MalformedPacket::Utf8Error(e).into()),
        };
        data.advance(length);
        out
    }
}

pub fn write_str(string: &str, writer: &mut impl Write) -> Result<usize, std::io::Error> {
    let length = string.len();
    writer.write_all(&(length as u16).to_be_bytes())?;
    writer.write_all(string.as_bytes())?;
    Ok(length + 2)
}

pub fn extract_bytes(data: &mut Bytes) -> Result<Bytes, Error> {
    let length = data.try_get_u16()? as usize;
    if length == 0 {
        Err(MalformedPacket::new(
            "Packet bytes cannot have length of 0.",
        ))
    } else if data.len() < length {
        Err(MalformedPacket::new("Packet too short to read bytes"))
    } else {
        let out = data.slice(0..length);
        data.advance(length);
        Ok(out)
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
pub fn read_variable_len_int(packet: &mut impl bytes::Buf) -> Result<u64, crate::Error> {
    let mut multiplier: u64 = 1;
    let mut value = 0;
    loop {
        if packet.remaining() == 0 {
            return Err(Error::NotEnoughData);
        }
        let byte = packet.get_u8();

        value += (byte & 127) as u64 * multiplier;
        multiplier *= 128;

        if byte & 128 == 0 {
            break;
        }
        if multiplier > 128 * 128 * 128 {
            return Err(MalformedPacket::new("Malformed Variable Byte Integer"));
        }
    }
    Ok(value)
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
        let mut buf = Bytes::from_static(&[0, 3, b'f', b'o', b'o']);
        let out = extract_str(&mut buf).unwrap();
        assert_eq!(out.len(), 3);
        assert_eq!(out, "foo");
    }
    #[test]
    fn deserialize_str_long() {
        let mut buf = vec![b'f'; 1002];
        buf[0] = 0x03;
        buf[1] = 0xe8;
        let mut buf = Bytes::from(buf);
        let expected = [b'f'; 1000];
        let expected = std::str::from_utf8(&expected).unwrap();
        let out = extract_str(&mut buf).unwrap();
        assert_eq!(out.len(), 1000);
        assert_eq!(out, expected);
    }

    #[test]
    fn deserialize_malformed_str() {
        let mut buf = Bytes::from_static(&[0, 100, b'f']);
        if let Err(e) = extract_str(&mut buf) {
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
        let mut buf = Bytes::from_static(&[0, 100, b'f']);
        if let Err(e) = extract_bytes(&mut buf) {
            match e {
                Error::MalformedPacket(_) => (),
                _ => panic!("Failed to detect bad input data!"),
            }
        } else {
            panic!("Should not get here");
        }
    }
}

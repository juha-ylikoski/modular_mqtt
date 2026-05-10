use std::io::Write;

use super::fixed_header::FixedHeader;
use crate::util::PacketError;

#[derive(Debug, PartialEq)]
pub enum ConnectRc {
    /// Connection accepted
    Accepted = 0,
    /// The Server does not support the level of the MQTT protocol requested by the Client
    UnacceptableVersion = 1,
    /// The Client identifier is correct UTF-8 but not allowed by the Server
    IdentifierRejected = 2,
    /// The Network Connection has been made but the MQTT service is unavailable
    ServerUnavailable = 3,
    /// The data in the user name or password is malformed
    BadUsernamePassword = 4,
    /// The Client is not authorized to connect
    Refused = 5,
}

impl From<ConnectRc> for u8 {
    fn from(value: ConnectRc) -> Self {
        value as u8
    }
}

impl TryFrom<u8> for ConnectRc {
    type Error = PacketError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Accepted),
            1 => Ok(Self::UnacceptableVersion),
            2 => Ok(Self::IdentifierRejected),
            3 => Ok(Self::ServerUnavailable),
            4 => Ok(Self::BadUsernamePassword),
            5 => Ok(Self::Refused),
            _ => Err(PacketError::MalformedPacket(
                "Mqtt connect return code was not one of 0, 1, 2, 3 or 5",
            )),
        }
    }
}

#[derive(Debug, PartialEq)]
/// The CONNACK Packet is the packet sent by the Server in response to a CONNECT Packet received from a Client. The first packet sent from the Server to the Client MUST be a CONNACK Packet [MQTT-3.2.0-1].
pub struct ConnAck {
    pub fixed_header: FixedHeader,
    pub session_present: bool,
    pub connect_rc: ConnectRc,
}

impl ConnAck {
    pub fn new(session_present: bool, connect_rc: ConnectRc) -> Self {
        Self {
            fixed_header: FixedHeader::new(super::fixed_header::ControlPacketType::ConnAck, 2),
            session_present,
            connect_rc,
        }
    }
    pub fn try_read(header: FixedHeader, data: &[u8]) -> Result<Self, PacketError> {
        if data.len() < 2 {
            return Err(PacketError::MissingBytes {
                expected: 2,
                got: data.len(),
            });
        }
        let flags = data[0];
        let connect_rc = data[1].try_into()?;
        if flags & (!1) != 0 {
            return Err(PacketError::MalformedPacket(
                "All reserved bits have to be 0 in CONNACK",
            ));
        }
        Ok(Self {
            fixed_header: header,
            session_present: (flags & 1) == 1,
            connect_rc,
        })
    }
    pub fn write_to_stream(self, writer: &mut impl Write) -> Result<usize, std::io::Error> {
        let length = self.fixed_header.write_to_stream(writer)?;
        writer.write_all(&[self.session_present as u8, self.connect_rc.into()])?;
        writer.flush()?;
        Ok(length + 2)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::io::BufWriter;
    use std::io::{BufReader, Read};

    #[test]
    fn serialize() {
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        let msg = ConnAck::new(false, ConnectRc::Accepted);
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
        assert_eq!(&buf, &[32, 2, 0, 0]);
    }

    #[test]
    fn serialize_session_present() {
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        let msg = ConnAck::new(true, ConnectRc::Accepted);
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
        assert_eq!(&buf, &[32, 2, 1, 0]);
    }
    #[test]
    fn serialize_rc() {
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        let msg = ConnAck::new(false, ConnectRc::Refused);
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
        assert_eq!(&buf, &[32, 2, 0, 5]);
    }

    #[test]
    fn deserialize() {
        let msg = [32, 2, 0, 0];
        let expected = ConnAck::new(false, ConnectRc::Accepted);
        let mut reader = BufReader::new(&msg[..]);
        let header = FixedHeader::try_read_sync(&mut reader).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(ConnAck::try_read(header, &data[..]).unwrap(), expected);
    }

    #[test]
    fn deserialize_session_present() {
        let msg = [32, 2, 1, 0];
        let expected = ConnAck::new(true, ConnectRc::Accepted);
        let mut reader = BufReader::new(&msg[..]);
        let header = FixedHeader::try_read_sync(&mut reader).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(ConnAck::try_read(header, &data[..]).unwrap(), expected);
    }
    #[test]
    fn deserialize_rc() {
        let msg = [32, 2, 0, 4];
        let expected = ConnAck::new(false, ConnectRc::BadUsernamePassword);
        let mut reader = BufReader::new(&msg[..]);
        let header = FixedHeader::try_read_sync(&mut reader).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(ConnAck::try_read(header, &data[..]).unwrap(), expected);
    }
}

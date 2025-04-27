use std::io::Write;

use crate::util::PacketError;

use super::fixed_header::{ControlPacketType, FixedHeader};

#[derive(Debug, PartialEq)]
pub enum SubRc {
    SuccessQos0 = 0,
    SuccessQos1 = 1,
    SuccessQos2 = 2,
    Failure = 0x80,
}

impl TryFrom<u8> for SubRc {
    type Error = PacketError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(SubRc::SuccessQos0),
            1 => Ok(SubRc::SuccessQos1),
            2 => Ok(SubRc::SuccessQos2),
            0x80 => Ok(SubRc::Failure),
            _ => Err(PacketError::MalformedPacket(
                "Invalid subscribe return code",
            )),
        }
    }
}

impl SubRc {
    pub fn is_succress(&self) -> bool {
        *self != SubRc::Failure
    }
}

#[derive(Debug, PartialEq)]
/// A SUBACK Packet is sent by the Server to the Client to confirm receipt and processing of a SUBSCRIBE Packet.
pub struct SubAck {
    fixed_header: FixedHeader,
    packet_identifier: u16,
    return_codes: Vec<SubRc>,
}

impl SubAck {
    pub fn new(packet_identifier: u16, return_codes: Vec<SubRc>) -> Self {
        Self {
            fixed_header: FixedHeader::new(ControlPacketType::SubAck, 2 + return_codes.len()),
            packet_identifier,
            return_codes,
        }
    }
    pub fn try_read(header: FixedHeader, data: &[u8]) -> Result<Self, PacketError> {
        let packet_identifier = u16::from_be_bytes([data[0], data[1]]);
        let mut remaining = header.remaining_length - 2;
        let mut index = 2;
        let mut return_codes = Vec::new();
        while remaining > 0 {
            return_codes.push(SubRc::try_from(data[index])?);
            index += 1;
            remaining -= 1;
        }
        Ok(Self {
            fixed_header: header,
            packet_identifier,
            return_codes,
        })
    }
    pub fn write_to_stream(self, writer: &mut impl Write) -> Result<usize, std::io::Error> {
        let mut length = self.fixed_header.write_to_stream(writer)?;
        writer.write_all(&[
            ((self.packet_identifier & 0xff00) >> 8) as u8,
            (self.packet_identifier & 0xff) as u8,
        ])?;
        length += 2;
        for rc in self.return_codes {
            writer.write_all(&[rc as u8])?;
            length += 1;
        }

        writer.flush()?;
        Ok(length)
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
        let msg = SubAck::new(
            42,
            vec![SubRc::SuccessQos0, SubRc::SuccessQos1, SubRc::Failure],
        );
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
        assert_eq!(&buf, &[144, 5, 0, 42, 0, 1, 0x80]);
    }

    #[test]
    fn deserialize() {
        let msg = [144, 5, 0, 42, 0, 1, 0x80];
        let expected = SubAck::new(
            42,
            vec![SubRc::SuccessQos0, SubRc::SuccessQos1, SubRc::Failure],
        );
        let mut reader = BufReader::new(&msg[..]);
        let header = FixedHeader::try_read(&mut reader).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(SubAck::try_read(header, &data[..]).unwrap(), expected);
    }
}

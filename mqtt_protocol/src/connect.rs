use std::io::Write;
use std::marker::PhantomData;

use crate::fixed_header::ControlPacketType;

use super::fixed_header::FixedHeader;
use super::util::{extract_bytes, extract_str, write_str, PacketError, Qos};

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct MqttVersion3_1_1;

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct MqttLastWill<'a> {
    topic: &'a str,
    payload: &'a [u8],
    retain: bool,
    qos: Qos,
}

impl<'a> MqttLastWill<'a> {
    pub fn new(topic: &'a str, payload: &'a [u8], retain: bool, qos: Qos) -> Self {
        Self {
            topic,
            payload,
            retain,
            qos,
        }
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
/// After a Network Connection is established by a Client to a Server, the first Packet sent from the Client to the Server MUST be a CONNECT Packet [MQTT-3.1.0-1].
pub struct Connect<'a, V> {
    fixed_header: FixedHeader,
    // length: u16,
    protocol_level: PhantomData<V>,

    /// This bit specifies the handling of the Session state.
    ///
    /// The Client and Server can store Session state to enable reliable messaging to continue across a sequence of Network Connections. This bit is used to control the lifetime of the Session state.
    ///
    ///If CleanSession is set to 0, the Server MUST resume communications with the Client based on state from the current Session (as identified by the Client identifier). If there is no Session associated with the Client identifier the Server MUST create a new Session. The Client and Server MUST store the Session after the Client and Server are disconnected [MQTT-3.1.2-4]. After the disconnection of a Session that had CleanSession set to 0, the Server MUST store further QoS 1 and QoS 2 messages that match any subscriptions that the client had at the time of disconnection as part of the Session state [MQTT-3.1.2-5]. It MAY also store QoS 0 messages that meet the same criteria.
    ///
    /// If CleanSession is set to 1, the Client and Server MUST discard any previous Session and start a new one. This Session lasts as long as the Network Connection. State data associated with this Session MUST NOT be reused in any subsequent Session [MQTT-3.1.2-6].
    ///
    /// <http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718030>
    pub clean_session: bool,

    /// The Keep Alive is a time interval measured in seconds. Expressed as a 16-bit word, it is the maximum time interval that is permitted to elapse between the point at which the Client finishes transmitting one Control Packet and the point it starts sending the next. It is the responsibility of the Client to ensure that the interval between Control Packets being sent does not exceed the Keep Alive value. In the absence of sending any other Control Packets, the Client MUST send a PINGREQ Packet [MQTT-3.1.2-23].
    ///
    /// A Keep Alive value of zero (0) has the effect of turning off the keep alive mechanism. This means that, in this case, the Server is not required to disconnect the Client on the grounds of inactivity.
    ///
    /// <http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718030>
    keep_alive: u16,

    ///  If the Will Flag is set to 1 this indicates that, if the Connect request is accepted, a Will Message MUST be stored on the Server and associated with the Network Connection. The Will Message MUST be published when the Network Connection is subsequently closed unless the Will Message has been deleted by the Server on receipt of a DISCONNECT Packet [MQTT-3.1.2-8].
    ///
    /// <http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718030>
    will: Option<MqttLastWill<'a>>,
    /// The Client Identifier (ClientId) identifies the Client to the Server. Each Client connecting to the Server has a unique ClientId. The ClientId MUST be used by Clients and by Servers to identify state that they hold relating to this MQTT Session between the Client and the Server [MQTT-3.1.3-2].
    ///
    /// <http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718030>
    client_identifier: &'a str,
    username: Option<&'a str>,
    password: Option<&'a [u8]>,
}

#[cfg_attr(test, derive(PartialEq))]
enum Flags {
    CleanSession = 1 << 1,
    Will = 1 << 2,
    WilLRetain = 1 << 5,
    Password = 1 << 6,
    Username = 1 << 7,
}
impl Flags {
    fn flag_set(self, flags: u8) -> bool {
        flags & (self as u8) != 0
    }
}
impl From<Flags> for u8 {
    fn from(value: Flags) -> Self {
        value as u8
    }
}

impl<'a> Connect<'a, MqttVersion3_1_1> {
    pub fn new_v3(
        clean_session: bool,
        keep_alive: u16,
        client_identifier: &'a str,
        will: Option<MqttLastWill<'a>>,
        username: Option<&'a str>,
        password: Option<&'a [u8]>,
    ) -> Self {
        let variable_header_len = 10;
        let payload_len = {
            let mut len = client_identifier.len() + 2;
            if let Some(will) = &will {
                len += will.topic.len() + 2 + will.payload.len() + 2;
            }
            if let Some(username) = &username {
                len += username.len() + 2;
            }
            if let Some(password) = &password {
                len += password.len() + 2;
            }
            len
        };
        let fixed_header = FixedHeader::new(
            ControlPacketType::Connect,
            variable_header_len + payload_len,
        );
        Self {
            fixed_header,
            protocol_level: PhantomData,
            clean_session,
            keep_alive,
            client_identifier,
            will,
            username,
            password,
        }
    }
    pub fn try_read(header: FixedHeader, data: &'a [u8]) -> Result<Self, PacketError> {
        let protocol_length = u16::from_be_bytes([data[0], data[1]]) as usize;
        if data[2..2 + protocol_length] != b"MQTT"[..] {
            return Err(PacketError::MalformedPacket(
                "Connect packet bytes 3..6 did not contain 'MQTT'.",
            ));
        }
        let protocol_level = data[6];
        if protocol_level != 4 {
            return Err(PacketError::MalformedPacket(
                "MQTT protocol level was not '4' which is expected for MQTT 3.1.1",
            ));
        }
        let flags = data[7];
        let keep_alive = u16::from_be_bytes([data[8], data[9]]);

        // Payload
        let client_identifier = extract_str(&data[10..])?;

        let mut next_index = 10 + 2 + client_identifier.len();

        let will = if Flags::Will.flag_set(flags) {
            let topic = extract_str(&data[next_index..])?;
            next_index = next_index + 2 + topic.len();
            let payload = extract_bytes(&data[next_index..])?;
            next_index += 2 + payload.len();
            Some(MqttLastWill {
                topic,
                payload,
                retain: Flags::WilLRetain.flag_set(flags),
                qos: Qos::try_from((flags & 0b00011000) >> 3).map_err(|_| {
                    PacketError::MalformedPacket("Quality of service was not 0, 1 or 2.")
                })?,
            })
        } else {
            None
        };

        let username = if Flags::Username.flag_set(flags) {
            let username = extract_str(&data[next_index..])?;
            next_index += 2 + username.len();
            Some(username)
        } else {
            None
        };
        let password = if Flags::Password.flag_set(flags) {
            let password = extract_bytes(&data[next_index..])?;
            Some(password)
        } else {
            None
        };

        Ok(Self {
            fixed_header: header,
            protocol_level: PhantomData,
            clean_session: Flags::CleanSession.flag_set(flags),
            keep_alive,
            username,
            password,
            will,
            client_identifier,
        })
    }
    pub fn write_to_stream(self, writer: &mut impl Write) -> Result<usize, std::io::Error> {
        let mut length = self.fixed_header.write_to_stream(writer)?;
        let mut flags = 0;
        if self.username.is_some() {
            flags |= u8::from(Flags::Username);
        }
        if self.password.is_some() {
            flags |= u8::from(Flags::Password);
        }
        if let Some(will) = &self.will {
            flags |= u8::from(Flags::Will) | (will.qos as u8) << 3;

            if will.retain {
                flags |= u8::from(Flags::WilLRetain);
            }
        }
        if self.clean_session {
            flags |= u8::from(Flags::CleanSession);
        }

        writer.write_all(&[
            // Protocol name length
            0,
            4,
            // Protocol name
            b'M',
            b'Q',
            b'T',
            b'T',
            // Protocol level
            4,
            flags,
            ((self.keep_alive & 0xff00) >> 8) as u8,
            (self.keep_alive & 0xff) as u8,
        ])?;
        length += 10;

        // Payload
        length += write_str(self.client_identifier, writer)?;

        if let Some(will) = self.will {
            length += write_str(will.topic, writer)?;
            let pl_len = will.payload.len();
            writer.write_all(&(pl_len as u16).to_be_bytes())?;
            writer.write_all(will.payload)?;
            length += 2 + pl_len;
        }
        if let Some(username) = self.username {
            length += write_str(username, writer)?;
        }
        if let Some(password) = self.password {
            let pl_len = password.len();
            writer.write_all(&(pl_len as u16).to_be_bytes())?;
            writer.write_all(password)?;
            length += 2 + pl_len;
        }
        writer.flush()?;
        Ok(length)
    }
}

#[cfg(test)]
mod test_ser {
    use std::io::BufWriter;

    use crate::connect::MqttLastWill;

    use super::*;

    #[test]
    fn v3() {
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        let msg = Connect::new_v3(false, 0, "client", None, None, None);
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
        assert_eq!(
            &buf,
            &[
                16, 18, 0, 4, b'M', b'Q', b'T', b'T', // Protocol name
                4,    // Protocol level
                0,    // flags
                0, 0, // Keep alive
                0, 6, b'c', b'l', b'i', b'e', b'n', b't' // Client identifier
            ]
        );
    }
    #[test]
    fn v3_will() {
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        let msg = Connect::new_v3(
            false,
            0,
            "client2",
            Some(MqttLastWill::new(
                "will",
                b"payload",
                true,
                Qos::ExactlyOnce,
            )),
            None,
            None,
        );
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
        assert_eq!(
            &buf,
            &[
                16,
                34,
                // Protocol name
                0,
                4,
                b'M',
                b'Q',
                b'T',
                b'T',
                // Protocol level
                4,
                // flags
                (1 << 5) | (2 << 3) | (1 << 2),
                // Keep alive
                0,
                0,
                // Client identifier
                0,
                7,
                b'c',
                b'l',
                b'i',
                b'e',
                b'n',
                b't',
                b'2',
                // Will topic
                0,
                4,
                b'w',
                b'i',
                b'l',
                b'l',
                // Will payload
                0,
                7,
                b'p',
                b'a',
                b'y',
                b'l',
                b'o',
                b'a',
                b'd',
            ]
        );
    }
    #[test]
    fn v3_username() {
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        let msg = Connect::new_v3(false, 0, "client", None, Some("username"), None);
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
        assert_eq!(
            &buf,
            &[
                16,
                28,
                // Protocol name
                0,
                4,
                b'M',
                b'Q',
                b'T',
                b'T',
                // Protocol level
                4,
                // flags
                1 << 7,
                // Keep alive
                0,
                0,
                // Client identifier
                0,
                6,
                b'c',
                b'l',
                b'i',
                b'e',
                b'n',
                b't',
                // Username
                0,
                8,
                b'u',
                b's',
                b'e',
                b'r',
                b'n',
                b'a',
                b'm',
                b'e',
            ]
        );
    }
    #[test]
    fn v3_password() {
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        let msg = Connect::new_v3(false, 0, "client", None, None, Some(b"password"));
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
        assert_eq!(
            &buf,
            &[
                16,
                28,
                // Protocol name
                0,
                4,
                b'M',
                b'Q',
                b'T',
                b'T',
                // Protocol level
                4,
                // flags
                1 << 6,
                // Keep alive
                0,
                0,
                // Client identifier
                0,
                6,
                b'c',
                b'l',
                b'i',
                b'e',
                b'n',
                b't',
                // Password
                0,
                8,
                b'p',
                b'a',
                b's',
                b's',
                b'w',
                b'o',
                b'r',
                b'd',
            ]
        );
    }
    #[test]
    fn v3_username_and_password() {
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        let msg = Connect::new_v3(
            false,
            0,
            "client",
            None,
            Some("username"),
            Some(b"password"),
        );
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
        assert_eq!(
            &buf,
            &[
                16,
                38,
                // Protocol name
                0,
                4,
                b'M',
                b'Q',
                b'T',
                b'T',
                // Protocol level
                4,
                // flags
                1 << 6 | 1 << 7,
                // Keep alive
                0,
                0,
                // Client identifier
                0,
                6,
                b'c',
                b'l',
                b'i',
                b'e',
                b'n',
                b't',
                // Username
                0,
                8,
                b'u',
                b's',
                b'e',
                b'r',
                b'n',
                b'a',
                b'm',
                b'e',
                // Password
                0,
                8,
                b'p',
                b'a',
                b's',
                b's',
                b'w',
                b'o',
                b'r',
                b'd',
            ]
        );
    }
    #[test]
    fn v3_clean_session() {
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        let msg = Connect::new_v3(true, 0, "client", None, None, None);
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
        assert_eq!(
            &buf,
            &[
                16,
                18,
                // Protocol name
                0,
                4,
                b'M',
                b'Q',
                b'T',
                b'T',
                // Protocol level
                4,
                // flags
                1 << 1,
                // Keep alive
                0,
                0,
                // Client identifier
                0,
                6,
                b'c',
                b'l',
                b'i',
                b'e',
                b'n',
                b't',
            ]
        );
    }
    #[test]
    fn v3_client_id() {
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        let msg = Connect::new_v3(false, 1800, "client", None, None, None);
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
        assert_eq!(
            &buf,
            &[
                16, 18, 0, 4, b'M', b'Q', b'T', b'T', // Protocol name
                4,    // Protocol level
                0,    // flags
                0x07, 0x08, // Keep alive
                0, 6, b'c', b'l', b'i', b'e', b'n', b't', // Client identifier
            ]
        );
    }
}

#[cfg(test)]
mod test_de {
    use std::io::{BufReader, Read};

    use super::*;

    #[test]
    fn v3() {
        let msg = [
            16, 18, 0, 4, b'M', b'Q', b'T', b'T', // Protocol name
            4,    // Protocol level
            0,    // flags
            0, 0, // Keep alive
            0, 6, b'c', b'l', b'i', b'e', b'n', b't', // Client identifier
        ];
        let expected = Connect::new_v3(false, 0, "client", None, None, None);
        let mut reader = BufReader::new(&msg[..]);
        let header = FixedHeader::try_read(&mut reader).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(Connect::try_read(header, &data[..]).unwrap(), expected);
    }
    #[test]
    fn v3_will() {
        let msg = [
            16,
            34,
            // Protocol name
            0,
            4,
            b'M',
            b'Q',
            b'T',
            b'T',
            // Protocol level
            4,
            // flags
            (1 << 5) | (2 << 3) | (1 << 2),
            // Keep alive
            0,
            0,
            // Client identifier
            0,
            7,
            b'c',
            b'l',
            b'i',
            b'e',
            b'n',
            b't',
            b'2',
            // Will topic
            0,
            4,
            b'w',
            b'i',
            b'l',
            b'l',
            // Will payload
            0,
            7,
            b'p',
            b'a',
            b'y',
            b'l',
            b'o',
            b'a',
            b'd',
        ];
        let expected = Connect::new_v3(
            false,
            0,
            "client2",
            Some(MqttLastWill::new(
                "will",
                b"payload",
                true,
                Qos::ExactlyOnce,
            )),
            None,
            None,
        );
        let mut reader = BufReader::new(&msg[..]);
        let header = FixedHeader::try_read(&mut reader).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(Connect::try_read(header, &data[..]).unwrap(), expected);
    }
    #[test]
    fn v3_username() {
        let msg = [
            16,
            28,
            // Protocol name
            0,
            4,
            b'M',
            b'Q',
            b'T',
            b'T',
            // Protocol level
            4,
            // flags
            1 << 7,
            // Keep alive
            0,
            0,
            // Client identifier
            0,
            6,
            b'c',
            b'l',
            b'i',
            b'e',
            b'n',
            b't',
            // Username
            0,
            8,
            b'u',
            b's',
            b'e',
            b'r',
            b'n',
            b'a',
            b'm',
            b'e',
        ];
        let expected = Connect::new_v3(false, 0, "client", None, Some("username"), None);
        let mut reader = BufReader::new(&msg[..]);
        let header = FixedHeader::try_read(&mut reader).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(Connect::try_read(header, &data[..]).unwrap(), expected);
    }
    #[test]
    fn v3_password() {
        let msg = [
            16,
            28,
            // Protocol name
            0,
            4,
            b'M',
            b'Q',
            b'T',
            b'T',
            // Protocol level
            4,
            // flags
            1 << 6,
            // Keep alive
            0,
            0,
            // Client identifier
            0,
            6,
            b'c',
            b'l',
            b'i',
            b'e',
            b'n',
            b't',
            // Password
            0,
            8,
            b'p',
            b'a',
            b's',
            b's',
            b'w',
            b'o',
            b'r',
            b'd',
        ];
        let expected = Connect::new_v3(false, 0, "client", None, None, Some(b"password"));
        let mut reader = BufReader::new(&msg[..]);
        let header = FixedHeader::try_read(&mut reader).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(Connect::try_read(header, &data[..]).unwrap(), expected);
    }
    #[test]
    fn v3_username_and_password() {
        let msg = [
            16,
            38,
            // Protocol name
            0,
            4,
            b'M',
            b'Q',
            b'T',
            b'T',
            // Protocol level
            4,
            // flags
            1 << 6 | 1 << 7,
            // Keep alive
            0,
            0,
            // Client identifier
            0,
            6,
            b'c',
            b'l',
            b'i',
            b'e',
            b'n',
            b't',
            // Username
            0,
            8,
            b'u',
            b's',
            b'e',
            b'r',
            b'n',
            b'a',
            b'm',
            b'e',
            // Password
            0,
            8,
            b'p',
            b'a',
            b's',
            b's',
            b'w',
            b'o',
            b'r',
            b'd',
        ];
        let expected = Connect::new_v3(
            false,
            0,
            "client",
            None,
            Some("username"),
            Some(b"password"),
        );
        let mut reader = BufReader::new(&msg[..]);
        let header = FixedHeader::try_read(&mut reader).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(Connect::try_read(header, &data[..]).unwrap(), expected);
    }
    #[test]
    fn v3_clean_session() {
        let msg = [
            16,
            18,
            // Protocol name
            0,
            4,
            b'M',
            b'Q',
            b'T',
            b'T',
            // Protocol level
            4,
            // flags
            1 << 1,
            // Keep alive
            0,
            0,
            // Client identifier
            0,
            6,
            b'c',
            b'l',
            b'i',
            b'e',
            b'n',
            b't',
        ];
        let expected = Connect::new_v3(true, 0, "client", None, None, None);
        let mut reader = BufReader::new(&msg[..]);
        let header = FixedHeader::try_read(&mut reader).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(Connect::try_read(header, &data[..]).unwrap(), expected);
    }
    #[test]
    fn v3_client_id() {
        let msg = [
            16, 18, 0, 4, b'M', b'Q', b'T', b'T', // Protocol name
            4,    // Protocol level
            0,    // flags
            0x07, 0x08, // Keep alive
            0, 6, b'c', b'l', b'i', b'e', b'n', b't', // Client identifier
        ];
        let expected = Connect::new_v3(false, 1800, "client", None, None, None);
        let mut reader = BufReader::new(&msg[..]);
        let header = FixedHeader::try_read(&mut reader).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(Connect::try_read(header, &data[..]).unwrap(), expected);
    }
}

use std::marker::PhantomData;

use bytes::{Buf, BufMut, Bytes};

use crate::{
    util::{
        extract_bytes, extract_str, read_variable_len_int, variable_len_int_size,
        write_variable_len_int,
    },
    ControlPacketType, Error, FixedHeader, MalformedPacket, MqttV5_0_0, Property,
    PropertyIdentifier, UserProperty,
};

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum ReasonCode {
    /// Authentication is successful
    Success = 0,
    /// Continue the authentication with another step
    ContinueAuthentication = 24,
    /// Initiate a re-authentication
    ReAuthenticate = 25,
}

impl TryFrom<u8> for ReasonCode {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Success),
            24 => Ok(Self::ContinueAuthentication),
            25 => Ok(Self::ReAuthenticate),
            _ => Err(MalformedPacket::new("Invalid Authentication reason code")),
        }
    }
}

/// An AUTH packet is sent from Client to Server or Server to Client as part of an extended authentication exchange, such as challenge / response authentication. It is a Protocol Error for the Client or Server to send an AUTH packet if the CONNECT packet did not contain the same Authentication Method.
#[derive(Debug, PartialEq)]
pub struct Auth<V> {
    protocol_level: PhantomData<V>,
    reason_code: ReasonCode,
    /// Followed by a UTF-8 Encoded String containing the name of the authentication method. It is a Protocol Error to omit the Authentication Method or to include it more than once. Refer to section 4.12 for more information about extended authentication.
    method: Option<String>,
    /// Followed by Binary Data containing authentication data. It is a Protocol Error to include Authentication Data more than once. The contents of this data are defined by the authentication method. Refer to section 4.12 for more information about extended authentication.
    auth_data: Bytes,
    /// Followed by the UTF-8 Encoded String representing the reason for the disconnect. This Reason String is human readable, designed for diagnostics and SHOULD NOT be parsed by the receiver.
    reason: Option<String>,
    user_property: Vec<UserProperty>,
}

impl Auth<MqttV5_0_0> {
    pub fn write_to_buf(&self, buf: &mut impl BufMut) {
        let properties_len = self.method.property_len()
            + self.auth_data.property_len()
            + self.reason.property_len()
            + self.user_property.property_len();
        //
        // If remaining length == 0 -> reason_code == 0x00 and there are no properties
        let remaining_length = if self.reason_code == ReasonCode::Success && properties_len == 0 {
            0
        } else {
            1 + variable_len_int_size(properties_len) + properties_len
        };

        let fixed_header = FixedHeader::new(crate::ControlPacketType::Auth, remaining_length);
        fixed_header.write_to_buf(buf);

        let properties_len = self.method.property_len()
            + self.auth_data.property_len()
            + self.reason.property_len()
            + self.user_property.property_len();

        // If reason code == 0x00 and there are no properties, fixed header length = 0 and we don't
        // send body
        if self.reason_code == ReasonCode::Success && properties_len == 0 {
            assert_eq!(fixed_header.remaining_length, 0);
            return;
        }

        buf.put_u8(self.reason_code as u8);

        write_variable_len_int(properties_len as u64, buf);

        self.method
            .serialize(PropertyIdentifier::AuthenticationMethod, buf);
        self.auth_data
            .serialize(PropertyIdentifier::AuthenticationData, buf);
        self.reason.serialize(PropertyIdentifier::Reason, buf);
        self.user_property
            .serialize(PropertyIdentifier::UserProperty, buf);
    }

    pub fn new_v5(
        reason_code: ReasonCode,
        method: Option<String>,
        auth_data: Option<Bytes>,
        reason: Option<String>,
        user_property: Vec<UserProperty>,
    ) -> Self {
        Self {
            reason_code,
            protocol_level: PhantomData,
            method,
            auth_data: auth_data.unwrap_or_default(),
            reason,
            user_property,
        }
    }

    pub fn try_read(header: FixedHeader, data: &mut Bytes) -> Result<Self, Error> {
        assert_eq!(header.control_packet_type, ControlPacketType::Auth);

        if header.remaining_length == 0 {
            return Ok(Self {
                protocol_level: PhantomData,
                reason_code: ReasonCode::Success,
                method: None,
                auth_data: Bytes::new(),
                reason: None,
                user_property: Vec::new(),
            });
        }

        let reason_code = ReasonCode::try_from(data.try_get_u8()?)?;

        let len_properties = read_variable_len_int(data)?;
        let len_properties = len_properties as usize;

        let mut method = None;
        let mut auth_data = None;
        let mut reason = None;
        let mut user_property = Vec::new();

        if data.remaining() < len_properties {
            return Err(MalformedPacket::new("Packet too short to parse"));
        }
        let data = &mut data.split_to(len_properties);

        while data.has_remaining() {
            let property_identifier = crate::util::read_variable_len_int(data)?;
            let property_identifier = PropertyIdentifier::try_from(property_identifier)?;
            match property_identifier {
                PropertyIdentifier::AuthenticationMethod => {
                    if method.is_some() {
                        return Err(Error::ProtocolError(
                            "AuthenticationMethod specified multiple times",
                        ));
                    }
                    method = Some(extract_str(data)?);
                }
                PropertyIdentifier::AuthenticationData => {
                    if auth_data.is_some() {
                        return Err(Error::ProtocolError(
                            "AuthenticationData specified multiple times",
                        ));
                    }
                    auth_data = Some(extract_bytes(data)?);
                }
                PropertyIdentifier::Reason => {
                    if reason.is_some() {
                        return Err(Error::ProtocolError("Reason specified multiple times"));
                    }
                    reason = Some(extract_str(data)?);
                }
                PropertyIdentifier::UserProperty => {
                    let key = extract_str(data)?.to_string();
                    let value = extract_str(data)?.to_string();
                    let property = UserProperty { key, value };
                    user_property.push(property);
                }
                _ => {
                    return Err(MalformedPacket::new(
                        "Received unexpected property for connect",
                    ))
                }
            };
        }

        Ok(Self {
            protocol_level: PhantomData,
            reason_code,
            method,
            auth_data: auth_data.unwrap_or_default(),
            reason,
            user_property,
        })
    }

    pub fn reason_code(&self) -> ReasonCode {
        self.reason_code
    }

    pub fn method(&self) -> Option<&String> {
        self.method.as_ref()
    }

    pub fn auth_data(&self) -> &Bytes {
        &self.auth_data
    }

    pub fn reason(&self) -> Option<&String> {
        self.reason.as_ref()
    }

    pub fn user_property(&self) -> &[UserProperty] {
        &self.user_property
    }
}

#[cfg(test)]
mod test_v5 {

    use bytes::BytesMut;

    use super::*;

    #[test]
    fn serialize() {
        let mut buf = BytesMut::new();
        let msg = Auth::new_v5(
            ReasonCode::ContinueAuthentication,
            None,
            None,
            None,
            Vec::new(),
        );
        msg.write_to_buf(&mut buf);
        assert_eq!(&buf[..], &[240, 2, 24, 0]);
    }

    #[test]
    fn serialize_short() {
        let mut buf = BytesMut::new();
        let msg = Auth::new_v5(ReasonCode::Success, None, None, None, Vec::new());
        msg.write_to_buf(&mut buf);
        assert_eq!(&buf[..], &[240, 0]);
    }

    #[test]
    fn serialize_properties() {
        let mut buf = BytesMut::new();
        let msg = Auth::new_v5(
            ReasonCode::ReAuthenticate,
            Some("method".to_string()),
            Some(Bytes::from_static(&[1, 2, 3, 4])),
            Some("reason".to_string()),
            vec![UserProperty {
                key: "property1".to_string(),
                value: "value1".to_string(),
            }],
        );
        msg.write_to_buf(&mut buf);
        assert_eq!(
            &buf[..],
            &[
                240, 47, 25, // Properties length
                45, // Auth method
                21, 0, 6, b'm', b'e', b't', b'h', b'o', b'd', // Auth data
                22, 0, 4, 1, 2, 3, 4, // Reason
                31, 0, 6, b'r', b'e', b'a', b's', b'o', b'n', // user property
                38, 0, 9, b'p', b'r', b'o', b'p', b'e', b'r', b't', b'y', b'1', 0, 6, b'v', b'a',
                b'l', b'u', b'e', b'1'
            ]
        );
    }

    #[test]
    fn deserialize() {
        let msg = [240, 2, 24, 0];
        let expected = Auth::new_v5(
            ReasonCode::ContinueAuthentication,
            None,
            None,
            None,
            Vec::new(),
        );
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(Auth::try_read(header, &mut body).unwrap(), expected);
    }

    #[test]
    fn deserialize_short() {
        let msg = [240, 0];
        let expected = Auth::new_v5(ReasonCode::Success, None, None, None, Vec::new());
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(Auth::try_read(header, &mut body).unwrap(), expected);
    }

    #[test]
    fn deserialize_properties() {
        let msg = [
            240, 47, 25, // Properties length
            45, // Auth method
            21, 0, 6, b'm', b'e', b't', b'h', b'o', b'd', // Auth data
            22, 0, 4, 1, 2, 3, 4, // Reason
            31, 0, 6, b'r', b'e', b'a', b's', b'o', b'n', // user property
            38, 0, 9, b'p', b'r', b'o', b'p', b'e', b'r', b't', b'y', b'1', 0, 6, b'v', b'a', b'l',
            b'u', b'e', b'1',
        ];
        let expected = Auth::new_v5(
            ReasonCode::ReAuthenticate,
            Some("method".to_string()),
            Some(Bytes::from_static(&[1, 2, 3, 4])),
            Some("reason".to_string()),
            vec![UserProperty {
                key: "property1".to_string(),
                value: "value1".to_string(),
            }],
        );
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(Auth::try_read(header, &mut body).unwrap(), expected);
    }
}

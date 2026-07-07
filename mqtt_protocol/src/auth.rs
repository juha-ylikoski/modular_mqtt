use std::{io::Write, marker::PhantomData};

use crate::{
    util::{
        extract_bytes, extract_str, read_variable_len_int, variable_len_int_size,
        write_variable_len_int,
    },
    Error, FixedHeader, MalformedPacket, MqttV5_0_0, Property, PropertyIdentifier, UserProperty,
};

#[derive(Debug, PartialEq)]
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
    fixed_header: FixedHeader,
    protocol_level: PhantomData<V>,
    reason_code: ReasonCode,
    /// Followed by a UTF-8 Encoded String containing the name of the authentication method. It is a Protocol Error to omit the Authentication Method or to include it more than once. Refer to section 4.12 for more information about extended authentication.
    method: Option<String>,
    /// Followed by Binary Data containing authentication data. It is a Protocol Error to include Authentication Data more than once. The contents of this data are defined by the authentication method. Refer to section 4.12 for more information about extended authentication.
    auth_data: Vec<u8>,
    /// Followed by the UTF-8 Encoded String representing the reason for the disconnect. This Reason String is human readable, designed for diagnostics and SHOULD NOT be parsed by the receiver.
    reason: Option<String>,
    user_property: Vec<UserProperty>,
}

impl Auth<MqttV5_0_0> {
    pub fn write_to_stream(self, writer: &mut impl Write) -> Result<usize, std::io::Error> {
        let mut len = self.fixed_header.write_to_stream(writer)?;
        writer.write_all(&[self.reason_code as u8])?;
        len += 1;

        let method = self.method.as_deref();
        let reason = self.reason.as_deref();
        let auth_data: &[u8] = &self.auth_data;
        let properties_len = method.property_len()
            + auth_data.property_len()
            + reason.property_len()
            + self.user_property.property_len();

        len += write_variable_len_int(properties_len as u64, writer)?;

        len += method.serialize(PropertyIdentifier::AuthenticationMethod, writer)?
            + auth_data.serialize(PropertyIdentifier::AuthenticationData, writer)?
            + reason.serialize(PropertyIdentifier::Reason, writer)?
            + self
                .user_property
                .serialize(PropertyIdentifier::UserProperty, writer)?;

        Ok(len)
    }

    pub fn new_v5(
        reason_code: ReasonCode,
        method: Option<String>,
        auth_data: Option<Vec<u8>>,
        reason: Option<String>,
        user_property: Vec<UserProperty>,
    ) -> Self {
        let _method = method.as_deref();
        let _reason = reason.as_deref();
        let properties_len = _method.property_len()
            + auth_data.property_len()
            + _reason.property_len()
            + user_property.property_len();
        let remaining_length = 1 + variable_len_int_size(properties_len) + properties_len;
        Self {
            fixed_header: FixedHeader::new(crate::ControlPacketType::Auth, remaining_length),
            reason_code,
            protocol_level: PhantomData,
            method,
            auth_data: auth_data.unwrap_or_default(),
            reason,
            user_property,
        }
    }

    pub fn try_read_v5(header: FixedHeader, data: &[u8]) -> Result<Self, Error> {
        if data.len() < 2 {
            return Err(MalformedPacket::new("Packet too short to parse"));
        }
        let reason_code = ReasonCode::try_from(data[0])?;

        let (i, len_properties) = read_variable_len_int(&data[1..])?;
        let len_properties = len_properties as usize;
        let mut index = 1 + i;

        let mut method = None;
        let mut auth_data = None;
        let mut reason = None;
        let mut user_property = Vec::new();

        let mut i = 0;
        while i < len_properties {
            let (len, property_identifier) = crate::util::read_variable_len_int(&data[index..])?;
            let property_identifier = PropertyIdentifier::try_from(property_identifier)?;
            i += len;
            index += len;
            let property_value = &data[index..];
            match property_identifier {
                PropertyIdentifier::AuthenticationMethod => {
                    if method.is_some() {
                        return Err(Error::ProtocolError(
                            "AuthenticationMethod specified multiple times",
                        ));
                    }
                    let _method = extract_str(property_value)?;
                    method = Some(_method.to_string());
                    i += 2 + _method.len();
                    index += 2 + _method.len();
                }
                PropertyIdentifier::AuthenticationData => {
                    if auth_data.is_some() {
                        return Err(Error::ProtocolError(
                            "AuthenticationData specified multiple times",
                        ));
                    }
                    let data = extract_bytes(property_value)?;
                    auth_data = Some(data.to_vec());
                    i += 2 + data.len();
                    index += 2 + data.len();
                }
                PropertyIdentifier::Reason => {
                    if reason.is_some() {
                        return Err(Error::ProtocolError("Reason specified multiple times"));
                    }
                    let _reason = extract_str(property_value)?;
                    reason = Some(_reason.to_string());
                    i += 2 + _reason.len();
                    index += 2 + _reason.len();
                }
                PropertyIdentifier::UserProperty => {
                    let key = extract_str(property_value)?.to_string();
                    let value = extract_str(&property_value[2 + key.len()..])?.to_string();
                    let len = 2 + key.len() + 2 + value.len();
                    let property = UserProperty { key, value };
                    user_property.push(property);
                    i += len;
                    index += len;
                }
                _ => {
                    return Err(MalformedPacket::new(
                        "Received unexpected property for connect",
                    ))
                }
            };
        }

        Ok(Self {
            fixed_header: header,
            protocol_level: PhantomData,
            reason_code,
            method,
            auth_data: auth_data.unwrap_or_default(),
            reason,
            user_property,
        })
    }
}

#[cfg(test)]
mod test_v5 {
    use std::io::{BufReader, BufWriter, Read};

    use super::*;

    #[test]
    fn serialize() {
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        let msg = Auth::new_v5(ReasonCode::Success, None, None, None, Vec::new());
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
        assert_eq!(&buf, &[240, 2, 0, 0]);
    }

    #[test]
    fn serialize_properties() {
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        let msg = Auth::new_v5(
            ReasonCode::ReAuthenticate,
            Some("method".to_string()),
            Some(vec![1, 2, 3, 4]),
            Some("reason".to_string()),
            vec![UserProperty {
                key: "property1".to_string(),
                value: "value1".to_string(),
            }],
        );
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
        #[rustfmt::skip]
        assert_eq!(
            &buf,
            &[
                240, 47, 
                25, 
                // Properties length
                45, 
                // Auth method
                21, 0, 6,
                b'm', b'e', b't', b'h', b'o', b'd',
                // Auth data
                22, 0, 4, 1, 2, 3, 4, 
                // Reason
                31, 0, 6, b'r', b'e', b'a', b's', b'o', b'n',
              // user property
                38, 0, 9, 
                b'p', b'r', b'o', b'p', b'e', b'r', b't', b'y', b'1', 
                0, 6, 
                b'v', b'a', b'l', b'u', b'e', b'1'
            ]
        );
    }

    #[test]
    fn deserialize() {
        let msg = [240, 2, 0, 0];
        let expected = Auth::new_v5(ReasonCode::Success, None, None, None, Vec::new());
        let mut reader = BufReader::new(&msg[..]);
        let header = FixedHeader::try_read_sync(&mut reader).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(Auth::try_read_v5(header, &data[..]).unwrap(), expected);
    }

    #[test]
    fn deserialize_properties() {
        #[rustfmt::skip]
        let msg = [
                240, 47, 
                25, 
                // Properties length
                45, 
                // Auth method
                21, 0, 6,
                b'm', b'e', b't', b'h', b'o', b'd',
                // Auth data
                22, 0, 4, 1, 2, 3, 4, 
                // Reason
                31, 0, 6, b'r', b'e', b'a', b's', b'o', b'n',
                // user property
                38, 0, 9, 
                b'p', b'r', b'o', b'p', b'e', b'r', b't', b'y', b'1', 
                0, 6, 
                b'v', b'a', b'l', b'u', b'e', b'1'
            ];
        let expected = Auth::new_v5(
            ReasonCode::ReAuthenticate,
            Some("method".to_string()),
            Some(vec![1, 2, 3, 4]),
            Some("reason".to_string()),
            vec![UserProperty {
                key: "property1".to_string(),
                value: "value1".to_string(),
            }],
        );
        let mut reader = BufReader::new(&msg[..]);
        let header = FixedHeader::try_read_sync(&mut reader).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(Auth::try_read_v5(header, &data[..]).unwrap(), expected);
    }
}

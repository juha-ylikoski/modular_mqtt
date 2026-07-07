use std::{io::Write, marker::PhantomData};

use super::fixed_header::FixedHeader;
use crate::{
    util::{
        extract_bytes, extract_str, read_variable_len_int, variable_len_int_size,
        write_variable_len_int,
    },
    Error, MalformedPacket, MqttV3_1_1, MqttV5_0_0, Property, PropertyIdentifier, Qos,
    UserProperty,
};

#[derive(Debug, PartialEq)]
pub enum ConnectRcV3 {
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

#[derive(Debug, PartialEq)]
pub enum ConnectRcV5 {
    /// Connection accepted
    Accepted = 0,
    /// The Server does not wish to reveal the reason for the failure, or none of the other Reason Codes apply.
    UnspecifiedError = 128,
    /// Data within the CONNECT packet could not be correctly parsed.
    MalformedPacket = 129,
    /// Data in the CONNECT packet does not conform to this specification.
    ProtocolError = 130,
    /// The CONNECT is valid but is not accepted by this Server.
    ImplementationSpecificError = 131,
    /// The Server does not support the version of the MQTT protocol requested by the Client.
    UnsupportedProtocolVersion = 132,
    /// The Client Identifier is a valid string but is not allowed by the Server.
    ClientIdentifierNotValid = 133,
    /// The Server does not accept the User Name or Password specified by the Client
    BadUsernameOrPassword = 134,
    /// The Client is not authorized to connect.
    NotAuthorized = 135,
    /// The MQTT Server is not available.
    ServerUnavailableV5 = 136,
    /// Server busy
    ServerBusy = 137,
    /// This Client has been banned by administrative action. Contact the server administrator.
    Banned = 138,
    /// The authentication method is not supported or does not match the authentication method currently in use.
    BadAuthenticationMethod = 140,
    /// The Will Topic Name is not malformed, but is not accepted by this Server.
    TopicNameInvalid = 144,
    /// The CONNECT packet exceeded the maximum permissible size.
    PacketTooLarge = 149,
    /// An implementation or administrative imposed limit has been exceeded.
    QuotaExceeded = 151,
    /// The Will Payload does not match the specified Payload Format Indicator.
    PayloadFormatInvalid = 153,
    /// The Server does not support retained messages, and Will Retain was set to 1.
    RetainNotSupported = 154,
    /// The Server does not support the QoS set in Will QoS.
    QosNotSupported = 155,
    /// The Client should temporarily use another server.
    UseAnotherServer = 156,
    /// The Client should permanently use another server.
    ServerModed = 157,
    /// The connection rate limit has been exceeded.
    ConnectionRateExceeded = 159,
}

#[derive(Debug, PartialEq)]
pub enum ConnectRc {
    V3(ConnectRcV3),
    V5(ConnectRcV5),
}

impl From<ConnectRc> for u8 {
    fn from(value: ConnectRc) -> Self {
        match value {
            ConnectRc::V3(connect_rc_v3) => connect_rc_v3 as u8,
            ConnectRc::V5(connect_rc_v5) => connect_rc_v5 as u8,
        }
    }
}

impl TryFrom<u8> for ConnectRcV3 {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Accepted),
            1 => Ok(Self::UnacceptableVersion),
            2 => Ok(Self::IdentifierRejected),
            3 => Ok(Self::ServerUnavailable),
            4 => Ok(Self::BadUsernamePassword),
            5 => Ok(Self::Refused),
            _ => Err(MalformedPacket::new(
                "Mqtt connect return code was not one of 0, 1, 2, 3 or 5",
            )),
        }
    }
}

impl TryFrom<u8> for ConnectRcV5 {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Accepted),
            128 => Ok(Self::UnspecifiedError),
            129 => Ok(Self::MalformedPacket),
            130 => Ok(Self::ProtocolError),
            131 => Ok(Self::ImplementationSpecificError),
            132 => Ok(Self::UnsupportedProtocolVersion),
            133 => Ok(Self::ClientIdentifierNotValid),
            134 => Ok(Self::BadUsernameOrPassword),
            135 => Ok(Self::NotAuthorized),
            136 => Ok(Self::ServerUnavailableV5),
            137 => Ok(Self::ServerBusy),
            138 => Ok(Self::Banned),
            140 => Ok(Self::BadAuthenticationMethod),
            144 => Ok(Self::TopicNameInvalid),
            149 => Ok(Self::PacketTooLarge),
            151 => Ok(Self::QuotaExceeded),
            153 => Ok(Self::PayloadFormatInvalid),
            154 => Ok(Self::RetainNotSupported),
            155 => Ok(Self::QosNotSupported),
            156 => Ok(Self::UseAnotherServer),
            157 => Ok(Self::ServerModed),
            159 => Ok(Self::ConnectionRateExceeded),
            _ => Err(MalformedPacket::new("Mqtt connect return code was invalid")),
        }
    }
}

#[derive(Debug, PartialEq)]
/// The CONNACK Packet is the packet sent by the Server in response to a CONNECT Packet received from a Client.
/// The first packet sent from the Server to the Client MUST be a CONNACK Packet [MQTT-3.2.0-1].
pub struct ConnAck<'a, V> {
    pub fixed_header: FixedHeader,
    pub session_present: bool,
    pub connect_rc: ConnectRc,

    protocol_level: PhantomData<V>,

    // v5 properties
    /// If the Session Expiry Interval is absent the value in the CONNECT Packet used. The server uses this
    /// property to inform the Client that it is using a value other than that sent by the Client in the
    /// CONNACK. Refer to section 3.1.2.11.2 for a description of the use of Session Expiry Interval.
    session_expiry_interval: Option<u32>,
    /// The Server uses this value to limit the number of QoS 1 and QoS 2 publications that it is willing
    /// to process concurrently for the Client. It does not provide a mechanism to limit the QoS 0
    /// publications that the Client might try to send.
    receive_maximum: Option<u16>,
    /// If a Server does not support QoS 1 or QoS 2 PUBLISH packets it MUST send a Maximum QoS in the
    /// CONNACK packet specifying the highest QoS it supports
    maximum_qos: Option<Qos>,
    /// If present, this byte declares whether the Server supports retained messages. A value of 0 means
    /// that retained messages are not supported. A value of 1 means retained messages are supported.
    /// If not present, then retained messages are supported. It is a Protocol Error to include Retain
    /// Available more than once or to use a value other than 0 or 1.
    retain_available: Option<bool>,
    /// Integer representing the Maximum Packet Size the Server is willing to accept. If the Maximum
    /// Packet Size is not present, there is no limit on the packet size imposed beyond the limitations
    /// in the protocol as a result of the remaining length encoding and the protocol header sizes.
    maximum_packet_size: Option<u32>,
    /// UTF-8 string which is the Assigned Client Identifier
    client_identifier: Option<&'a str>,
    /// This value indicates the highest value that the Server will accept as a Topic Alias sent by the
    /// Client. The Server uses this value to limit the number of Topic Aliases that it is willing to
    /// hold on this Connection. The Client MUST NOT send a Topic Alias in a PUBLISH packet to the
    /// Server greater than this value
    topic_alias_maximum: Option<u16>,
    /// UTF-8 Encoded String representing the reason associated with this response. This Reason String
    /// is a human readable string designed for diagnostics and SHOULD NOT be parsed by the Client
    reason: Option<&'a str>,
    /// UTF-8 String Pair. This property can be used to provide additional information to the Client
    /// including diagnostic information
    user_property: Vec<UserProperty<'a>>,
    /// this byte declares whether the Server supports Wildcard Subscriptions. A value is 0 means
    /// that Wildcard Subscriptions are not supported
    wildcard_subscription_available: Option<bool>,
    /// this byte declares whether the Server supports Subscription Identifiers
    subscription_identifiers_available: Option<bool>,
    /// this byte declares whether the Server supports Shared Subscriptions
    shared_subscription_available: Option<bool>,
    /// If the Server sends a Server Keep Alive on the CONNACK packet, the Client MUST use this value
    /// instead of the Keep Alive value the Client sent on CONNECT
    server_keep_alive: Option<u16>,
    /// UTF-8 Encoded String which is used as the basis for creating a Response Topic
    response_information: Option<&'a str>,
    /// UTF-8 Encoded String which can be used by the Client to identify another Server to use
    /// The Server uses a Server Reference in either a CONNACK or DISCONNECT packet with Reason
    /// code of 0x9C (Use another server) or Reason Code 0x9D (Server moved)
    server_reference: Option<&'a str>,
    /// UTF-8 Encoded String containing the name of the authentication method
    authentication_method: Option<&'a str>,
    /// Binary Data containing authentication data
    authentication_data: &'a [u8],
}

impl<'a, V> ConnAck<'a, V> {
    /// Re-calculate fixed header length
    ///
    /// We initially have no properties -> property len == 0
    /// -> we have 1 byte to store it
    ///
    /// if our properties take more than 128 bytes, we need more than
    /// 1 byte for the length -> we need to modify fixed header
    ///
    /// we also need to add length of properties into fixed header and we know them
    /// only at serialization time due to builder pattern (without finalize)
    fn re_calculate_fixed_header_length(&mut self) {
        let v5 = matches!(self.connect_rc, ConnectRc::V5(_));
        if v5 {
            let user_properties: &[UserProperty] = self.user_property.as_ref();
            let properties_length = self.session_expiry_interval.property_len()
                + self.receive_maximum.property_len()
                + self.maximum_qos.property_len()
                + self.retain_available.property_len()
                + self.maximum_packet_size.property_len()
                + self.client_identifier.property_len()
                + self.topic_alias_maximum.property_len()
                + self.reason.property_len()
                + user_properties.property_len()
                + self.wildcard_subscription_available.property_len()
                + self.subscription_identifiers_available.property_len()
                + self.shared_subscription_available.property_len()
                + self.server_keep_alive.property_len()
                + self.response_information.property_len()
                + self.server_reference.property_len()
                + self.authentication_method.property_len()
                + self.authentication_data.property_len();
            self.fixed_header.remaining_length =
                2 + variable_len_int_size(properties_length) + properties_length;
        }
    }
    pub fn write_to_stream(mut self, writer: &mut impl Write) -> Result<usize, std::io::Error> {
        let v5 = matches!(self.connect_rc, ConnectRc::V5(_));
        self.re_calculate_fixed_header_length();
        let mut length = self.fixed_header.write_to_stream(writer)?;
        writer.write_all(&[self.session_present as u8, self.connect_rc.into()])?;
        length += 2;
        if v5 {
            let user_properties: &[UserProperty] = self.user_property.as_ref();
            let properties_length = self.session_expiry_interval.property_len()
                + self.receive_maximum.property_len()
                + self.maximum_qos.property_len()
                + self.retain_available.property_len()
                + self.maximum_packet_size.property_len()
                + self.client_identifier.property_len()
                + self.topic_alias_maximum.property_len()
                + self.reason.property_len()
                + user_properties.property_len()
                + self.wildcard_subscription_available.property_len()
                + self.subscription_identifiers_available.property_len()
                + self.shared_subscription_available.property_len()
                + self.server_keep_alive.property_len()
                + self.response_information.property_len()
                + self.server_reference.property_len()
                + self.authentication_method.property_len()
                + self.authentication_data.property_len();
            length += write_variable_len_int(properties_length as u64, writer)?;
            length += self
                .session_expiry_interval
                .serialize(PropertyIdentifier::SessionExpiryInterval, writer)?
                + self
                    .receive_maximum
                    .serialize(PropertyIdentifier::ReceiveMaximum, writer)?
                + self
                    .maximum_qos
                    .serialize(PropertyIdentifier::MaximumQos, writer)?
                + self
                    .retain_available
                    .serialize(PropertyIdentifier::RetainAvailable, writer)?
                + self
                    .maximum_packet_size
                    .serialize(PropertyIdentifier::MaximumPacketSize, writer)?
                + self
                    .client_identifier
                    .serialize(PropertyIdentifier::AssignedClientIdentifier, writer)?
                + self
                    .topic_alias_maximum
                    .serialize(PropertyIdentifier::TopicAliasMaximum, writer)?
                + self.reason.serialize(PropertyIdentifier::Reason, writer)?
                + user_properties.serialize(PropertyIdentifier::UserProperty, writer)?
                + self
                    .wildcard_subscription_available
                    .serialize(PropertyIdentifier::WildcardSubscriptionAvailable, writer)?
                + self
                    .subscription_identifiers_available
                    .serialize(PropertyIdentifier::SubscriptionIdentifierAvailable, writer)?
                + self
                    .shared_subscription_available
                    .serialize(PropertyIdentifier::SharedSubscriptionAvailable, writer)?
                + self
                    .server_keep_alive
                    .serialize(PropertyIdentifier::ServerKeepAlive, writer)?
                + self
                    .response_information
                    .serialize(PropertyIdentifier::ResponseInformation, writer)?
                + self
                    .server_reference
                    .serialize(PropertyIdentifier::ServerReference, writer)?
                + self
                    .authentication_method
                    .serialize(PropertyIdentifier::AuthenticationMethod, writer)?
                + self
                    .authentication_data
                    .serialize(PropertyIdentifier::AuthenticationData, writer)?;
        }
        writer.flush()?;
        Ok(length)
    }
}

impl<'a> ConnAck<'a, MqttV3_1_1> {
    pub fn new_v3(session_present: bool, connect_rc: ConnectRcV3) -> Self {
        Self {
            fixed_header: FixedHeader::new(super::fixed_header::ControlPacketType::ConnAck, 2),
            session_present,
            connect_rc: ConnectRc::V3(connect_rc),
            protocol_level: PhantomData,
            session_expiry_interval: None,
            receive_maximum: None,
            maximum_qos: None,
            retain_available: None,
            maximum_packet_size: None,
            client_identifier: None,
            topic_alias_maximum: None,
            reason: None,
            user_property: Vec::new(),
            wildcard_subscription_available: None,
            subscription_identifiers_available: None,
            shared_subscription_available: None,
            server_keep_alive: None,
            response_information: None,
            server_reference: None,
            authentication_method: None,
            authentication_data: &[],
        }
    }
    pub fn try_read_v3(header: FixedHeader, data: &[u8]) -> Result<Self, Error> {
        if data.len() < 2 {
            return Err(MalformedPacket::new("Packet too short to parse"));
        }
        let flags = data[0];
        let connect_rc: ConnectRcV3 = data[1].try_into()?;
        if flags & (!1) != 0 {
            return Err(MalformedPacket::new(
                "All reserved bits have to be 0 in CONNACK",
            ));
        }
        Ok(Self {
            fixed_header: header,
            session_present: (flags & 1) == 1,
            connect_rc: ConnectRc::V3(connect_rc),
            protocol_level: PhantomData,
            session_expiry_interval: None,
            receive_maximum: None,
            maximum_qos: None,
            retain_available: None,
            maximum_packet_size: None,
            client_identifier: None,
            topic_alias_maximum: None,
            reason: None,
            user_property: Vec::new(),
            wildcard_subscription_available: None,
            subscription_identifiers_available: None,
            shared_subscription_available: None,
            server_keep_alive: None,
            response_information: None,
            server_reference: None,
            authentication_method: None,
            authentication_data: &[],
        })
    }
}

impl<'a> ConnAck<'a, MqttV5_0_0> {
    pub fn new_v5(session_present: bool, connect_rc: ConnectRcV5) -> Self {
        Self {
            fixed_header: FixedHeader::new(super::fixed_header::ControlPacketType::ConnAck, 3),
            session_present,
            connect_rc: ConnectRc::V5(connect_rc),
            protocol_level: PhantomData,
            session_expiry_interval: None,
            receive_maximum: None,
            maximum_qos: None,
            retain_available: None,
            maximum_packet_size: None,
            client_identifier: None,
            topic_alias_maximum: None,
            reason: None,
            user_property: Vec::new(),
            wildcard_subscription_available: None,
            subscription_identifiers_available: None,
            shared_subscription_available: None,
            server_keep_alive: None,
            response_information: None,
            server_reference: None,
            authentication_method: None,
            authentication_data: &[],
        }
    }

    fn read_property(&mut self, data: &'a [u8]) -> Result<usize, Error> {
        if data.is_empty() {
            return Err(Error::NotEnoughData);
        }

        let (i, property_identifier) = read_variable_len_int(data)?;
        let data = &data[i..];
        let property_identifier = PropertyIdentifier::try_from(property_identifier)?;
        match property_identifier {
            PropertyIdentifier::SessionExpiryInterval => {
                if self.session_expiry_interval.is_some() {
                    return Err(Error::ProtocolError(
                        "SessionExpiryInterval specified multiple times",
                    ));
                }
                self.session_expiry_interval =
                    Some(u32::from_be_bytes(data[0..4].try_into().map_err(|_| {
                        MalformedPacket::new("Packet too short to read property")
                    })?));
                Ok(4)
            }
            PropertyIdentifier::ReceiveMaximum => {
                if self.receive_maximum.is_some() {
                    return Err(Error::ProtocolError(
                        "ReceiveMaximum specified multiple times",
                    ));
                }
                self.receive_maximum =
                    Some(u16::from_be_bytes(data[0..2].try_into().map_err(|_| {
                        MalformedPacket::new("Packet too short to read property")
                    })?));
                Ok(2)
            }
            PropertyIdentifier::MaximumQos => {
                if self.maximum_qos.is_some() {
                    return Err(Error::ProtocolError("MaximumQos specified multiple times"));
                }
                if data.is_empty() {
                    Err(MalformedPacket::new("Packet too short to read property"))
                } else {
                    self.maximum_qos = Some(Qos::try_from(data[0])?);
                    Ok(1)
                }
            }
            PropertyIdentifier::RetainAvailable => {
                if self.retain_available.is_some() {
                    return Err(Error::ProtocolError(
                        "RetainAvailable specified multiple times",
                    ));
                }
                if data.is_empty() {
                    Err(MalformedPacket::new("Packet too short to read property"))
                } else {
                    self.retain_available = Some(data[0] == 1);
                    Ok(1)
                }
            }
            PropertyIdentifier::MaximumPacketSize => {
                if self.maximum_packet_size.is_some() {
                    return Err(Error::ProtocolError(
                        "MaximumPacketSize specified multiple times",
                    ));
                }
                self.maximum_packet_size =
                    Some(u32::from_be_bytes(data[0..4].try_into().map_err(|_| {
                        MalformedPacket::new("Packet too short to read property")
                    })?));
                Ok(4)
            }
            PropertyIdentifier::AssignedClientIdentifier => {
                if self.client_identifier.is_some() {
                    return Err(Error::ProtocolError(
                        "AssignedClientIdentifier specified multiple times",
                    ));
                }
                let client_identifier = extract_str(data)?;
                self.client_identifier = Some(client_identifier);
                Ok(2 + client_identifier.len())
            }
            PropertyIdentifier::TopicAliasMaximum => {
                if self.topic_alias_maximum.is_some() {
                    return Err(Error::ProtocolError(
                        "TopicAliasmaximum specified multiple times",
                    ));
                }
                self.topic_alias_maximum =
                    Some(u16::from_be_bytes(data[0..2].try_into().map_err(|_| {
                        MalformedPacket::new("Packet too short to read property")
                    })?));
                Ok(2)
            }
            PropertyIdentifier::Reason => {
                if self.reason.is_some() {
                    return Err(Error::ProtocolError("Reason specified multiple times"));
                }
                let reason = extract_str(data)?;
                self.reason = Some(reason);
                Ok(2 + reason.len())
            }
            PropertyIdentifier::UserProperty => {
                let key = extract_str(data)?;
                let value = extract_str(&data[2 + key.len()..])?;
                let property = UserProperty { key, value };
                self.user_property.push(property);
                Ok(2 + key.len() + 2 + value.len())
            }
            PropertyIdentifier::WildcardSubscriptionAvailable => {
                if self.wildcard_subscription_available.is_some() {
                    return Err(Error::ProtocolError(
                        "WildcardSubscriptionAvailable specified multiple times",
                    ));
                }
                if data.is_empty() {
                    Err(MalformedPacket::new("Packet too short to read property"))
                } else {
                    self.wildcard_subscription_available = Some(data[0] == 1);
                    Ok(1)
                }
            }
            PropertyIdentifier::SubscriptionIdentifierAvailable => {
                if self.subscription_identifiers_available.is_some() {
                    return Err(Error::ProtocolError(
                        "SubscriptionIdentifierAvailable specified multiple times",
                    ));
                }
                if data.is_empty() {
                    Err(MalformedPacket::new("Packet too short to read property"))
                } else {
                    self.subscription_identifiers_available = Some(data[0] == 1);
                    Ok(1)
                }
            }
            PropertyIdentifier::SharedSubscriptionAvailable => {
                if self.shared_subscription_available.is_some() {
                    return Err(Error::ProtocolError(
                        "SharedSubscriptionAvailable specified multiple times",
                    ));
                }
                if data.is_empty() {
                    Err(MalformedPacket::new("Packet too short to read property"))
                } else {
                    self.shared_subscription_available = Some(data[0] == 1);
                    Ok(1)
                }
            }
            PropertyIdentifier::ServerKeepAlive => {
                if self.server_keep_alive.is_some() {
                    return Err(Error::ProtocolError(
                        "ServerKeepAlive specified multiple times",
                    ));
                }
                self.server_keep_alive =
                    Some(u16::from_be_bytes(data[0..2].try_into().map_err(|_| {
                        MalformedPacket::new("Packet too short to read property")
                    })?));
                Ok(2)
            }
            PropertyIdentifier::ResponseInformation => {
                if self.response_information.is_some() {
                    return Err(Error::ProtocolError(
                        "ResponseInformation specified multiple times",
                    ));
                }
                let response_information = extract_str(data)?;
                self.response_information = Some(response_information);
                Ok(2 + response_information.len())
            }
            PropertyIdentifier::ServerReference => {
                if self.server_reference.is_some() {
                    return Err(Error::ProtocolError(
                        "ServerReference specified multiple times",
                    ));
                }
                let reference = extract_str(data)?;
                self.server_reference = Some(reference);
                Ok(2 + reference.len())
            }
            PropertyIdentifier::AuthenticationMethod => {
                if self.authentication_method.is_some() {
                    return Err(Error::ProtocolError(
                        "AuthenticationMethod specified multiple times",
                    ));
                }
                let method = extract_str(data)?;
                self.authentication_method = Some(method);
                Ok(2 + method.len())
            }
            PropertyIdentifier::AuthenticationData => {
                if !self.authentication_data.is_empty() {
                    return Err(Error::ProtocolError(
                        "AuthenticationData specified multiple times",
                    ));
                }
                let data = extract_bytes(data)?;
                self.authentication_data = data;
                Ok(2 + data.len())
            }
            _ => Err(MalformedPacket::new(
                "Received unexpected property for connect",
            )),
        }
        .map(|len| i + len)
    }
    pub fn try_read_v5(header: FixedHeader, data: &'a [u8]) -> Result<Self, Error> {
        if data.len() < 2 {
            return Err(MalformedPacket::new("Packet too short to parse"));
        }
        let flags = data[0];
        let connect_rc: ConnectRcV5 = data[1].try_into()?;
        if flags & (!1) != 0 {
            return Err(MalformedPacket::new(
                "All reserved bits have to be 0 in CONNACK",
            ));
        }

        let mut connack = Self {
            fixed_header: header,
            session_present: (flags & 1) == 1,
            connect_rc: ConnectRc::V5(connect_rc),
            protocol_level: PhantomData,
            session_expiry_interval: None,
            receive_maximum: None,
            maximum_qos: None,
            retain_available: None,
            maximum_packet_size: None,
            client_identifier: None,
            topic_alias_maximum: None,
            reason: None,
            user_property: Vec::new(),
            wildcard_subscription_available: None,
            subscription_identifiers_available: None,
            shared_subscription_available: None,
            server_keep_alive: None,
            response_information: None,
            server_reference: None,
            authentication_method: None,
            authentication_data: &[],
        };

        let (len, len_properties) = read_variable_len_int(&data[2..])?;
        let mut i = 2 + len;
        if data.len() < i + len_properties as usize {
            return Err(Error::NotEnoughData);
        }

        println!("Read properties!");

        while i < data.len() {
            i += connack.read_property(&data[i..])?;
        }

        Ok(connack)
    }

    pub fn set_session_expiry_interval(mut self, value: u32) -> Self {
        self.session_expiry_interval = Some(value);
        self
    }
    pub fn session_expiry_interval(&self) -> Option<u32> {
        self.session_expiry_interval
    }
    pub fn set_receive_maximum(mut self, value: u16) -> Self {
        self.receive_maximum = Some(value);
        self
    }
    pub fn receive_maximum(&self) -> u16 {
        self.receive_maximum.unwrap_or_default()
    }
    pub fn set_maximum_qos(mut self, value: Qos) -> Self {
        self.maximum_qos = Some(value);
        self
    }
    pub fn maximum_qos(&self) -> Qos {
        self.maximum_qos.unwrap_or(Qos::ExactlyOnce)
    }
    pub fn set_retain_available(mut self, value: bool) -> Self {
        self.retain_available = Some(value);
        self
    }
    pub fn retain_available(&self) -> bool {
        self.retain_available.unwrap_or(true)
    }
    pub fn set_maximum_packet_size(mut self, value: u32) -> Self {
        self.maximum_packet_size = Some(value);
        self
    }
    pub fn maximum_packet_size(&self) -> Option<u32> {
        self.maximum_packet_size
    }
    pub fn set_client_identifier(mut self, value: &'a str) -> Self {
        self.client_identifier = Some(value);
        self
    }
    pub fn client_identifier(&self) -> Option<&'a str> {
        self.client_identifier
    }
    pub fn set_topic_alias_maximum(mut self, value: u16) -> Self {
        self.topic_alias_maximum = Some(value);
        self
    }
    pub fn topic_alias_maximum(&self) -> u16 {
        self.topic_alias_maximum.unwrap_or_default()
    }
    pub fn set_reason(mut self, value: &'a str) -> Self {
        self.reason = Some(value);
        self
    }
    pub fn reason(&self) -> Option<&'a str> {
        self.reason
    }
    pub fn set_user_property(mut self, value: Vec<UserProperty<'a>>) -> Self {
        self.user_property = value;
        self
    }
    pub fn user_property(&self) -> &[UserProperty<'a>] {
        &self.user_property
    }
    pub fn set_wildcard_subscription_available(mut self, value: bool) -> Self {
        self.wildcard_subscription_available = Some(value);
        self
    }
    pub fn wildcard_subscription_available(&self) -> bool {
        self.wildcard_subscription_available.unwrap_or(true)
    }
    pub fn set_subscription_identifiers_available(mut self, value: bool) -> Self {
        self.subscription_identifiers_available = Some(value);
        self
    }
    pub fn subscription_identifiers_available(&self) -> bool {
        self.subscription_identifiers_available.unwrap_or(true)
    }
    pub fn set_shared_subscription_available(mut self, value: bool) -> Self {
        self.shared_subscription_available = Some(value);
        self
    }
    pub fn shared_subscription_available(&self) -> bool {
        self.shared_subscription_available.unwrap_or(true)
    }
    pub fn set_server_keep_alive(mut self, value: u16) -> Self {
        self.server_keep_alive = Some(value);
        self
    }
    pub fn server_keep_alive(&self) -> Option<u16> {
        self.server_keep_alive
    }
    pub fn set_response_information(mut self, value: &'a str) -> Self {
        self.response_information = Some(value);
        self
    }
    pub fn response_information(&self) -> Option<&'a str> {
        self.response_information
    }
    pub fn set_server_reference(mut self, value: &'a str) -> Self {
        self.server_reference = Some(value);
        self
    }
    pub fn server_reference(&self) -> Option<&'a str> {
        self.server_reference
    }
    pub fn set_authentication_method(mut self, value: &'a str) -> Self {
        self.authentication_method = Some(value);
        self
    }
    pub fn authentication_method(&self) -> Option<&'a str> {
        self.authentication_method
    }
    pub fn set_authentication_data(mut self, value: &'a [u8]) -> Self {
        self.authentication_data = value;
        self
    }
    pub fn authentication_data(&self) -> &'a [u8] {
        self.authentication_data
    }
}

#[cfg(test)]
mod test_v3 {
    use super::*;
    use std::io::BufWriter;
    use std::io::{BufReader, Read};

    #[test]
    fn serialize() {
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        let msg = ConnAck::new_v3(false, ConnectRcV3::Accepted);
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
        assert_eq!(&buf, &[32, 2, 0, 0]);
    }

    #[test]
    fn serialize_session_present() {
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        let msg = ConnAck::new_v3(true, ConnectRcV3::Accepted);
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
        assert_eq!(&buf, &[32, 2, 1, 0]);
    }
    #[test]
    fn serialize_rc() {
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        let msg = ConnAck::new_v3(false, ConnectRcV3::Refused);
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
        assert_eq!(&buf, &[32, 2, 0, 5]);
    }

    #[test]
    fn deserialize() {
        let msg = [32, 2, 0, 0];
        let expected = ConnAck::new_v3(false, ConnectRcV3::Accepted);
        let mut reader = BufReader::new(&msg[..]);
        let header = FixedHeader::try_read_sync(&mut reader).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(ConnAck::try_read_v3(header, &data[..]).unwrap(), expected);
    }

    #[test]
    fn deserialize_session_present() {
        let msg = [32, 2, 1, 0];
        let expected = ConnAck::new_v3(true, ConnectRcV3::Accepted);
        let mut reader = BufReader::new(&msg[..]);
        let header = FixedHeader::try_read_sync(&mut reader).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(ConnAck::try_read_v3(header, &data[..]).unwrap(), expected);
    }
    #[test]
    fn deserialize_rc() {
        let msg = [32, 2, 0, 4];
        let expected = ConnAck::new_v3(false, ConnectRcV3::BadUsernamePassword);
        let mut reader = BufReader::new(&msg[..]);
        let header = FixedHeader::try_read_sync(&mut reader).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(ConnAck::try_read_v3(header, &data[..]).unwrap(), expected);
    }
}

#[cfg(test)]
mod test_v5 {
    use super::*;
    use std::io::BufWriter;
    use std::io::{BufReader, Read};

    #[test]
    fn serialize() {
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        let msg = ConnAck::new_v5(false, ConnectRcV5::Accepted);
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
        assert_eq!(&buf, &[32, 3, 0, 0, 0]);
    }

    #[test]
    fn serialize_session_present() {
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        let msg = ConnAck::new_v5(true, ConnectRcV5::Accepted);
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
        assert_eq!(&buf, &[32, 3, 1, 0, 0]);
    }
    #[test]
    fn serialize_rc() {
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        let msg = ConnAck::new_v5(false, ConnectRcV5::NotAuthorized);
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
        assert_eq!(&buf, &[32, 3, 0, 135, 0]);
    }

    #[test]
    fn serialize_properties() {
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        let msg = ConnAck::new_v5(false, ConnectRcV5::NotAuthorized)
            .set_session_expiry_interval(24)
            .set_receive_maximum(42)
            .set_maximum_qos(Qos::AtLeastOnce)
            .set_retain_available(false)
            .set_maximum_packet_size(100)
            .set_client_identifier("server-assigned")
            .set_topic_alias_maximum(5)
            .set_reason("arbitrary")
            .set_user_property(vec![
                UserProperty {
                    key: "property0",
                    value: "value0",
                },
                UserProperty {
                    key: "property1",
                    value: "value1",
                },
            ])
            .set_wildcard_subscription_available(true)
            .set_subscription_identifiers_available(false)
            .set_shared_subscription_available(true)
            .set_response_information("response")
            .set_server_keep_alive(25)
            .set_server_reference("new.server")
            .set_authentication_method("auth")
            .set_authentication_data(b"secret");
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
        assert_eq!(
            &buf,
            &[
                32, 143, 1, 0, 135, //
                // properties
                139, 1, //
                // session expiry interval
                17, 0, 0, 0, 24, //
                // receive maximum
                33, 0, 42, //
                // max qos
                36, 1, //
                // retain available
                37, 0, //
                // maximum packet size
                39, 0, 0, 0, 100, //
                // client identifier
                18, 0, 15, b's', b'e', b'r', b'v', b'e', b'r', b'-', b'a', b's', b's', b'i', b'g',
                b'n', b'e', b'd', //
                // topic alias maximum
                34, 0, 5, //
                // reason
                31, 0, 9, b'a', b'r', b'b', b'i', b't', b'r', b'a', b'r', b'y', //
                //// User property
                38, 0, 9, b'p', b'r', b'o', b'p', b'e', b'r', b't', b'y', b'0', 0, 6, b'v', b'a',
                b'l', b'u', b'e', b'0', //
                // User property
                38, 0, 9, b'p', b'r', b'o', b'p', b'e', b'r', b't', b'y', b'1', 0, 6, b'v', b'a',
                b'l', b'u', b'e', b'1', //
                // wildcard subscription available
                40, 1, //
                // Subscription identifier available
                41, 0, //
                // Shared subscription available
                42, 1, //
                // server keep alive
                19, 0, 25, //
                // Response information
                26, 0, 8, b'r', b'e', b's', b'p', b'o', b'n', b's', b'e', //
                // server reference
                28, 0, 10, b'n', b'e', b'w', b'.', b's', b'e', b'r', b'v', b'e', b'r', //
                // authentication method
                21, 0, 4, b'a', b'u', b't', b'h', //
                // authentication data
                22, 0, 6, b's', b'e', b'c', b'r', b'e', b't', //
            ]
        );
    }

    #[test]
    fn deserialize() {
        let msg = [32, 3, 0, 0, 0];
        let mut expected = ConnAck::new_v5(false, ConnectRcV5::Accepted);
        expected.re_calculate_fixed_header_length();
        let mut reader = BufReader::new(&msg[..]);
        let header = FixedHeader::try_read_sync(&mut reader).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(ConnAck::try_read_v5(header, &data[..]).unwrap(), expected);
    }

    #[test]
    fn deserialize_session_present() {
        let msg = [32, 3, 1, 0, 0];
        let mut expected = ConnAck::new_v5(true, ConnectRcV5::Accepted);
        expected.re_calculate_fixed_header_length();
        let mut reader = BufReader::new(&msg[..]);
        let header = FixedHeader::try_read_sync(&mut reader).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(ConnAck::try_read_v5(header, &data[..]).unwrap(), expected);
    }
    #[test]
    fn deserialize_rc() {
        let msg = [32, 3, 0, 135, 0];
        let mut expected = ConnAck::new_v5(false, ConnectRcV5::NotAuthorized);
        expected.re_calculate_fixed_header_length();
        let mut reader = BufReader::new(&msg[..]);
        let header = FixedHeader::try_read_sync(&mut reader).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(ConnAck::try_read_v5(header, &data[..]).unwrap(), expected);
    }

    #[test]
    fn deserialize_properties() {
        let msg = [
            32, 143, 1, 0, 135, //
            // properties
            139, 1, //
            // session expiry interval
            17, 0, 0, 0, 24, //
            // receive maximum
            33, 0, 42, //
            // max qos
            36, 1, //
            // retain available
            37, 0, //
            // maximum packet size
            39, 0, 0, 0, 100, //
            // client identifier
            18, 0, 15, b's', b'e', b'r', b'v', b'e', b'r', b'-', b'a', b's', b's', b'i', b'g', b'n',
            b'e', b'd', //
            // topic alias maximum
            34, 0, 5, //
            // reason
            31, 0, 9, b'a', b'r', b'b', b'i', b't', b'r', b'a', b'r', b'y', //
            //// User property
            38, 0, 9, b'p', b'r', b'o', b'p', b'e', b'r', b't', b'y', b'0', 0, 6, b'v', b'a', b'l',
            b'u', b'e', b'0', //
            // User property
            38, 0, 9, b'p', b'r', b'o', b'p', b'e', b'r', b't', b'y', b'1', 0, 6, b'v', b'a', b'l',
            b'u', b'e', b'1', //
            // wildcard subscription available
            40, 1, //
            // Subscription identifier available
            41, 0, //
            // Shared subscription available
            42, 1, //
            // server keep alive
            19, 0, 25, //
            // Response information
            26, 0, 8, b'r', b'e', b's', b'p', b'o', b'n', b's', b'e', //
            // server reference
            28, 0, 10, b'n', b'e', b'w', b'.', b's', b'e', b'r', b'v', b'e', b'r', //
            // authentication method
            21, 0, 4, b'a', b'u', b't', b'h', //
            // authentication data
            22, 0, 6, b's', b'e', b'c', b'r', b'e', b't', //
        ];
        let mut expected = ConnAck::new_v5(false, ConnectRcV5::NotAuthorized)
            .set_session_expiry_interval(24)
            .set_receive_maximum(42)
            .set_maximum_qos(Qos::AtLeastOnce)
            .set_retain_available(false)
            .set_maximum_packet_size(100)
            .set_client_identifier("server-assigned")
            .set_topic_alias_maximum(5)
            .set_reason("arbitrary")
            .set_user_property(vec![
                UserProperty {
                    key: "property0",
                    value: "value0",
                },
                UserProperty {
                    key: "property1",
                    value: "value1",
                },
            ])
            .set_wildcard_subscription_available(true)
            .set_subscription_identifiers_available(false)
            .set_shared_subscription_available(true)
            .set_response_information("response")
            .set_server_keep_alive(25)
            .set_server_reference("new.server")
            .set_authentication_method("auth")
            .set_authentication_data(b"secret");
        expected.re_calculate_fixed_header_length();
        let mut reader = BufReader::new(&msg[..]);
        let header = FixedHeader::try_read_sync(&mut reader).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(ConnAck::try_read_v5(header, &data[..]).unwrap(), expected);
    }
}

use bytes::{Buf, BufMut, Bytes};

use crate::{
    util::{extract_str, read_variable_len_int, variable_len_int_size, write_variable_len_int},
    version::PacketProperties,
    ControlPacketType, Error, FixedHeader, MalformedPacket, MqttV3_1_1, MqttV5_0_0, MqttVersion,
    Packet, Property, PropertyIdentifier, UserProperty,
};

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum DisconnectReasonCode {
    /// Close the connection normally. Do not send the Will Message.
    Normal = 0,
    /// The Client wishes to disconnect but requires that the Server also publishes its Will Message.
    DisconnectWithWill = 4,
    /// The Connection is closed but the sender either does not wish to reveal the reason, or none of the other Reason Codes apply.
    UnspecifiedError = 128,
    /// The received packet does not conform to this specification.
    MalformedPacket = 129,
    /// An unexpected or out of order packet was received.
    ProtocolError = 130,
    /// The packet received is valid but cannot be processed by this implementation.
    ImplementationSpecificError = 131,
    /// The request is not authorized.
    NotAuthorized = 135,
    /// The Server is busy and cannot continue processing requests from this Client.
    ServerBusy = 137,
    /// The Server is shutting down.
    ServerShuttingDown = 139,
    /// The Connection is closed because no packet has been received for 1.5 times the Keepalive time.
    KeepAliveTimeout = 141,
    /// Another Connection using the same ClientID has connected causing this Connection to be closed.
    SessionTakenOver = 142,
    /// The Topic Filter is correctly formed, but is not accepted by this Sever.
    TopicFilterInvalid = 143,
    /// The Topic Name is correctly formed, but is not accepted by this Client or Server.
    TopicNameInvalid = 144,
    /// The Client or Server has received more than Receive Maximum publication for which it has not sent PUBACK or PUBCOMP.
    ReceiveMaximumExceeded = 147,
    /// The Client or Server has received a PUBLISH packet containing a Topic Alias which is greater than the Maximum Topic Alias it sent in the CONNECT or CONNACK packet.
    TopicAliasInvalid = 148,
    /// The packet size is greater than Maximum Packet Size for this Client or Server.
    PacketTooLarge = 149,
    /// The received data rate is too high.
    MessageRateTooHigh = 150,
    /// An implementation or administrative imposed limit has been exceeded.
    QuotaExceeded = 151,
    /// The Connection is closed due to an administrative action.
    AdmininstrativeAction = 152,
    /// The payload format does not match the one specified by the Payload Format Indicator.
    PayloadFormatInvalid = 153,
    /// The Server has does not support retained messages.
    RetainNotSupported = 154,
    /// The Client specified a QoS greater than the QoS specified in a Maximum QoS in the CONNACK.
    QosNotSupported = 155,
    /// The Client should temporarily change its Server.
    UseAnotherServer = 156,
    /// The Server is moved and the Client should permanently change its server location.
    ServerMoved = 157,
    /// The Server does not support Shared Subscriptions.
    SharedSubscriptionNotSupported = 158,
    /// This connection is closed because the connection rate is too high.
    ConnectionRateExceeded = 159,
    /// The maximum connection time authorized for this connection has been exceeded.
    MaximumConnectionTime = 160,
    /// The Server does not support Subscription Identifiers; the subscription is not accepted.
    SubscriptionIdentifierNotSupported = 161,
    /// The Server does not support Wildcard Subscriptions; the subscription is not accepted.
    WildcardSubscriptionsAreNotSupported = 162,
}

impl TryFrom<u8> for DisconnectReasonCode {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Normal),
            4 => Ok(Self::DisconnectWithWill),
            128 => Ok(Self::UnspecifiedError),
            129 => Ok(Self::MalformedPacket),
            130 => Ok(Self::ProtocolError),
            131 => Ok(Self::ImplementationSpecificError),
            135 => Ok(Self::NotAuthorized),
            137 => Ok(Self::ServerBusy),
            139 => Ok(Self::ServerShuttingDown),
            141 => Ok(Self::KeepAliveTimeout),
            142 => Ok(Self::SessionTakenOver),
            143 => Ok(Self::TopicFilterInvalid),
            144 => Ok(Self::TopicNameInvalid),
            147 => Ok(Self::ReceiveMaximumExceeded),
            148 => Ok(Self::TopicAliasInvalid),
            149 => Ok(Self::PacketTooLarge),
            150 => Ok(Self::MessageRateTooHigh),
            151 => Ok(Self::QuotaExceeded),
            152 => Ok(Self::AdmininstrativeAction),
            153 => Ok(Self::PayloadFormatInvalid),
            154 => Ok(Self::RetainNotSupported),
            155 => Ok(Self::QosNotSupported),
            156 => Ok(Self::UseAnotherServer),
            157 => Ok(Self::ServerMoved),
            158 => Ok(Self::SharedSubscriptionNotSupported),
            159 => Ok(Self::ConnectionRateExceeded),
            160 => Ok(Self::MaximumConnectionTime),
            161 => Ok(Self::SubscriptionIdentifierNotSupported),
            162 => Ok(Self::WildcardSubscriptionsAreNotSupported),
            _ => Err(MalformedPacket::new("Invalid Disconnect reason code")),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DisconnectData {
    reason_code: DisconnectReasonCode,
    /// Followed by the Four Byte Integer representing the Session Expiry Interval in seconds. It is a Protocol Error to include the Session Expiry Interval more than once.
    ///
    /// If the Session Expiry Interval is absent, the Session Expiry Interval in the CONNECT packet is used.
    ///
    /// The Session Expiry Interval MUST NOT be sent on a DISCONNECT by the Server [MQTT-3.14.2-2].
    ///
    /// If the Session Expiry Interval in the CONNECT packet was zero, then it is a Protocol Error to set a non-zero Session Expiry Interval in the DISCONNECT packet sent by the Client. If such a non-zero Session Expiry Interval is received by the Server, it does not treat it as a valid DISCONNECT packet. The Server uses DISCONNECT with Reason Code 0x82 (Protocol Error) as described in section 4.13.
    session_expiry_interval: Option<u32>,
    /// Followed by the UTF-8 Encoded String representing the reason for the disconnect. This Reason String is human readable, designed for diagnostics and SHOULD NOT be parsed by the receiver.
    reason: Option<String>,
    user_property: Vec<UserProperty>,
    /// Followed by a UTF-8 Encoded String which can be used by the Client to identify another Server to use. It is a Protocol Error to include the Server Reference more than once.
    /// The Server sends DISCONNECT including a Server Reference and Reason Code 0x9C (Use another server) or 0x9D (Server moved) as described in section 4.13.
    server_reference: Option<String>,
}

impl PacketProperties for DisconnectData {
    fn try_read(data: &mut Bytes) -> Result<Self, Error> {
        // If disconnect fixed header remaining length == 0 -> reason_code == 0x00 + no properties
        if !data.has_remaining() {
            return Ok(DisconnectData {
                reason_code: DisconnectReasonCode::Normal,
                session_expiry_interval: None,
                reason: None,
                user_property: Vec::new(),
                server_reference: None,
            });
        }

        let reason_code = DisconnectReasonCode::try_from(data.try_get_u8()?)?;

        let len_properties = read_variable_len_int(data)? as usize;

        let mut properties = Self {
            reason_code,
            session_expiry_interval: None,
            reason: None,
            user_property: Vec::new(),
            server_reference: None,
        };

        if data.remaining() < len_properties {
            return Err(MalformedPacket::new("Packet too short to parse"));
        }

        let data = &mut data.split_to(len_properties);
        while data.has_remaining() {
            let property_identifier = crate::util::read_variable_len_int(data)?;
            let property_identifier = PropertyIdentifier::try_from(property_identifier)?;
            match property_identifier {
                PropertyIdentifier::SessionExpiryInterval => {
                    if properties.session_expiry_interval.is_some() {
                        return Err(Error::ProtocolError(
                            "SessionExpiryInterval specified multiple times",
                        ));
                    }
                    properties.session_expiry_interval =
                        Some(data.try_get_u32().map_err(|_| {
                            MalformedPacket::new("Packet too short to read property")
                        })?);
                }
                PropertyIdentifier::Reason => {
                    if properties.reason.is_some() {
                        return Err(Error::ProtocolError("Reason specified multiple times"));
                    }
                    properties.reason = Some(extract_str(data)?.to_string());
                }
                PropertyIdentifier::UserProperty => {
                    let key = extract_str(data)?;
                    let value = extract_str(data)?;
                    let property = UserProperty {
                        key: key.to_string(),
                        value: value.to_string(),
                    };
                    properties.user_property.push(property);
                }
                PropertyIdentifier::ServerReference => {
                    if properties.server_reference.is_some() {
                        return Err(Error::ProtocolError(
                            "ServerReference specified multiple times",
                        ));
                    }
                    let reference = extract_str(data)?;
                    properties.server_reference = Some(reference.to_string());
                }
                _ => {
                    return Err(MalformedPacket::new(
                        "Received unexpected property for connect",
                    ))
                }
            }
        }

        Ok(properties)
    }

    fn write_properties(&self, buf: &mut impl BufMut) {
        let properties_len = self.properties_len();

        // If disconnect fixed header remaining length == 0 -> reason_code == 0x00 + no properties
        if self.reason_code == DisconnectReasonCode::Normal && properties_len == 0 {
            return;
        }

        buf.put_u8(self.reason_code as u8);

        write_variable_len_int(properties_len as u64, buf);

        self.session_expiry_interval
            .serialize(PropertyIdentifier::SessionExpiryInterval, buf);
        self.reason.serialize(PropertyIdentifier::Reason, buf);
        self.user_property
            .serialize(PropertyIdentifier::UserProperty, buf);
        self.server_reference
            .serialize(PropertyIdentifier::ServerReference, buf);
    }

    fn properties_block_len(&self) -> usize {
        let l = self.properties_len();
        if l == 0 && self.reason_code == DisconnectReasonCode::Normal {
            0
        } else {
            1 + variable_len_int_size(l) + l
        }
    }

    fn properties_len(&self) -> usize {
        self.session_expiry_interval.property_len()
            + self.reason.property_len()
            + self.user_property.property_len()
            + self.server_reference.property_len()
    }
}

#[derive(Debug, Clone, PartialEq)]
/// The DISCONNECT Packet is the final Control Packet sent from the Client to the Server. It indicates that the Client is disconnecting cleanly.
pub struct Disconnect<V: MqttVersion>(V::DisconnectData);

impl<V: MqttVersion> Packet for Disconnect<V> {
    fn write_to_buf(&self, buf: &mut impl BufMut) {
        let header = FixedHeader::new(ControlPacketType::Disconnect, self.0.properties_block_len());
        header.write_to_buf(buf);
        self.0.write_properties(buf);
    }
    fn try_read(header: FixedHeader, data: &mut Bytes) -> Result<Self, Error> {
        assert_eq!(header.control_packet_type, ControlPacketType::Disconnect);
        V::DisconnectData::try_read(data).map(Self)
    }
}

impl Disconnect<MqttV3_1_1> {
    pub fn new_v3() -> Self {
        Self(())
    }
}

impl Disconnect<MqttV5_0_0> {
    pub fn new_v5(
        reason_code: DisconnectReasonCode,
        session_expiry_interval: Option<u32>,
        reason: Option<String>,
        user_property: Vec<UserProperty>,
        server_reference: Option<String>,
    ) -> Self {
        Self(DisconnectData {
            reason_code,
            session_expiry_interval,
            reason,
            user_property,
            server_reference,
        })
    }

    pub fn reason_code(&self) -> DisconnectReasonCode {
        self.0.reason_code
    }

    pub fn session_expiry_interval(&self) -> Option<u32> {
        self.0.session_expiry_interval
    }
    pub fn reason(&self) -> Option<&String> {
        self.0.reason.as_ref()
    }
    pub fn user_property(&self) -> &[UserProperty] {
        &self.0.user_property
    }
    pub fn server_reference(&self) -> Option<&String> {
        self.0.server_reference.as_ref()
    }
}

#[cfg(test)]
mod disconnect_v3 {

    use bytes::BytesMut;

    use super::*;

    #[test]
    fn serialize() {
        let mut buf = Vec::new();
        Disconnect::new_v3().write_to_buf(&mut buf);
        assert_eq!(&buf, &[224, 0]);
    }
    #[test]
    fn deserialize() {
        let msg = [224, 0];
        let expected = Disconnect::new_v3();
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(
            Disconnect::<MqttV3_1_1>::try_read(header, &mut body).unwrap(),
            expected
        );
    }
}

#[cfg(test)]
mod disconnect_v5 {

    use bytes::BytesMut;

    use super::*;

    #[test]
    fn serialize() {
        let mut buf = Vec::new();
        Disconnect::new_v5(
            DisconnectReasonCode::QuotaExceeded,
            None,
            None,
            Vec::new(),
            None,
        )
        .write_to_buf(&mut buf);
        assert_eq!(&buf, &[224, 2, 151, 0]);
    }

    #[test]
    fn serialize_short() {
        let mut buf = Vec::new();
        Disconnect::new_v5(DisconnectReasonCode::Normal, None, None, Vec::new(), None)
            .write_to_buf(&mut buf);
        assert_eq!(&buf, &[224, 0]);
    }

    #[test]
    fn serialize_properties() {
        let mut buf = Vec::new();
        Disconnect::new_v5(
            DisconnectReasonCode::MalformedPacket,
            Some(123),
            Some("reason".to_string()),
            vec![UserProperty {
                key: "property1".into(),
                value: "value1".into(),
            }],
            Some("server".to_string()),
        )
        .write_to_buf(&mut buf);

        assert_eq!(
            &buf,
            &[
                224, 45, 129, // Properties
                43,  // session expiry interval
                17, 0, 0, 0, 123, // Reason
                31, 0, 6, b'r', b'e', b'a', b's', b'o', b'n', // user property
                38, 0, 9, b'p', b'r', b'o', b'p', b'e', b'r', b't', b'y', b'1', 0, 6, b'v', b'a',
                b'l', b'u', b'e', b'1', // Server reference
                28, 0, 6, b's', b'e', b'r', b'v', b'e', b'r'
            ]
        );
    }
    #[test]
    fn deserialize() {
        let msg = [224, 2, 151, 0];
        let expected = Disconnect::new_v5(
            DisconnectReasonCode::QuotaExceeded,
            None,
            None,
            Vec::new(),
            None,
        );
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(
            Disconnect::<MqttV5_0_0>::try_read(header, &mut body).unwrap(),
            expected
        );
    }

    #[test]
    fn deserialize_short() {
        let msg = [224, 0];
        let expected =
            Disconnect::new_v5(DisconnectReasonCode::Normal, None, None, Vec::new(), None);
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(
            Disconnect::<MqttV5_0_0>::try_read(header, &mut body).unwrap(),
            expected
        );
    }

    #[test]
    fn deserialize_properties() {
        let msg = [
            224, 45, 129, // Properties
            43,  // session expiry interval
            17, 0, 0, 0, 123, // Reason
            31, 0, 6, b'r', b'e', b'a', b's', b'o', b'n', // user property
            38, 0, 9, b'p', b'r', b'o', b'p', b'e', b'r', b't', b'y', b'1', 0, 6, b'v', b'a', b'l',
            b'u', b'e', b'1', // Server reference
            28, 0, 6, b's', b'e', b'r', b'v', b'e', b'r',
        ];
        let expected = Disconnect::new_v5(
            DisconnectReasonCode::MalformedPacket,
            Some(123),
            Some("reason".to_string()),
            vec![UserProperty {
                key: "property1".into(),
                value: "value1".into(),
            }],
            Some("server".to_string()),
        );
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(
            Disconnect::<MqttV5_0_0>::try_read(header, &mut body).unwrap(),
            expected
        );
    }
}

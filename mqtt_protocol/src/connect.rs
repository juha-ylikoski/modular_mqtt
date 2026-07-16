use bytes::{Buf, BufMut, Bytes};

use crate::fixed_header::ControlPacketType;
use crate::util::{variable_len_int_size, write_variable_len_int};
use crate::version::PacketProperties;
use crate::{
    Error, MalformedPacket, MqttTopic, MqttV3_1_1, MqttV5_0_0, MqttVersion, Packet, PayloadFormat,
    Property, PropertyIdentifier, UserProperty,
};

use super::fixed_header::FixedHeader;
use super::util::{extract_bytes, extract_str, write_str, Qos};

pub trait MqttLastWill: Sized + std::fmt::Debug + Clone + PartialEq {
    fn new(topic: MqttTopic, payload: Bytes, qos: Qos, retain: bool) -> Self;
    fn try_read(flags: u8, data: &mut Bytes) -> Result<Self, Error>;
    fn write_to_buf(&self, buf: &mut impl BufMut);
    fn topic(&self) -> &str;
    fn qos(&self) -> Qos;
    fn retain(&self) -> bool;
    fn payload(&self) -> Bytes;
    fn block_size(&self) -> usize;
}

#[derive(Debug, PartialEq, Clone)]
pub struct MqttLastWill3_1_1 {
    topic: String,
    payload: Bytes,
    retain: bool,
    qos: Qos,
}

impl MqttLastWill for MqttLastWill3_1_1 {
    fn new(topic: MqttTopic, payload: Bytes, qos: Qos, retain: bool) -> Self {
        Self {
            topic: topic.0,
            payload,
            retain,
            qos,
        }
    }

    fn try_read(flags: u8, data: &mut Bytes) -> Result<Self, Error> {
        let topic = extract_str(data)?;
        let payload = extract_bytes(data)?;
        Ok(MqttLastWill3_1_1 {
            topic,
            payload,
            retain: Flags::WillRetain.flag_set(flags),
            qos: Qos::try_from((flags & 0b00011000) >> 3)?,
        })
    }

    fn write_to_buf(&self, buf: &mut impl BufMut) {
        write_str(&self.topic, buf);
        let pl_len = self.payload.len();
        buf.put_u16(pl_len as u16);
        buf.put(&self.payload[..]);
    }

    fn topic(&self) -> &str {
        &self.topic
    }
    fn qos(&self) -> Qos {
        self.qos
    }
    fn retain(&self) -> bool {
        self.retain
    }
    fn payload(&self) -> Bytes {
        self.payload.clone()
    }
    fn block_size(&self) -> usize {
        2 + self.topic.len() + 2 + self.payload.len()
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct MqttLastWill5_0_0 {
    topic: String,
    payload: Bytes,
    retain: bool,
    qos: Qos,
    // v5 properties
    /// Integer representing the Will Delay Interval in seconds
    /// If the Will Delay Interval is absent, the default value is 0 and there is no delay before the Will
    /// Message is published
    ///
    /// The Server delays publishing the Client’s Will Message until the Will Delay Interval has passed or
    /// the Session ends, whichever happens first. If a new Network Connection to this Session is made before
    /// the Will Delay Interval has passed, the Server MUST NOT send the Will Message
    delay_interval: Option<u32>,
    /// Payload Format Indicator, either of:
    ///   - 0 (0x00) Byte Indicates that the Will Message is unspecified bytes, which is equivalent to not sending a Payload Format Indicator.
    ///   - 1 (0x01) Byte Indicates that the Will Message is UTF-8 Encoded Character Data. The UTF-8 data in the Payload MUST be well-formed UTF-8
    payload_format: Option<PayloadFormat>,
    /// If present, the Four Byte value is the lifetime of the Will Message in seconds and is sent as the
    /// Publication Expiry Interval when the Server publishes the Will Message.
    /// If absent, no Message Expiry Interval is sent when the Server publishes the Will Message.
    message_expiry_interval: Option<u32>,
    /// UTF-8 Encoded String describing the content of the Will Message
    /// The value of the Content Type is defined by the sending and receiving application.
    content_type: Option<String>,
    /// UTF-8 Encoded String which is used as the Topic Name for a response message
    /// The presence of a Response Topic identifies the Will Message as a Request.
    response_topic: Option<String>,
    /// The Correlation Data is used by the sender of the Request Message to identify which request the Response Message is for when it is received
    /// The value of the Correlation Data only has meaning to the sender of the Request Message and receiver of the Response Message.
    correlation_data: Bytes,
    user_property: Vec<UserProperty>,
}

impl MqttLastWill for MqttLastWill5_0_0 {
    fn new(topic: MqttTopic, payload: Bytes, qos: Qos, retain: bool) -> Self {
        Self {
            topic: topic.0,
            payload,
            retain,
            qos,
            delay_interval: None,
            payload_format: None,
            message_expiry_interval: None,
            content_type: None,
            response_topic: None,
            correlation_data: Bytes::new(),
            user_property: Vec::new(),
        }
    }
    fn try_read(flags: u8, data: &mut Bytes) -> Result<Self, Error> {
        let properties_len = crate::util::read_variable_len_int(data)? as usize;

        let mut delay_interval = None;
        let mut payload_format = None;
        let mut message_expiry_interval = None;
        let mut content_type = None;
        let mut response_topic = None;
        let mut correlation_data = Bytes::new();
        let mut user_property = Vec::new();

        if data.remaining() < properties_len {
            return Err(MalformedPacket::new(
                "Packet too short to read will properties",
            ));
        }

        let properties = &mut data.split_to(properties_len);
        while properties.has_remaining() {
            let property_identifier = crate::util::read_variable_len_int(properties)?;
            let property_identifier = PropertyIdentifier::try_from(property_identifier)?;

            match property_identifier {
                PropertyIdentifier::WillDelayInterval => {
                    if delay_interval.is_some() {
                        return Err(Error::ProtocolError(
                            "WillDelayInterval specified multiple times",
                        ));
                    }
                    delay_interval =
                        Some(properties.try_get_u32().map_err(|_| {
                            MalformedPacket::new("Packet too short to read property")
                        })?);
                }
                PropertyIdentifier::PayloadFormatIndicator => {
                    if payload_format.is_some() {
                        return Err(Error::ProtocolError(
                            "PayloadFormatIndicator specified multiple times",
                        ));
                    }
                    payload_format = Some(
                        match properties.try_get_u8().map_err(|_| {
                            MalformedPacket::new("Packet too short to read property")
                        })? {
                            0 => PayloadFormat::Binary,
                            1 => PayloadFormat::Utf8,
                            _ => return Err(MalformedPacket::new("Invalid payload format")),
                        },
                    );
                }
                PropertyIdentifier::MessageExpiryInterval => {
                    if message_expiry_interval.is_some() {
                        return Err(Error::ProtocolError(
                            "MessageExpiryInterval specified multiple times",
                        ));
                    }
                    message_expiry_interval =
                        Some(properties.try_get_u32().map_err(|_| {
                            MalformedPacket::new("Packet too short to read property")
                        })?);
                }
                PropertyIdentifier::ContentType => {
                    if content_type.is_some() {
                        return Err(Error::ProtocolError("ContentType specified multiple times"));
                    }
                    content_type = Some(extract_str(properties)?);
                }
                PropertyIdentifier::ResponseTopic => {
                    if response_topic.is_some() {
                        return Err(Error::ProtocolError(
                            "ResponseTopic specified multiple times",
                        ));
                    }
                    response_topic = Some(extract_str(properties)?);
                }
                PropertyIdentifier::CorrelationData => {
                    if !correlation_data.is_empty() {
                        return Err(Error::ProtocolError(
                            "CorrelationData specified multiple times",
                        ));
                    }
                    correlation_data = extract_bytes(properties)?;
                }
                PropertyIdentifier::UserProperty => {
                    let key = extract_str(properties)?;
                    let value = extract_str(properties)?;
                    user_property.push(UserProperty {
                        key: key.to_string(),
                        value: value.to_string(),
                    });
                }
                _ => {
                    return Err(MalformedPacket::new(
                        "Received unexpected property for will",
                    ))
                }
            }
        }
        let topic = extract_str(data)?;
        let payload = extract_bytes(data)?;

        Ok(MqttLastWill5_0_0 {
            topic,
            payload,
            retain: Flags::WillRetain.flag_set(flags),
            qos: Qos::try_from((flags & 0b00011000) >> 3)?,
            delay_interval,
            payload_format,
            message_expiry_interval,
            content_type,
            response_topic,
            correlation_data,
            user_property,
        })
    }

    fn write_to_buf(&self, buf: &mut impl BufMut) {
        write_variable_len_int(self.properties_len() as u64, buf);
        self.delay_interval
            .serialize(PropertyIdentifier::WillDelayInterval, buf);
        self.payload_format
            .serialize(PropertyIdentifier::PayloadFormatIndicator, buf);
        self.message_expiry_interval
            .serialize(PropertyIdentifier::MessageExpiryInterval, buf);
        self.content_type
            .serialize(PropertyIdentifier::ContentType, buf);
        self.response_topic
            .serialize(PropertyIdentifier::ResponseTopic, buf);
        self.correlation_data
            .serialize(PropertyIdentifier::CorrelationData, buf);
        self.user_property
            .serialize(PropertyIdentifier::UserProperty, buf);

        write_str(&self.topic, buf);
        let pl_len = self.payload.len();
        buf.put_u16(pl_len as u16);
        buf.put(&self.payload[..]);
    }
    fn topic(&self) -> &str {
        &self.topic
    }
    fn qos(&self) -> Qos {
        self.qos
    }
    fn retain(&self) -> bool {
        self.retain
    }
    fn payload(&self) -> Bytes {
        self.payload.clone()
    }
    fn block_size(&self) -> usize {
        let l = self.properties_len();
        variable_len_int_size(l) + l + 2 + self.topic.len() + 2 + self.payload.len()
    }
}

impl MqttLastWill5_0_0 {
    pub fn set_delay_interval(mut self, new_delay_interval: u32) -> Self {
        self.delay_interval = Some(new_delay_interval);
        self
    }
    pub fn delay_interval(&self) -> u32 {
        self.delay_interval.unwrap_or_default()
    }

    pub fn set_payload_format(mut self, format: PayloadFormat) -> Self {
        self.payload_format = Some(format);
        self
    }
    pub fn payload_format(&self) -> Option<PayloadFormat> {
        self.payload_format
    }
    pub fn set_message_expiry_interval(mut self, interval: u32) -> Self {
        self.message_expiry_interval = Some(interval);
        self
    }
    pub fn message_expiry_interval(&self) -> Option<u32> {
        self.message_expiry_interval
    }

    pub fn set_content_type(mut self, new_content_type: String) -> Self {
        self.content_type = Some(new_content_type);
        self
    }
    pub fn content_type(&self) -> &Option<String> {
        &self.content_type
    }

    pub fn set_response_topic(mut self, new_response_topic: String) -> Self {
        self.response_topic = Some(new_response_topic);
        self
    }
    pub fn response_topic(&self) -> &Option<String> {
        &self.response_topic
    }

    pub fn set_correlation_data(mut self, new_correlation_data: Bytes) -> Self {
        self.correlation_data = new_correlation_data;
        self
    }
    pub fn correlation_data(&self) -> Bytes {
        self.correlation_data.clone()
    }

    pub fn set_user_property(mut self, new_user_property: Vec<UserProperty>) -> Self {
        self.user_property = new_user_property;
        self
    }
    pub fn user_property(&self) -> &[UserProperty] {
        &self.user_property
    }

    pub fn properties_len(&self) -> usize {
        self.delay_interval.property_len()
            + self.payload_format.property_len()
            + self.message_expiry_interval.property_len()
            + self.content_type.property_len()
            + self.response_topic.property_len()
            + self.correlation_data.property_len()
            + self.user_property.property_len()
    }
}

#[derive(Debug, PartialEq, Default)]
pub struct ConnectProperties {
    // v5 properties
    /// If the Session Expiry Interval is absent the value 0 is used. If it is set to 0, or is absent,
    /// the Session ends when the Network Connection is closed.
    /// If the Session Expiry Interval is 0xFFFFFFFF (UINT_MAX), the Session does not expire.
    session_expiry_interval: Option<u32>,

    /// The Client uses this value to limit the number of QoS 1 and QoS 2 publications that it is willing
    /// to process concurrently. There is no mechanism to limit the QoS 0 publications that the Server might try to send.
    /// The value of Receive Maximum applies only to the current Network Connection. If the Receive Maximum
    /// value is absent then its value defaults to 65,535.
    receive_maximum: Option<u16>,
    /// If the Maximum Packet Size is not present, no limit on the packet size is imposed beyond the limitations
    /// in the protocol as a result of the remaining length encoding and the protocol header sizes.
    /// The packet size is the total number of bytes in an MQTT Control Packet, as defined in section 2.1.4.
    /// The Client uses the Maximum Packet Size to inform the Server that it will not process packets exceeding this limit.
    maximum_packet_size: Option<u32>,
    /// If the Topic Alias Maximum property is absent, the default value is 0.
    /// This value indicates the highest value that the Client will accept as a Topic Alias sent by the Server.
    /// The Client uses this value to limit the number of Topic Aliases that it is willing to hold on this Connection
    /// A value of 0 indicates that the Client does not accept any Topic Aliases on this connection. If Topic
    /// Alias Maximum is absent or zero, the Server MUST NOT send any Topic Aliases to the Client
    topic_alias_maximum: Option<u16>,
    /// If the Request Response Information is absent, the value of 0 is used.
    /// The Client uses this value to request the Server to return Response Information in the CONNACK. A value of 0
    /// indicates that the Server MUST NOT return Response Information
    /// If the value is 1 the Server MAY return Response Information in the CONNACK packet.
    request_response_information: Option<bool>,
    /// The Client uses this value to indicate whether the Reason String or User Properties are sent in the case of failures.
    /// If the value of Request Problem Information is 0, the Server MAY return a Reason String or User Properties on
    /// a CONNACK or DISCONNECT packet, but MUST NOT send a Reason String or User Properties on any packet other than PUBLISH, CONNACK, or DISCONNECT
    request_problem_information: Option<bool>,
    /// The User Property is allowed to appear multiple times to represent multiple name, value pairs. The same name is allowed to appear more than once.
    user_property: Vec<UserProperty>,
    /// If Authentication Method is absent, extended authentication is not performed
    authentication_method: Option<String>,
    /// Binary Data containing authentication data
    /// The contents of this data are defined by the authentication method
    authentication_data: Bytes,
}

impl PacketProperties for ConnectProperties {
    fn try_read(data: &mut Bytes) -> Result<Self, Error> {
        let mut properties = Self::default();

        let properties_len = crate::util::read_variable_len_int(data)? as usize;
        if data.remaining() < properties_len {
            return Err(MalformedPacket::new("Packet too short to read properties"));
        }
        let end_of_properties = data.remaining() - properties_len;
        while data.remaining() > end_of_properties {
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
                PropertyIdentifier::ReceiveMaximum => {
                    if properties.receive_maximum.is_some() {
                        return Err(Error::ProtocolError(
                            "ReceiveMaximum specified multiple times",
                        ));
                    }
                    properties.receive_maximum =
                        Some(data.try_get_u16().map_err(|_| {
                            MalformedPacket::new("Packet too short to read property")
                        })?);
                }
                PropertyIdentifier::MaximumPacketSize => {
                    if properties.maximum_packet_size.is_some() {
                        return Err(Error::ProtocolError(
                            "MaximumPacketSize specified multiple times",
                        ));
                    }
                    properties.maximum_packet_size =
                        Some(data.try_get_u32().map_err(|_| {
                            MalformedPacket::new("Packet too short to read property")
                        })?);
                }
                PropertyIdentifier::TopicAliasMaximum => {
                    if properties.topic_alias_maximum.is_some() {
                        return Err(Error::ProtocolError(
                            "TopicAliasmaximum specified multiple times",
                        ));
                    }
                    properties.topic_alias_maximum =
                        Some(data.try_get_u16().map_err(|_| {
                            MalformedPacket::new("Packet too short to read property")
                        })?);
                }
                PropertyIdentifier::RequestResponseInformation => {
                    if properties.request_response_information.is_some() {
                        return Err(Error::ProtocolError(
                            "RequestResponseInformation specified multiple times",
                        ));
                    }
                    properties.request_response_information = Some(data.try_get_u8()? == 1);
                }
                PropertyIdentifier::RequestProblemInformation => {
                    if properties.request_problem_information.is_some() {
                        return Err(Error::ProtocolError(
                            "RequestProblemInformation specified multiple times",
                        ));
                    }
                    properties.request_problem_information = Some(data.try_get_u8()? == 1);
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
                PropertyIdentifier::AuthenticationMethod => {
                    if properties.authentication_method.is_some() {
                        return Err(Error::ProtocolError(
                            "AuthenticationMethod specified multiple times",
                        ));
                    }
                    properties.authentication_method = Some(extract_str(data)?);
                }
                PropertyIdentifier::AuthenticationData => {
                    if !properties.authentication_data.is_empty() {
                        return Err(Error::ProtocolError(
                            "AuthenticationData specified multiple times",
                        ));
                    }
                    properties.authentication_data = extract_bytes(data)?;
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
        let l = self.properties_len();
        write_variable_len_int(l as u64, buf);
        self.session_expiry_interval
            .serialize(PropertyIdentifier::SessionExpiryInterval, buf);
        self.receive_maximum
            .serialize(PropertyIdentifier::ReceiveMaximum, buf);
        self.maximum_packet_size
            .serialize(PropertyIdentifier::MaximumPacketSize, buf);
        self.topic_alias_maximum
            .serialize(PropertyIdentifier::TopicAliasMaximum, buf);
        self.request_response_information
            .serialize(PropertyIdentifier::RequestResponseInformation, buf);
        self.request_problem_information
            .serialize(PropertyIdentifier::RequestProblemInformation, buf);
        self.user_property
            .serialize(PropertyIdentifier::UserProperty, buf);
        self.authentication_method
            .serialize(PropertyIdentifier::AuthenticationMethod, buf);
        self.authentication_data
            .serialize(PropertyIdentifier::AuthenticationData, buf);
    }

    fn properties_len(&self) -> usize {
        self.session_expiry_interval.property_len()
            + self.receive_maximum.property_len()
            + self.maximum_packet_size.property_len()
            + self.topic_alias_maximum.property_len()
            + self.request_response_information.property_len()
            + self.request_problem_information.property_len()
            + self.user_property.property_len()
            + self.authentication_method.property_len()
            + self.authentication_data.property_len()
    }
}

#[derive(Debug, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum VersionedConnect {
    V3(Connect<MqttV3_1_1>),
    V5(Connect<MqttV5_0_0>),
}

impl Packet for VersionedConnect {
    fn try_read(header: FixedHeader, data: &mut Bytes) -> Result<Self, Error> {
        assert_eq!(header.control_packet_type, ControlPacketType::Connect);
        let protocol_length = data.try_get_u16()? as usize;
        if data.remaining() < protocol_length {
            return Err(MalformedPacket::new("Packet too short to read property"));
        }
        if data[..protocol_length] != b"MQTT"[..] {
            return Err(MalformedPacket::new(
                "Connect packet bytes 3..6 did not contain 'MQTT'.",
            ));
        }
        data.advance(4);
        let protocol_level = data.try_get_u8()?;

        match protocol_level {
            4 => Connect::<MqttV3_1_1>::try_read_after_level(header, data).map(Self::V3),
            5 => Connect::<MqttV5_0_0>::try_read_after_level(header, data).map(Self::V5),
            _ => Err(MalformedPacket::UnexpectedMqttVersion(protocol_level).into()),
        }
    }
    fn write_to_buf(&self, buf: &mut impl BufMut) {
        match self {
            VersionedConnect::V3(connect) => connect.write_to_buf(buf),
            VersionedConnect::V5(connect) => connect.write_to_buf(buf),
        }
    }
}

#[derive(Debug, PartialEq)]
/// After a Network Connection is established by a Client to a Server, the first
/// Packet sent from the Client to the Server MUST be a CONNECT Packet
pub struct Connect<V: MqttVersion> {
    /// This bit specifies the handling of the Session state.
    ///
    /// The Client and Server can store Session state to enable reliable messaging to
    /// continue across a sequence of Network Connections. This bit is used to control
    /// the lifetime of the Session state.
    ///
    /// If CleanSession is set to 0, the Server MUST resume communications with the Client
    /// based on state from the current Session (as identified by the Client identifier).
    /// If there is no Session associated with the Client identifier the Server MUST create a
    /// new Session. The Client and Server MUST store the Session after the Client and
    /// Server are disconnected [MQTT-3.1.2-4]. After the disconnection of a Session that
    /// had CleanSession set to 0, the Server MUST store further QoS 1 and QoS 2 messages
    /// that match any subscriptions that the client had at the time of disconnection as
    /// part of the Session state [MQTT-3.1.2-5]. It MAY also store QoS 0 messages that
    /// meet the same criteria.
    ///
    /// If CleanSession is set to 1, the Client and Server MUST discard any previous Session
    /// and start a new one. This Session lasts as long as the Network Connection. State
    /// data associated with this Session MUST NOT be reused in any subsequent Session
    ///
    /// <http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718030>
    clean_session: bool,

    /// The Keep Alive is a time interval measured in seconds. Expressed as a 16-bit word,
    /// it is the maximum time interval that is permitted to elapse between the point at
    /// which the Client finishes transmitting one Control Packet and the point it starts
    /// sending the next. It is the responsibility of the Client to ensure that the interval
    /// between Control Packets being sent does not exceed the Keep Alive value. In the absence
    /// of sending any other Control Packets, the Client MUST send a PINGREQ Packet [MQTT-3.1.2-23].
    ///
    /// A Keep Alive value of zero (0) has the effect of turning off the keep alive mechanism.
    /// This means that, in this case, the Server is not required to disconnect the Client on the grounds of inactivity.
    ///
    /// <http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718030>
    keep_alive: u16,

    ///  If the Will Flag is set to 1 this indicates that, if the Connect request is accepted, a Will
    ///  Message MUST be stored on the Server and associated with the Network Connection. The
    ///  Will Message MUST be published when the Network Connection is subsequently closed unless
    ///  the Will Message has been deleted by the Server on receipt of a DISCONNECT Packet [MQTT-3.1.2-8].
    ///
    /// <http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718030>
    will: Option<V::LastWill>,
    /// The Client Identifier (ClientId) identifies the Client to the Server. Each Client connecting to
    /// the Server has a unique ClientId. The ClientId MUST be used by Clients and by Servers to identify
    /// state that they hold relating to this MQTT Session between the Client and the Server [MQTT-3.1.3-2].
    ///
    /// <http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718030>
    client_identifier: String,
    username: Option<String>,
    password: Option<Bytes>,
    properties: V::ConnectProperties,
}

#[derive(PartialEq)]
enum Flags {
    CleanSession = 1 << 1,
    Will = 1 << 2,
    WillRetain = 1 << 5,
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

impl<V: MqttVersion> Connect<V> {
    fn try_read_after_level(_header: FixedHeader, data: &mut Bytes) -> Result<Self, Error> {
        let flags = data.try_get_u8()?;
        let keep_alive = data.try_get_u16()?;

        let properties = V::ConnectProperties::try_read(data)?;

        // Payload
        let client_identifier = extract_str(data)?;

        let will = if Flags::Will.flag_set(flags) {
            Some(V::LastWill::try_read(flags, data)?)
        } else {
            None
        };
        let username = if Flags::Username.flag_set(flags) {
            Some(extract_str(data)?)
        } else {
            None
        };

        let password = if Flags::Password.flag_set(flags) {
            Some(extract_bytes(data)?)
        } else {
            None
        };

        Ok(Self {
            clean_session: Flags::CleanSession.flag_set(flags),
            keep_alive,
            username,
            password,
            properties,
            will,
            client_identifier,
        })
    }

    pub fn write_to_buf(&self, buf: &mut impl BufMut) {
        let variable_header_len = 10;
        let payload_len = {
            let mut len = self.properties.properties_block_len() + 2 + self.client_identifier.len();
            if let Some(will) = &self.will {
                len += will.block_size();
            }
            if let Some(username) = &self.username {
                len += username.len() + 2;
            }
            if let Some(password) = &self.password {
                len += password.len() + 2;
            }
            len
        };
        let fixed_header = FixedHeader::new(
            ControlPacketType::Connect,
            variable_header_len + payload_len,
        );

        fixed_header.write_to_buf(buf);
        let mut flags = 0;
        if self.username.is_some() {
            flags |= u8::from(Flags::Username);
        }
        if self.password.is_some() {
            flags |= u8::from(Flags::Password);
        }
        if let Some(will) = &self.will {
            flags |= u8::from(Flags::Will) | (will.qos() as u8) << 3;

            if will.retain() {
                flags |= u8::from(Flags::WillRetain);
            }
        }
        if self.clean_session {
            flags |= u8::from(Flags::CleanSession);
        }

        buf.put(
            [
                // Protocol name length
                0,
                4,
                // Protocol name
                b'M',
                b'Q',
                b'T',
                b'T',
                // Protocol level
                V::VERSION,
                flags,
                ((self.keep_alive & 0xff00) >> 8) as u8,
                (self.keep_alive & 0xff) as u8,
            ]
            .as_slice(),
        );

        self.properties.write_properties(buf);

        // Payload
        write_str(&self.client_identifier, buf);

        if let Some(will) = &self.will {
            will.write_to_buf(buf);
        }
        if let Some(username) = &self.username {
            write_str(username, buf);
        }
        if let Some(password) = &self.password {
            let pl_len = password.len();
            buf.put_u16(pl_len as u16);
            buf.put(&password[..]);
        }
    }

    pub fn set_clean_session(&mut self, clean_session: bool) {
        self.clean_session = clean_session;
    }

    pub fn clean_session(&self) -> bool {
        self.clean_session
    }

    pub fn set_keep_alive(&mut self, keep_alive: u16) {
        self.keep_alive = keep_alive;
    }

    pub fn keep_alive(&self) -> u16 {
        self.keep_alive
    }

    pub fn set_will(&mut self, will: Option<V::LastWill>) {
        self.will = will;
    }

    pub fn will(&self) -> Option<&V::LastWill> {
        self.will.as_ref()
    }

    pub fn set_client_identifier(&mut self, client_identifier: String) {
        self.client_identifier = client_identifier;
    }

    pub fn client_identifier(&self) -> &str {
        &self.client_identifier
    }

    pub fn set_username(&mut self, username: Option<String>) {
        self.username = username;
    }

    pub fn username(&self) -> Option<&String> {
        self.username.as_ref()
    }

    pub fn set_password(&mut self, password: Option<Bytes>) {
        self.password = password;
    }

    pub fn password(&self) -> Option<&Bytes> {
        self.password.as_ref()
    }
}

impl Connect<MqttV3_1_1> {
    pub fn new_v3(
        clean_session: bool,
        keep_alive: u16,
        client_identifier: String,
        will: Option<MqttLastWill3_1_1>,
        username: Option<String>,
        password: Option<Bytes>,
    ) -> Self {
        Self {
            clean_session,
            keep_alive,
            client_identifier,
            will,
            username,
            password,
            properties: (),
        }
    }
}

impl Connect<MqttV5_0_0> {
    pub fn new_v5(
        clean_session: bool,
        keep_alive: u16,
        client_identifier: String,
        will: Option<MqttLastWill5_0_0>,
        username: Option<String>,
        password: Option<Bytes>,
    ) -> Self {
        Self {
            clean_session,
            keep_alive,
            client_identifier,
            will,
            username,
            password,
            properties: ConnectProperties::default(),
        }
    }

    pub fn set_session_expiry_interval(mut self, expiry_interval: u32) -> Self {
        self.properties.session_expiry_interval = Some(expiry_interval);
        self
    }

    pub fn set_receive_maximum(mut self, receive_maximum: u16) -> Self {
        self.properties.receive_maximum = Some(receive_maximum);
        self
    }

    pub fn set_maximum_packet_size(mut self, maximum_packet_size: u32) -> Self {
        self.properties.maximum_packet_size = Some(maximum_packet_size);
        self
    }

    pub fn set_topic_alias_maximum(mut self, topic_alias_maximum: u16) -> Self {
        self.properties.topic_alias_maximum = Some(topic_alias_maximum);
        self
    }

    pub fn set_request_response_information(mut self, request_response_information: bool) -> Self {
        self.properties.request_response_information = Some(request_response_information);
        self
    }

    pub fn set_request_problem_information(mut self, request_problem_information: bool) -> Self {
        self.properties.request_problem_information = Some(request_problem_information);
        self
    }

    pub fn set_user_property(mut self, user_property: Vec<UserProperty>) -> Self {
        self.properties.user_property = user_property;
        self
    }

    pub fn set_authentication_method(mut self, authentication_method: String) -> Self {
        self.properties.authentication_method = Some(authentication_method);
        self
    }

    pub fn set_authentication_data(mut self, authentication_method: Bytes) -> Self {
        self.properties.authentication_data = authentication_method;
        self
    }

    pub fn session_expiry_interval(&self) -> Option<u32> {
        self.properties.session_expiry_interval
    }

    pub fn receive_maximum(&self) -> u16 {
        self.properties.receive_maximum.unwrap_or(65535)
    }
    pub fn maximum_packet_size(&self) -> Option<u32> {
        self.properties.maximum_packet_size
    }
    pub fn topic_alias_maximum(&self) -> u16 {
        self.properties.topic_alias_maximum.unwrap_or_default()
    }
    pub fn request_response_information(&self) -> bool {
        self.properties
            .request_response_information
            .unwrap_or_default()
    }
    pub fn request_problem_information(&self) -> bool {
        self.properties.request_problem_information.unwrap_or(true)
    }
    pub fn user_property(&self) -> &[UserProperty] {
        &self.properties.user_property
    }
    pub fn authentication_method(&self) -> &Option<String> {
        &self.properties.authentication_method
    }
    pub fn authentication_data(&self) -> Bytes {
        self.properties.authentication_data.clone()
    }
}

#[cfg(test)]
mod test_ser_v3 {

    use super::*;

    #[test]
    fn connect() {
        let mut buf = Vec::new();
        let msg = Connect::new_v3(false, 0, "client".to_string(), None, None, None);
        msg.write_to_buf(&mut buf);
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
    fn will() {
        let mut buf = Vec::new();
        let msg = Connect::new_v3(
            false,
            0,
            "client2".to_string(),
            Some(MqttLastWill3_1_1::new(
                "will".try_into().unwrap(),
                Bytes::from_static(b"payload"),
                Qos::ExactlyOnce,
                true,
            )),
            None,
            None,
        );
        msg.write_to_buf(&mut buf);
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
    fn username() {
        let mut buf = Vec::new();
        let msg = Connect::new_v3(
            false,
            0,
            "client".to_string(),
            None,
            Some("username".to_string()),
            None,
        );
        msg.write_to_buf(&mut buf);
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
    fn password() {
        let mut buf = Vec::new();
        let msg = Connect::new_v3(
            false,
            0,
            "client".to_string(),
            None,
            None,
            Some(Bytes::from_static(b"password")),
        );
        msg.write_to_buf(&mut buf);
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
    fn username_and_password() {
        let mut buf = Vec::new();
        let msg = Connect::new_v3(
            false,
            0,
            "client".to_string(),
            None,
            Some("username".to_string()),
            Some(Bytes::from_static(b"password")),
        );
        msg.write_to_buf(&mut buf);
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
    fn clean_session() {
        let mut buf = Vec::new();
        let msg = Connect::new_v3(true, 0, "client".to_string(), None, None, None);
        msg.write_to_buf(&mut buf);
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
    fn client_id() {
        let mut buf = Vec::new();
        let msg = Connect::new_v3(false, 1800, "client".to_string(), None, None, None);
        msg.write_to_buf(&mut buf);
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
mod test_ser_v5 {

    use super::*;
    #[test]
    fn connect() {
        let mut buf = Vec::new();
        let msg = Connect::new_v5(false, 0, "client".to_string(), None, None, None);
        msg.write_to_buf(&mut buf);
        assert_eq!(
            &buf,
            &[
                16, 19, 0, 4, b'M', b'Q', b'T', b'T', // Protocol name
                5,    // Protocol level
                0,    // flags
                0, 0, // Keep alive
                // Properties
                0, // Payload
                0, 6, b'c', b'l', b'i', b'e', b'n', b't' // Client identifier
            ]
        );
    }

    #[test]
    fn will_no_properties() {
        let mut buf = Vec::new();
        let msg = Connect::new_v5(
            false,
            0,
            "client2".to_string(),
            Some(MqttLastWill5_0_0::new(
                MqttTopic::try_from("will").unwrap(),
                Bytes::from_static(b"payload"),
                Qos::ExactlyOnce,
                true,
            )),
            None,
            None,
        );
        msg.write_to_buf(&mut buf);
        assert_eq!(
            &buf,
            &[
                16,
                36,
                // Protocol name
                0,
                4,
                b'M',
                b'Q',
                b'T',
                b'T',
                // Protocol level
                5,
                // flags
                (1 << 5) | (2 << 3) | (1 << 2),
                // Keep alive
                0,
                0,
                // properties
                0,
                // payload
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
                // Will properties
                0,
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
    fn will_properties() {
        let mut buf = Vec::new();
        let msg = Connect::new_v5(
            false,
            0,
            "client2".to_string(),
            Some(
                MqttLastWill5_0_0::new(
                    MqttTopic::try_from("will").unwrap(),
                    Bytes::from_static(b"payload"),
                    Qos::ExactlyOnce,
                    true,
                )
                .set_delay_interval(42)
                .set_payload_format(PayloadFormat::Utf8)
                .set_message_expiry_interval(24)
                .set_content_type("test".to_string())
                .set_response_topic("response".to_string())
                .set_correlation_data(Bytes::from_static(b"badcafee"))
                .set_user_property(vec![
                    UserProperty {
                        key: "property0".to_string(),
                        value: "value0".to_string(),
                    },
                    UserProperty {
                        key: "property1".to_string(),
                        value: "value1".to_string(),
                    },
                ]),
            ),
            None,
            None,
        );
        msg.write_to_buf(&mut buf);
        assert_eq!(
            &buf,
            &[
                16,
                117,
                // Protocol name
                0,
                4,
                b'M',
                b'Q',
                b'T',
                b'T',
                // Protocol level
                5,
                // flags
                (1 << 5) | (2 << 3) | (1 << 2),
                // Keep alive
                0,
                0,
                // properties
                0,
                // payload
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
                // Will properties
                81, // properties length
                // Delay interval
                24,
                0,
                0,
                0,
                42,
                // Payload format
                1,
                1,
                // Message expiry
                2,
                0,
                0,
                0,
                24,
                // Content type
                3,
                0,
                4,
                b't',
                b'e',
                b's',
                b't',
                // Response topic
                8,
                0,
                8,
                b'r',
                b'e',
                b's',
                b'p',
                b'o',
                b'n',
                b's',
                b'e',
                // correlation data
                9,
                0,
                8,
                b'b',
                b'a',
                b'd',
                b'c',
                b'a',
                b'f',
                b'e',
                b'e',
                // User property
                38,
                0,
                9,
                b'p',
                b'r',
                b'o',
                b'p',
                b'e',
                b'r',
                b't',
                b'y',
                b'0',
                0,
                6,
                b'v',
                b'a',
                b'l',
                b'u',
                b'e',
                b'0',
                // User property
                38,
                0,
                9,
                b'p',
                b'r',
                b'o',
                b'p',
                b'e',
                b'r',
                b't',
                b'y',
                b'1',
                0,
                6,
                b'v',
                b'a',
                b'l',
                b'u',
                b'e',
                b'1',
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
    fn username() {
        let mut buf = Vec::new();
        let msg = Connect::new_v5(
            false,
            0,
            "client".to_string(),
            None,
            Some("username".to_string()),
            None,
        );
        msg.write_to_buf(&mut buf);
        assert_eq!(
            &buf,
            &[
                16,
                29,
                // Protocol name
                0,
                4,
                b'M',
                b'Q',
                b'T',
                b'T',
                // Protocol level
                5,
                // flags
                1 << 7,
                // Keep alive
                0,
                0,
                // properties
                0,
                // Payload
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
    fn password() {
        let mut buf = Vec::new();
        let msg = Connect::new_v5(
            false,
            0,
            "client".to_string(),
            None,
            None,
            Some(Bytes::from_static(b"password")),
        );
        msg.write_to_buf(&mut buf);
        assert_eq!(
            &buf,
            &[
                16,
                29,
                // Protocol name
                0,
                4,
                b'M',
                b'Q',
                b'T',
                b'T',
                // Protocol level
                5,
                // flags
                1 << 6,
                // Keep alive
                0,
                0,
                // properties
                0,
                // payload
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
    fn username_and_password() {
        let mut buf = Vec::new();
        let msg = Connect::new_v5(
            false,
            0,
            "client".to_string(),
            None,
            Some("username".to_string()),
            Some(Bytes::from_static(b"password")),
        );
        msg.write_to_buf(&mut buf);
        assert_eq!(
            &buf,
            &[
                16,
                39,
                // Protocol name
                0,
                4,
                b'M',
                b'Q',
                b'T',
                b'T',
                // Protocol level
                5,
                // flags
                1 << 6 | 1 << 7,
                // Keep alive
                0,
                0,
                // properties
                0,
                // payload
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
    fn clean_session() {
        let mut buf = Vec::new();
        let msg = Connect::new_v5(true, 0, "client".to_string(), None, None, None);
        msg.write_to_buf(&mut buf);
        assert_eq!(
            &buf,
            &[
                16,
                19,
                // Protocol name
                0,
                4,
                b'M',
                b'Q',
                b'T',
                b'T',
                // Protocol level
                5,
                // flags
                1 << 1,
                // Keep alive
                0,
                0,
                // properties
                0,
                // payload
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
    fn client_id() {
        let mut buf = Vec::new();
        let msg = Connect::new_v5(false, 1800, "client".to_string(), None, None, None);
        msg.write_to_buf(&mut buf);
        assert_eq!(
            &buf,
            &[
                16, 19, 0, 4, b'M', b'Q', b'T', b'T', // Protocol name
                5,    // Protocol level
                0,    // flags
                0x07, 0x08, // Keep alive
                // properties
                0, // payload
                0, 6, b'c', b'l', b'i', b'e', b'n', b't', // Client identifier
            ]
        );
    }

    #[test]
    fn properties() {
        let mut buf = Vec::new();
        let msg = Connect::new_v5(false, 1800, "client".to_string(), None, None, None)
            .set_session_expiry_interval(42)
            .set_receive_maximum(24)
            .set_maximum_packet_size(100)
            .set_topic_alias_maximum(10)
            .set_request_response_information(true)
            .set_request_problem_information(true)
            .set_user_property(vec![
                UserProperty {
                    key: "property0".to_string(),
                    value: "value0".to_string(),
                },
                UserProperty {
                    key: "property1".to_string(),
                    value: "value1".to_string(),
                },
            ])
            .set_authentication_method("auth".to_string())
            .set_authentication_data(Bytes::from_static(b"secret"));
        msg.write_to_buf(&mut buf);
        assert_eq!(
            &buf,
            &[
                16, 95, 0, 4, b'M', b'Q', b'T', b'T', // Protocol name
                5,    // Protocol level
                0,    // flags
                0x07, 0x08, // Keep alive
                76,   // properties
                17, 0, 0, 0, 42, // Session expiry interval
                33, 0, 24, // receive maximum
                39, 0, 0, 0, 100, // maximum packet size
                34, 0, 10, // topic alias maximum
                25, 1, // request response information
                23, 1, // request problem information
                // User property
                38, 0, 9, b'p', b'r', b'o', b'p', b'e', b'r', b't', b'y', b'0', 0, 6, b'v', b'a',
                b'l', b'u', b'e', b'0', //
                // User property
                38, 0, 9, b'p', b'r', b'o', b'p', b'e', b'r', b't', b'y', b'1', 0, 6, b'v', b'a',
                b'l', b'u', b'e', b'1', //
                // authentication method
                21, 0, 4, b'a', b'u', b't', b'h', //
                // authentication method
                22, 0, 6, b's', b'e', b'c', b'r', b'e', b't', //
                // payload
                0, 6, b'c', b'l', b'i', b'e', b'n', b't', // Client identifier
            ]
        );
    }
}

#[cfg(test)]
mod test_de_v3 {

    use bytes::BytesMut;

    use super::*;

    #[test]
    fn connect() {
        let msg = [
            16, 18, 0, 4, b'M', b'Q', b'T', b'T', // Protocol name
            4,    // Protocol level
            0,    // flags
            0, 0, // Keep alive
            0, 6, b'c', b'l', b'i', b'e', b'n', b't', // Client identifier
        ];
        let expected = Connect::new_v3(false, 0, "client".to_string(), None, None, None);
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(
            VersionedConnect::try_read(header, &mut body).unwrap(),
            VersionedConnect::V3(expected)
        );
    }
    #[test]
    fn will() {
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
            "client2".to_string(),
            Some(MqttLastWill3_1_1::new(
                "will".try_into().unwrap(),
                Bytes::from_static(b"payload"),
                Qos::ExactlyOnce,
                true,
            )),
            None,
            None,
        );
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(
            VersionedConnect::try_read(header, &mut body).unwrap(),
            VersionedConnect::V3(expected)
        );
    }
    #[test]
    fn username() {
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
        let expected = Connect::new_v3(
            false,
            0,
            "client".to_string(),
            None,
            Some("username".to_string()),
            None,
        );
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(
            VersionedConnect::try_read(header, &mut body).unwrap(),
            VersionedConnect::V3(expected)
        );
    }
    #[test]
    fn password() {
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
        let expected = Connect::new_v3(
            false,
            0,
            "client".to_string(),
            None,
            None,
            Some(Bytes::from_static(b"password")),
        );
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(
            VersionedConnect::try_read(header, &mut body).unwrap(),
            VersionedConnect::V3(expected)
        );
    }
    #[test]
    fn username_and_password() {
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
            "client".to_string(),
            None,
            Some("username".to_string()),
            Some(Bytes::from_static(b"password")),
        );
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(
            VersionedConnect::try_read(header, &mut body).unwrap(),
            VersionedConnect::V3(expected)
        );
    }
    #[test]
    fn clean_session() {
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
        let expected = Connect::new_v3(true, 0, "client".to_string(), None, None, None);
        let mut reader = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut reader, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(
            VersionedConnect::try_read(header, &mut body).unwrap(),
            VersionedConnect::V3(expected)
        );
    }
    #[test]
    fn client_id() {
        let msg = [
            16, 18, 0, 4, b'M', b'Q', b'T', b'T', // Protocol name
            4,    // Protocol level
            0,    // flags
            0x07, 0x08, // Keep alive
            0, 6, b'c', b'l', b'i', b'e', b'n', b't', // Client identifier
        ];
        let expected = Connect::new_v3(false, 1800, "client".to_string(), None, None, None);
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(
            VersionedConnect::try_read(header, &mut body).unwrap(),
            VersionedConnect::V3(expected)
        );
    }
}

#[cfg(test)]
mod test_de_v5 {

    use bytes::BytesMut;

    use super::*;

    #[test]
    fn connect() {
        let msg = [
            16, 19, 0, 4, b'M', b'Q', b'T', b'T', // Protocol name
            5,    // Protocol level
            0,    // flags
            0, 0, // Keep alive
            // properties
            0, //
            // client identifier
            0, 6, b'c', b'l', b'i', b'e', b'n', b't',
        ];
        let expected = Connect::new_v5(false, 0, "client".to_string(), None, None, None);
        let mut reader = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut reader, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(
            VersionedConnect::try_read(header, &mut body).unwrap(),
            VersionedConnect::V5(expected)
        );
    }
    #[test]
    fn will_no_properties() {
        let msg = [
            16,
            36,
            // Protocol name
            0,
            4,
            b'M',
            b'Q',
            b'T',
            b'T',
            // Protocol level
            5,
            // flags
            (1 << 5) | (2 << 3) | (1 << 2),
            // Keep alive
            0,
            0,
            // Properties
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
            // Will properties
            0,
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
        let expected = Connect::new_v5(
            false,
            0,
            "client2".to_string(),
            Some(MqttLastWill5_0_0::new(
                MqttTopic::try_from("will").unwrap(),
                Bytes::from_static(b"payload"),
                Qos::ExactlyOnce,
                true,
            )),
            None,
            None,
        );
        let mut reader = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut reader, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(
            VersionedConnect::try_read(header, &mut body).unwrap(),
            VersionedConnect::V5(expected)
        );
    }

    #[test]
    fn will_properties() {
        let msg = &[
            16,
            117,
            // Protocol name
            0,
            4,
            b'M',
            b'Q',
            b'T',
            b'T',
            // Protocol level
            5,
            // flags
            (1 << 5) | (2 << 3) | (1 << 2),
            // Keep alive
            0,
            0,
            // properties
            0,
            // payload
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
            // Will properties
            81, // properties length
            // Delay interval
            24,
            0,
            0,
            0,
            42,
            // Payload format
            1,
            1,
            // Message expiry
            2,
            0,
            0,
            0,
            24,
            // Content type
            3,
            0,
            4,
            b't',
            b'e',
            b's',
            b't',
            // Response topic
            8,
            0,
            8,
            b'r',
            b'e',
            b's',
            b'p',
            b'o',
            b'n',
            b's',
            b'e',
            // correlation data
            9,
            0,
            8,
            b'b',
            b'a',
            b'd',
            b'c',
            b'a',
            b'f',
            b'e',
            b'e',
            // User property
            38,
            0,
            9,
            b'p',
            b'r',
            b'o',
            b'p',
            b'e',
            b'r',
            b't',
            b'y',
            b'0',
            0,
            6,
            b'v',
            b'a',
            b'l',
            b'u',
            b'e',
            b'0',
            // User property
            38,
            0,
            9,
            b'p',
            b'r',
            b'o',
            b'p',
            b'e',
            b'r',
            b't',
            b'y',
            b'1',
            0,
            6,
            b'v',
            b'a',
            b'l',
            b'u',
            b'e',
            b'1',
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
        let expected = Connect::new_v5(
            false,
            0,
            "client2".to_string(),
            Some(
                MqttLastWill5_0_0::new(
                    MqttTopic::try_from("will").unwrap(),
                    Bytes::from_static(b"payload"),
                    Qos::ExactlyOnce,
                    true,
                )
                .set_delay_interval(42)
                .set_payload_format(PayloadFormat::Utf8)
                .set_message_expiry_interval(24)
                .set_content_type("test".to_string())
                .set_response_topic("response".to_string())
                .set_correlation_data(Bytes::from_static(b"badcafee"))
                .set_user_property(vec![
                    UserProperty {
                        key: "property0".to_string(),
                        value: "value0".to_string(),
                    },
                    UserProperty {
                        key: "property1".to_string(),
                        value: "value1".to_string(),
                    },
                ]),
            ),
            None,
            None,
        );
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(
            VersionedConnect::try_read(header, &mut body).unwrap(),
            VersionedConnect::V5(expected)
        );
    }
    #[test]
    fn username() {
        let msg = [
            16,
            29,
            // Protocol name
            0,
            4,
            b'M',
            b'Q',
            b'T',
            b'T',
            // Protocol level
            5,
            // flags
            1 << 7,
            // Keep alive
            0,
            0,
            // Properties
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
        let expected = Connect::new_v5(
            false,
            0,
            "client".to_string(),
            None,
            Some("username".to_string()),
            None,
        );
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(
            VersionedConnect::try_read(header, &mut body).unwrap(),
            VersionedConnect::V5(expected)
        );
    }
    #[test]
    fn password() {
        let msg = [
            16,
            29,
            // Protocol name
            0,
            4,
            b'M',
            b'Q',
            b'T',
            b'T',
            // Protocol level
            5,
            // flags
            1 << 6,
            // Keep alive
            0,
            0,
            // Properties
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
        let expected = Connect::new_v5(
            false,
            0,
            "client".to_string(),
            None,
            None,
            Some(Bytes::from_static(b"password")),
        );
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(
            VersionedConnect::try_read(header, &mut body).unwrap(),
            VersionedConnect::V5(expected)
        );
    }
    #[test]
    fn username_and_password() {
        let msg = [
            16,
            39,
            // Protocol name
            0,
            4,
            b'M',
            b'Q',
            b'T',
            b'T',
            // Protocol level
            5,
            // flags
            1 << 6 | 1 << 7,
            // Keep alive
            0,
            0,
            // properties
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
        let expected = Connect::new_v5(
            false,
            0,
            "client".to_string(),
            None,
            Some("username".to_string()),
            Some(Bytes::from_static(b"password")),
        );
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(
            VersionedConnect::try_read(header, &mut body).unwrap(),
            VersionedConnect::V5(expected)
        );
    }
    #[test]
    fn clean_session() {
        let msg = [
            16,
            19,
            // Protocol name
            0,
            4,
            b'M',
            b'Q',
            b'T',
            b'T',
            // Protocol level
            5,
            // flags
            1 << 1,
            // Keep alive
            0,
            0,
            // properties
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
        let expected = Connect::new_v5(true, 0, "client".to_string(), None, None, None);
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(
            VersionedConnect::try_read(header, &mut body).unwrap(),
            VersionedConnect::V5(expected)
        );
    }
    #[test]
    fn client_id() {
        let msg = [
            16, 19, 0, 4, b'M', b'Q', b'T', b'T', // Protocol name
            5,    // Protocol level
            0,    // flags
            0x07, 0x08, // Keep alive
            // properties
            0, //
            // Client identifier
            0, 6, b'c', b'l', b'i', b'e', b'n', b't',
        ];
        let expected = Connect::new_v5(false, 1800, "client".to_string(), None, None, None);
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(
            VersionedConnect::try_read(header, &mut body).unwrap(),
            VersionedConnect::V5(expected)
        );
    }

    #[test]
    fn properties() {
        let msg = [
            16, 95, 0, 4, b'M', b'Q', b'T', b'T', // Protocol name
            5,    // Protocol level
            0,    // flags
            0x07, 0x08, // Keep alive
            76,   // properties
            17, 0, 0, 0, 42, // Session expiry interval
            33, 0, 24, // receive maximum
            39, 0, 0, 0, 100, // maximum packet size
            34, 0, 10, // topic alias maximum
            25, 1, // request response information
            23, 1, // request problem information
            // User property
            38, 0, 9, b'p', b'r', b'o', b'p', b'e', b'r', b't', b'y', b'0', 0, 6, b'v', b'a', b'l',
            b'u', b'e', b'0', //
            // User property
            38, 0, 9, b'p', b'r', b'o', b'p', b'e', b'r', b't', b'y', b'1', 0, 6, b'v', b'a', b'l',
            b'u', b'e', b'1', //
            // authentication method
            21, 0, 4, b'a', b'u', b't', b'h', //
            // authentication data
            22, 0, 6, b's', b'e', b'c', b'r', b'e', b't', //
            // payload
            0, 6, b'c', b'l', b'i', b'e', b'n', b't', // Client identifier
        ];

        let expected = Connect::new_v5(false, 1800, "client".to_string(), None, None, None)
            .set_session_expiry_interval(42)
            .set_receive_maximum(24)
            .set_maximum_packet_size(100)
            .set_topic_alias_maximum(10)
            .set_request_response_information(true)
            .set_request_problem_information(true)
            .set_user_property(vec![
                UserProperty {
                    key: "property0".to_string(),
                    value: "value0".to_string(),
                },
                UserProperty {
                    key: "property1".to_string(),
                    value: "value1".to_string(),
                },
            ])
            .set_authentication_method("auth".to_string())
            .set_authentication_data(Bytes::from_static(b"secret"));
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(
            VersionedConnect::try_read(header, &mut body).unwrap(),
            VersionedConnect::V5(expected)
        );
    }
}

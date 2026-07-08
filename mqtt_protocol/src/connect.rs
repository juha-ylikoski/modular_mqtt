use std::io::Write;

use bytes::{Buf, Bytes};

use crate::fixed_header::ControlPacketType;
use crate::util::variable_len_int_size;
use crate::{
    Error, MalformedPacket, MqttTopic, PayloadFormat, Property, PropertyIdentifier, UserProperty,
    MQTT_VERSION_3_1_1, MQTT_VERSION_5_0_0, SUPPORTED_PROTOCOL_VERSION,
};

use super::fixed_header::FixedHeader;
use super::util::{extract_bytes, extract_str, write_str, Qos};

#[derive(Debug, PartialEq)]
pub struct MqttLastWill3_1_1 {
    topic: String,
    payload: Bytes,
    retain: bool,
    qos: Qos,
}

impl MqttLastWill3_1_1 {
    pub fn new(topic: MqttTopic, payload: Bytes, retain: bool, qos: Qos) -> Self {
        Self {
            topic: topic.0,
            payload,
            retain,
            qos,
        }
    }
}

#[derive(Debug, PartialEq)]
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

#[derive(Debug, PartialEq)]
pub enum MqttLastWill {
    V3(MqttLastWill3_1_1),
    V5(MqttLastWill5_0_0),
}

impl MqttLastWill {
    pub fn topic(&self) -> &str {
        match self {
            MqttLastWill::V3(mqtt_last_will3_1_1) => &mqtt_last_will3_1_1.topic,
            MqttLastWill::V5(mqtt_v5_0_0) => &mqtt_v5_0_0.topic,
        }
    }
    pub fn qos(&self) -> Qos {
        match self {
            MqttLastWill::V3(mqtt_last_will3_1_1) => mqtt_last_will3_1_1.qos,
            MqttLastWill::V5(mqtt_v5_0_0) => mqtt_v5_0_0.qos,
        }
    }
    pub fn retain(&self) -> bool {
        match self {
            MqttLastWill::V3(mqtt_last_will3_1_1) => mqtt_last_will3_1_1.retain,
            MqttLastWill::V5(mqtt_v5_0_0) => mqtt_v5_0_0.retain,
        }
    }
    pub fn payload(&self) -> Bytes {
        match self {
            MqttLastWill::V3(mqtt_last_will3_1_1) => mqtt_last_will3_1_1.payload.clone(),
            MqttLastWill::V5(mqtt_v5_0_0) => mqtt_v5_0_0.payload.clone(),
        }
    }
}

impl MqttLastWill5_0_0 {
    pub fn new(topic: String, payload: Bytes, retain: bool, qos: Qos) -> Self {
        Self {
            topic,
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

    pub fn write_properties(&self, writer: &mut impl Write) -> Result<usize, std::io::Error> {
        let properties_len = self.properties_len();
        let mut length = crate::util::write_variable_len_int(properties_len as u64, writer)?;

        length += self
            .delay_interval
            .serialize(PropertyIdentifier::WillDelayInterval, writer)?
            + self
                .payload_format
                .serialize(PropertyIdentifier::PayloadFormatIndicator, writer)?
            + self
                .message_expiry_interval
                .serialize(PropertyIdentifier::MessageExpiryInterval, writer)?
            + self
                .content_type
                .serialize(PropertyIdentifier::ContentType, writer)?
            + self
                .response_topic
                .serialize(PropertyIdentifier::ResponseTopic, writer)?
            + self
                .correlation_data
                .serialize(PropertyIdentifier::CorrelationData, writer)?
            + self
                .user_property
                .serialize(PropertyIdentifier::UserProperty, writer)?;

        Ok(length)
    }

    fn read_properties(mut self, properties: &mut Bytes) -> Result<Self, Error> {
        let len_properties = properties.len();

        if properties.remaining() < len_properties {
            return Err(MalformedPacket::new(
                "Packet too short to read will properties",
            ));
        }

        let properties_end = properties.remaining() - len_properties;
        while properties.remaining() > properties_end {
            let property_identifier = crate::util::read_variable_len_int(properties)?;
            let property_identifier = PropertyIdentifier::try_from(property_identifier)?;

            match property_identifier {
                PropertyIdentifier::WillDelayInterval => {
                    if self.delay_interval.is_some() {
                        return Err(Error::ProtocolError(
                            "WillDelayInterval specified multiple times",
                        ));
                    }
                    self.delay_interval =
                        Some(properties.try_get_u32().map_err(|_| {
                            MalformedPacket::new("Packet too short to read property")
                        })?);
                }
                PropertyIdentifier::PayloadFormatIndicator => {
                    if self.payload_format.is_some() {
                        return Err(Error::ProtocolError(
                            "PayloadFormatIndicator specified multiple times",
                        ));
                    }
                    self.payload_format = Some(
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
                    if self.message_expiry_interval.is_some() {
                        return Err(Error::ProtocolError(
                            "MessageExpiryInterval specified multiple times",
                        ));
                    }
                    self.message_expiry_interval =
                        Some(properties.try_get_u32().map_err(|_| {
                            MalformedPacket::new("Packet too short to read property")
                        })?);
                }
                PropertyIdentifier::ContentType => {
                    if self.content_type.is_some() {
                        return Err(Error::ProtocolError("ContentType specified multiple times"));
                    }
                    self.content_type = Some(extract_str(properties)?);
                }
                PropertyIdentifier::ResponseTopic => {
                    if self.response_topic.is_some() {
                        return Err(Error::ProtocolError(
                            "ResponseTopic specified multiple times",
                        ));
                    }
                    self.response_topic = Some(extract_str(properties)?);
                }
                PropertyIdentifier::CorrelationData => {
                    if !self.correlation_data.is_empty() {
                        return Err(Error::ProtocolError(
                            "CorrelationData specified multiple times",
                        ));
                    }
                    self.correlation_data = extract_bytes(properties)?;
                }
                PropertyIdentifier::UserProperty => {
                    let key = extract_str(properties)?;
                    let value = extract_str(properties)?;
                    self.user_property.push(UserProperty {
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
        Ok(self)
    }
}

#[derive(Debug, PartialEq)]
/// After a Network Connection is established by a Client to a Server, the first
/// Packet sent from the Client to the Server MUST be a CONNECT Packet
pub struct Connect {
    fixed_header: FixedHeader,
    protocol_level: u8,

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
    pub clean_session: bool,

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
    pub keep_alive: u16,

    ///  If the Will Flag is set to 1 this indicates that, if the Connect request is accepted, a Will
    ///  Message MUST be stored on the Server and associated with the Network Connection. The
    ///  Will Message MUST be published when the Network Connection is subsequently closed unless
    ///  the Will Message has been deleted by the Server on receipt of a DISCONNECT Packet [MQTT-3.1.2-8].
    ///
    /// <http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718030>
    pub will: Option<MqttLastWill>,
    /// The Client Identifier (ClientId) identifies the Client to the Server. Each Client connecting to
    /// the Server has a unique ClientId. The ClientId MUST be used by Clients and by Servers to identify
    /// state that they hold relating to this MQTT Session between the Client and the Server [MQTT-3.1.3-2].
    ///
    /// <http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718030>
    pub client_identifier: String,
    pub username: Option<String>,
    pub password: Option<Bytes>,

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
    pub topic_alias_maximum: Option<u16>,
    /// If the Request Response Information is absent, the value of 0 is used.
    /// The Client uses this value to request the Server to return Response Information in the CONNACK. A value of 0
    /// indicates that the Server MUST NOT return Response Information
    /// If the value is 1 the Server MAY return Response Information in the CONNACK packet.
    pub request_response_information: Option<bool>,
    /// The Client uses this value to indicate whether the Reason String or User Properties are sent in the case of failures.
    /// If the value of Request Problem Information is 0, the Server MAY return a Reason String or User Properties on
    /// a CONNACK or DISCONNECT packet, but MUST NOT send a Reason String or User Properties on any packet other than PUBLISH, CONNACK, or DISCONNECT
    pub request_problem_information: Option<bool>,
    /// The User Property is allowed to appear multiple times to represent multiple name, value pairs. The same name is allowed to appear more than once.
    pub user_property: Vec<UserProperty>,
    /// If Authentication Method is absent, extended authentication is not performed
    pub authentication_method: Option<String>,
    /// Binary Data containing authentication data
    /// The contents of this data are defined by the authentication method
    pub authentication_data: Bytes,
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

impl Connect {
    fn read_property(
        &mut self,
        property_identifier: PropertyIdentifier,
        data: &mut Bytes,
    ) -> Result<(), crate::Error> {
        match property_identifier {
            PropertyIdentifier::SessionExpiryInterval => {
                if self.session_expiry_interval.is_some() {
                    return Err(Error::ProtocolError(
                        "SessionExpiryInterval specified multiple times",
                    ));
                }
                self.session_expiry_interval = Some(
                    data.try_get_u32()
                        .map_err(|_| MalformedPacket::new("Packet too short to read property"))?,
                );
            }
            PropertyIdentifier::ReceiveMaximum => {
                if self.receive_maximum.is_some() {
                    return Err(Error::ProtocolError(
                        "ReceiveMaximum specified multiple times",
                    ));
                }
                self.receive_maximum = Some(
                    data.try_get_u16()
                        .map_err(|_| MalformedPacket::new("Packet too short to read property"))?,
                );
            }
            PropertyIdentifier::MaximumPacketSize => {
                if self.maximum_packet_size.is_some() {
                    return Err(Error::ProtocolError(
                        "MaximumPacketSize specified multiple times",
                    ));
                }
                self.maximum_packet_size = Some(
                    data.try_get_u32()
                        .map_err(|_| MalformedPacket::new("Packet too short to read property"))?,
                );
            }
            PropertyIdentifier::TopicAliasMaximum => {
                if self.topic_alias_maximum.is_some() {
                    return Err(Error::ProtocolError(
                        "TopicAliasmaximum specified multiple times",
                    ));
                }
                self.topic_alias_maximum = Some(
                    data.try_get_u16()
                        .map_err(|_| MalformedPacket::new("Packet too short to read property"))?,
                );
            }
            PropertyIdentifier::RequestResponseInformation => {
                if self.request_response_information.is_some() {
                    return Err(Error::ProtocolError(
                        "RequestResponseInformation specified multiple times",
                    ));
                }
                self.request_response_information = Some(data.try_get_u8()? == 1);
            }
            PropertyIdentifier::RequestProblemInformation => {
                if self.request_problem_information.is_some() {
                    return Err(Error::ProtocolError(
                        "RequestProblemInformation specified multiple times",
                    ));
                }
                self.request_problem_information = Some(data.try_get_u8()? == 1);
            }
            PropertyIdentifier::UserProperty => {
                let key = extract_str(data)?;
                let value = extract_str(data)?;
                let property = UserProperty {
                    key: key.to_string(),
                    value: value.to_string(),
                };
                self.user_property.push(property);
            }
            PropertyIdentifier::AuthenticationMethod => {
                if self.authentication_method.is_some() {
                    return Err(Error::ProtocolError(
                        "AuthenticationMethod specified multiple times",
                    ));
                }
                self.authentication_method = Some(extract_str(data)?);
            }
            PropertyIdentifier::AuthenticationData => {
                if !self.authentication_data.is_empty() {
                    return Err(Error::ProtocolError(
                        "AuthenticationData specified multiple times",
                    ));
                }
                self.authentication_data = extract_bytes(data)?;
            }
            _ => {
                return Err(MalformedPacket::new(
                    "Received unexpected property for connect",
                ))
            }
        }
        Ok(())
    }
    pub fn try_read(header: FixedHeader, data: &mut Bytes) -> Result<Self, Error> {
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
        if !SUPPORTED_PROTOCOL_VERSION.contains(&protocol_level) {
            return Err(MalformedPacket::UnexpectedMqttVersion(protocol_level).into());
        }

        let flags = data.try_get_u8()?;
        let keep_alive = data.try_get_u16()?;

        let mut connect = Self {
            fixed_header: header,
            protocol_level,
            clean_session: Flags::CleanSession.flag_set(flags),
            keep_alive,
            username: None,
            password: None,
            will: None,
            client_identifier: String::new(),
            session_expiry_interval: None,
            receive_maximum: None,
            maximum_packet_size: None,
            topic_alias_maximum: None,
            request_response_information: None,
            request_problem_information: None,
            user_property: Vec::new(),
            authentication_method: None,
            authentication_data: Bytes::new(),
        };

        if protocol_level == MQTT_VERSION_5_0_0 {
            let properties_len = crate::util::read_variable_len_int(data)?;
            let properties_len = properties_len as usize;
            if data.remaining() < properties_len {
                return Err(MalformedPacket::new("Packet too short to read properties"));
            }
            let end_of_properties = data.remaining() - properties_len;
            while data.remaining() > end_of_properties {
                let property_identifier = crate::util::read_variable_len_int(data)?;
                let property_identifier = PropertyIdentifier::try_from(property_identifier)?;
                connect.read_property(property_identifier, data)?;
            }
        }

        // Payload
        connect.client_identifier = extract_str(data)?;

        if Flags::Will.flag_set(flags) {
            if connect.protocol_level == MQTT_VERSION_5_0_0 {
                let properties_len = crate::util::read_variable_len_int(data)?;
                let properties_len = properties_len as usize;
                let mut properties = data.slice(0..properties_len);
                data.advance(properties_len);
                let topic = extract_str(data)?;
                let payload = extract_bytes(data)?;
                connect.will = Some(MqttLastWill::V5(
                    MqttLastWill5_0_0::new(
                        topic,
                        payload,
                        Flags::WillRetain.flag_set(flags),
                        Qos::try_from((flags & 0b00011000) >> 3)?,
                    )
                    .read_properties(&mut properties)?,
                ));
            } else {
                let topic = extract_str(data)?;
                let payload = extract_bytes(data)?;
                connect.will = Some(MqttLastWill::V3(MqttLastWill3_1_1 {
                    topic,
                    payload,
                    retain: Flags::WillRetain.flag_set(flags),
                    qos: Qos::try_from((flags & 0b00011000) >> 3)?,
                }));
            }
        };

        if Flags::Username.flag_set(flags) {
            let username = extract_str(data)?;
            connect.username = Some(username);
        };
        if Flags::Password.flag_set(flags) {
            let password = extract_bytes(data)?;
            connect.password = Some(password);
        };

        Ok(connect)
    }

    fn write_until_properties(&mut self, writer: &mut impl Write) -> Result<usize, std::io::Error> {
        self.re_calculate_fixed_header_length();

        let mut length = self.fixed_header.write_to_stream(writer)?;
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
            self.protocol_level,
            flags,
            ((self.keep_alive & 0xff00) >> 8) as u8,
            (self.keep_alive & 0xff) as u8,
        ])?;
        length += 10;
        Ok(length)
    }

    pub fn write_to_stream(mut self, writer: &mut impl Write) -> Result<usize, std::io::Error> {
        let mut length = self.write_until_properties(writer)?;
        length += match self.protocol_level {
            MQTT_VERSION_3_1_1 => self.write_to_stream_v3(writer),
            MQTT_VERSION_5_0_0 => self.write_to_stream_v5(writer),
            _ => unreachable!(),
        }?;
        Ok(length)
    }
}

impl Connect {
    pub fn new_v3(
        clean_session: bool,
        keep_alive: u16,
        client_identifier: String,
        will: Option<MqttLastWill3_1_1>,
        username: Option<String>,
        password: Option<Bytes>,
    ) -> Self {
        let will = will.map(MqttLastWill::V3);
        let variable_header_len = 10;
        let payload_len = {
            let mut len = client_identifier.len() + 2;
            if let Some(will) = &will {
                len += will.topic().len() + 2 + will.payload().len() + 2;
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
            protocol_level: 4,
            clean_session,
            keep_alive,
            client_identifier,
            will,
            username,
            password,
            session_expiry_interval: None,
            receive_maximum: None,
            maximum_packet_size: None,
            topic_alias_maximum: None,
            request_response_information: None,
            request_problem_information: None,
            user_property: Vec::new(),
            authentication_method: None,
            authentication_data: Bytes::new(),
        }
    }

    fn write_to_stream_v3(self, writer: &mut impl Write) -> Result<usize, std::io::Error> {
        let mut length = 0;
        // Payload
        length += write_str(&self.client_identifier, writer)?;

        if let Some(will) = self.will {
            length += write_str(will.topic(), writer)?;
            let pl_len = will.payload().len();
            writer.write_all(&(pl_len as u16).to_be_bytes())?;
            writer.write_all(&will.payload())?;
            length += 2 + pl_len;
        }
        if let Some(username) = self.username {
            length += write_str(&username, writer)?;
        }
        if let Some(password) = self.password {
            let pl_len = password.len();
            writer.write_all(&(pl_len as u16).to_be_bytes())?;
            writer.write_all(&password)?;
            length += 2 + pl_len;
        }
        writer.flush()?;
        Ok(length)
    }
}

impl Connect {
    pub fn new_v5(
        clean_session: bool,
        keep_alive: u16,
        client_identifier: String,
        will: Option<MqttLastWill5_0_0>,
        username: Option<String>,
        password: Option<Bytes>,
    ) -> Self {
        let will = will.map(MqttLastWill::V5);
        let variable_header_len = 10;
        let payload_len = {
            let mut len = client_identifier.len() + 2;
            if let Some(will) = &will {
                len += will.topic().len() + 2 + will.payload().len() + 2;
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
            // + 1 for property length
            variable_header_len + 1 + payload_len,
        );
        Self {
            fixed_header,
            protocol_level: 5,
            clean_session,
            keep_alive,
            client_identifier,
            will,
            username,
            password,
            session_expiry_interval: None,
            receive_maximum: None,
            maximum_packet_size: None,
            topic_alias_maximum: None,
            request_response_information: None,
            request_problem_information: None,
            user_property: Vec::new(),
            authentication_method: None,
            authentication_data: Bytes::new(),
        }
    }
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
        if self.protocol_level == MQTT_VERSION_5_0_0 {
            let payload_len = {
                let mut len = self.client_identifier.len() + 2;
                if let Some(will) = &self.will {
                    len += will.topic().len() + 2 + will.payload().len() + 2;
                }
                if let Some(username) = &self.username {
                    len += username.len() + 2;
                }
                if let Some(password) = &self.password {
                    len += password.len() + 2;
                }
                len
            };

            let properties_size = self.properties_len();
            self.fixed_header.remaining_length = 10
                + variable_len_int_size(properties_size)
                + properties_size
                + self
                    .will
                    .as_ref()
                    .map(|will| {
                        let will = match will {
                            MqttLastWill::V3(_) => unreachable!(),
                            MqttLastWill::V5(will) => will,
                        };
                        let len = will.properties_len();
                        variable_len_int_size(len) + len
                    })
                    .unwrap_or_default()
                + payload_len;
        }
    }

    pub fn set_session_expiry_interval(mut self, expiry_interval: u32) -> Self {
        self.session_expiry_interval = Some(expiry_interval);
        self
    }

    pub fn set_receive_maximum(mut self, receive_maximum: u16) -> Self {
        self.receive_maximum = Some(receive_maximum);
        self
    }

    pub fn set_maximum_packet_size(mut self, maximum_packet_size: u32) -> Self {
        self.maximum_packet_size = Some(maximum_packet_size);
        self
    }

    pub fn set_topic_alias_maximum(mut self, topic_alias_maximum: u16) -> Self {
        self.topic_alias_maximum = Some(topic_alias_maximum);
        self
    }

    pub fn set_request_response_information(mut self, request_response_information: bool) -> Self {
        self.request_response_information = Some(request_response_information);
        self
    }

    pub fn set_request_problem_information(mut self, request_problem_information: bool) -> Self {
        self.request_problem_information = Some(request_problem_information);
        self
    }

    pub fn set_user_property(mut self, user_property: Vec<UserProperty>) -> Self {
        self.user_property = user_property;
        self
    }

    pub fn set_authentication_method(mut self, authentication_method: String) -> Self {
        self.authentication_method = Some(authentication_method);
        self
    }

    pub fn set_authentication_data(mut self, authentication_method: Bytes) -> Self {
        self.authentication_data = authentication_method;
        self
    }

    fn write_properties(&self, writer: &mut impl Write) -> Result<usize, std::io::Error> {
        let len = self
            .session_expiry_interval
            .serialize(PropertyIdentifier::SessionExpiryInterval, writer)?
            + self
                .receive_maximum
                .serialize(PropertyIdentifier::ReceiveMaximum, writer)?
            + self
                .maximum_packet_size
                .serialize(PropertyIdentifier::MaximumPacketSize, writer)?
            + self
                .topic_alias_maximum
                .serialize(PropertyIdentifier::TopicAliasMaximum, writer)?
            + self
                .request_response_information
                .serialize(PropertyIdentifier::RequestResponseInformation, writer)?
            + self
                .request_problem_information
                .serialize(PropertyIdentifier::RequestProblemInformation, writer)?
            + self
                .user_property
                .serialize(PropertyIdentifier::UserProperty, writer)?
            + self
                .authentication_method
                .serialize(PropertyIdentifier::AuthenticationMethod, writer)?
            + self
                .authentication_data
                .serialize(PropertyIdentifier::AuthenticationData, writer)?;

        Ok(len)
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

    fn write_to_stream_v5(self, writer: &mut impl Write) -> Result<usize, std::io::Error> {
        let mut length = 0;
        length += crate::util::write_variable_len_int(self.properties_len() as u64, writer)?;
        length += self.write_properties(writer)?;

        // Payload
        length += write_str(&self.client_identifier, writer)?;

        if let Some(will) = self.will {
            let will = match will {
                MqttLastWill::V3(_) => unreachable!(),
                MqttLastWill::V5(will) => will,
            };
            length += will.write_properties(writer)?;
            length += write_str(&will.topic, writer)?;
            let pl_len = will.payload.len();
            writer.write_all(&(pl_len as u16).to_be_bytes())?;
            writer.write_all(&will.payload)?;
            length += 2 + pl_len;
        }
        if let Some(username) = self.username {
            length += write_str(&username, writer)?;
        }
        if let Some(password) = self.password {
            let pl_len = password.len();
            writer.write_all(&(pl_len as u16).to_be_bytes())?;
            writer.write_all(&password)?;
            length += 2 + pl_len;
        }
        writer.flush()?;
        Ok(length)
    }

    pub fn session_expiry_interval(&self) -> Option<u32> {
        self.session_expiry_interval
    }

    pub fn receive_maximum(&self) -> u16 {
        self.receive_maximum.unwrap_or(65535)
    }
    pub fn maximum_packet_size(&self) -> Option<u32> {
        self.maximum_packet_size
    }
    pub fn topic_alias_maximum(&self) -> u16 {
        self.topic_alias_maximum.unwrap_or_default()
    }
    pub fn request_response_information(&self) -> bool {
        self.request_response_information.unwrap_or_default()
    }
    pub fn request_problem_information(&self) -> bool {
        self.request_problem_information.unwrap_or(true)
    }
    pub fn user_property(&self) -> &[UserProperty] {
        &self.user_property
    }
    pub fn authentication_method(&self) -> &Option<String> {
        &self.authentication_method
    }
    pub fn authentication_data(&self) -> Bytes {
        self.authentication_data.clone()
    }
}

#[cfg(test)]
mod test_ser_v3 {
    use std::io::BufWriter;

    use super::*;

    #[test]
    fn connect() {
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        let msg = Connect::new_v3(false, 0, "client".to_string(), None, None, None);
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
    fn will() {
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        let msg = Connect::new_v3(
            false,
            0,
            "client2".to_string(),
            Some(MqttLastWill3_1_1::new(
                "will".try_into().unwrap(),
                Bytes::from_static(b"payload"),
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
    fn username() {
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        let msg = Connect::new_v3(
            false,
            0,
            "client".to_string(),
            None,
            Some("username".to_string()),
            None,
        );
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
    fn password() {
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        let msg = Connect::new_v3(
            false,
            0,
            "client".to_string(),
            None,
            None,
            Some(Bytes::from_static(b"password")),
        );
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
    fn username_and_password() {
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        let msg = Connect::new_v3(
            false,
            0,
            "client".to_string(),
            None,
            Some("username".to_string()),
            Some(Bytes::from_static(b"password")),
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
    fn clean_session() {
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        let msg = Connect::new_v3(true, 0, "client".to_string(), None, None, None);
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
    fn client_id() {
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        let msg = Connect::new_v3(false, 1800, "client".to_string(), None, None, None);
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
mod test_ser_v5 {
    use std::io::BufWriter;

    use super::*;
    #[test]
    fn connect() {
        let mut buf = Vec::new();
        let mut writer = BufWriter::new(&mut buf);
        let msg = Connect::new_v5(false, 0, "client".to_string(), None, None, None);
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
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
        let mut writer = BufWriter::new(&mut buf);
        let msg = Connect::new_v5(
            false,
            0,
            "client2".to_string(),
            Some(MqttLastWill5_0_0::new(
                "will".to_string(),
                Bytes::from_static(b"payload"),
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
        let mut writer = BufWriter::new(&mut buf);
        let msg = Connect::new_v5(
            false,
            0,
            "client2".to_string(),
            Some(
                MqttLastWill5_0_0::new(
                    "will".to_string(),
                    Bytes::from_static(b"payload"),
                    true,
                    Qos::ExactlyOnce,
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
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
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
        let mut writer = BufWriter::new(&mut buf);
        let msg = Connect::new_v5(
            false,
            0,
            "client".to_string(),
            None,
            Some("username".to_string()),
            None,
        );
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
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
        let mut writer = BufWriter::new(&mut buf);
        let msg = Connect::new_v5(
            false,
            0,
            "client".to_string(),
            None,
            None,
            Some(Bytes::from_static(b"password")),
        );
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
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
        let mut writer = BufWriter::new(&mut buf);
        let msg = Connect::new_v5(
            false,
            0,
            "client".to_string(),
            None,
            Some("username".to_string()),
            Some(Bytes::from_static(b"password")),
        );
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
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
        let mut writer = BufWriter::new(&mut buf);
        let msg = Connect::new_v5(true, 0, "client".to_string(), None, None, None);
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
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
        let mut writer = BufWriter::new(&mut buf);
        let msg = Connect::new_v5(false, 1800, "client".to_string(), None, None, None);
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
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
        let mut writer = BufWriter::new(&mut buf);
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
        msg.write_to_stream(&mut writer).unwrap();
        drop(writer);
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
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE).unwrap().unwrap();
        assert_eq!(Connect::try_read(header, &mut body).unwrap(), expected);
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
                true,
                Qos::ExactlyOnce,
            )),
            None,
            None,
        );
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE).unwrap().unwrap();
        assert_eq!(Connect::try_read(header, &mut body).unwrap(), expected);
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
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE).unwrap().unwrap();
        assert_eq!(Connect::try_read(header, &mut body).unwrap(), expected);
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
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE).unwrap().unwrap();
        assert_eq!(Connect::try_read(header, &mut body).unwrap(), expected);
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
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE).unwrap().unwrap();
        assert_eq!(Connect::try_read(header, &mut body).unwrap(), expected);
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
        assert_eq!(Connect::try_read(header, &mut body).unwrap(), expected);
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
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE).unwrap().unwrap();
        assert_eq!(Connect::try_read(header, &mut body).unwrap(), expected);
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
        let mut expected = Connect::new_v5(false, 0, "client".to_string(), None, None, None);
        expected.re_calculate_fixed_header_length();
        let mut reader = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut reader, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(Connect::try_read(header, &mut body).unwrap(), expected);
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
        let mut expected = Connect::new_v5(
            false,
            0,
            "client2".to_string(),
            Some(MqttLastWill5_0_0::new(
                "will".to_string(),
                Bytes::from_static(b"payload"),
                true,
                Qos::ExactlyOnce,
            )),
            None,
            None,
        );
        expected.re_calculate_fixed_header_length();
        let mut reader = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut reader, crate::MAX_MQTT_PACKET_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(Connect::try_read(header, &mut body).unwrap(), expected);
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
        let mut expected = Connect::new_v5(
            false,
            0,
            "client2".to_string(),
            Some(
                MqttLastWill5_0_0::new(
                    "will".to_string(),
                    Bytes::from_static(b"payload"),
                    true,
                    Qos::ExactlyOnce,
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
        expected.re_calculate_fixed_header_length();
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE).unwrap().unwrap();
        assert_eq!(Connect::try_read(header, &mut body).unwrap(), expected);
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
        let mut expected = Connect::new_v5(
            false,
            0,
            "client".to_string(),
            None,
            Some("username".to_string()),
            None,
        );
        expected.re_calculate_fixed_header_length();
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE).unwrap().unwrap();
        assert_eq!(Connect::try_read(header, &mut body).unwrap(), expected);
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
        let mut expected = Connect::new_v5(
            false,
            0,
            "client".to_string(),
            None,
            None,
            Some(Bytes::from_static(b"password")),
        );
        expected.re_calculate_fixed_header_length();
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE).unwrap().unwrap();
        assert_eq!(Connect::try_read(header, &mut body).unwrap(), expected);
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
        let mut expected = Connect::new_v5(
            false,
            0,
            "client".to_string(),
            None,
            Some("username".to_string()),
            Some(Bytes::from_static(b"password")),
        );
        expected.re_calculate_fixed_header_length();
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE).unwrap().unwrap();
        assert_eq!(Connect::try_read(header, &mut body).unwrap(), expected);
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
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE).unwrap().unwrap();
        assert_eq!(Connect::try_read(header, &mut body).unwrap(), expected);
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
        let mut expected = Connect::new_v5(false, 1800, "client".to_string(), None, None, None);
        expected.re_calculate_fixed_header_length();
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE).unwrap().unwrap();
        assert_eq!(Connect::try_read(header, &mut body).unwrap(), expected);
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

        let mut expected = Connect::new_v5(false, 1800, "client".to_string(), None, None, None)
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
        expected.re_calculate_fixed_header_length();
        let mut buf = BytesMut::from(&msg[..]);
        let (header, mut body) = FixedHeader::parse(&mut buf, crate::MAX_MQTT_PACKET_SIZE).unwrap().unwrap();
        assert_eq!(Connect::try_read(header, &mut body).unwrap(), expected);
    }
}

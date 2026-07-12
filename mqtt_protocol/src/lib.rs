//! MQTT Packet Definitions.
//!
//! Quotes within this crate are sourced from the MQTT Version 5.0 OASIS Standard.
//! Copyright © OASIS Open 2019. All Rights Reserved.
//! See the NOTICE file in the project root for the full license text.

pub use ack_messages::{
    PubAck, PubAckReasonCode, PubComp, PubCompReasonCode, PubRec, PubRecReasonCode, PubRel,
    PubRelReasonCode, UnsubAck,
};
pub use auth::Auth;
use bytes::{BufMut, Bytes};
pub use connack::{ConnAck, ConnectRc, ConnectRcV3, ConnectRcV5};
pub use connect::{Connect, MqttLastWill, MqttLastWill3_1_1, MqttLastWill5_0_0};
pub use disconnect::Disconnect;
pub use fixed_header::{ControlPacketType, FixedHeader};
pub use ping::{PingReq, PingResp};
pub use publish::Publish;
pub use suback::{SubAck, SubRcV3};
pub use subscribe::{Subscribe, TopicSubscription};
pub use unsubscribe::Unsubscribe;
pub use util::{MqttTopic, Qos, QosPacketIdentifier};

mod ack_messages;
mod auth;
mod connack;
mod connect;
mod disconnect;
mod fixed_header;
mod ping;
mod publish;
mod suback;
mod subscribe;
mod unsubscribe;
mod util;

#[derive(Debug, PartialEq, Clone)]
pub struct MqttV3_1_1;
#[derive(Debug, PartialEq, Clone)]
pub struct MqttV5_0_0;

pub(crate) const MQTT_VERSION_3_1_1: u8 = 4;
pub(crate) const MQTT_VERSION_5_0_0: u8 = 5;

pub const MAX_MQTT_PACKET_SIZE: usize = 268_435_455 + 5;

const SUPPORTED_PROTOCOL_VERSION: &[u8] = &[MQTT_VERSION_3_1_1, MQTT_VERSION_5_0_0];

pub enum MqttPackage<V, Q> {
    Connect(Connect),
    ConnAck(ConnAck<V>),
    Publish(Publish<V, Q>),
    PubAck(PubAck<V>),
    PubRec(PubRec<V>),
    PubRel(PubRel<V>),
    PubComp(PubComp<V>),
    Subscribe(Subscribe<V>),
    SubAck(SubAck<V>),
    Unsubscribe(Unsubscribe<V>),
    UnsubscribeAck(UnsubAck<V>),
    PingReq(PingReq),
    PingResp(PingResp),
    Disconnect(Disconnect<V>),
    /// Mqtt V5 specific packet
    Auth(Auth<V>),
}

#[derive(Debug)]
pub enum MalformedPacket {
    Generic(&'static str),
    UnexpectedMqttVersion(u8),
    InvalidProperty(u64),
    Utf8Error(std::str::Utf8Error),
    InvalidQos(u8),
    InvalidFlags(u8, ControlPacketType),
    InvalidMqttTopic,
}

impl MalformedPacket {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(error: &'static str) -> Error {
        Error::MalformedPacket(Self::Generic(error))
    }
}

impl From<MalformedPacket> for Error {
    fn from(value: MalformedPacket) -> Self {
        Self::MalformedPacket(value)
    }
}

#[derive(Debug)]
pub enum Error {
    MalformedPacket(MalformedPacket),
    ProtocolError(&'static str),
    NotEnoughData,
    IoError(std::io::Error),
    PacketTooLarge {
        packet_size: usize,
        max_configured_size: usize,
    },
    Generic(&'static str),
}

impl From<bytes::TryGetError> for Error {
    fn from(_value: bytes::TryGetError) -> Self {
        MalformedPacket::new("Packet too short to read property")
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{self:?}"))
    }
}
impl std::error::Error for Error {}

#[derive(Debug, PartialEq)]
pub(crate) enum PropertyIdentifier {
    PayloadFormatIndicator = 1,
    MessageExpiryInterval = 2,
    ContentType = 3,
    ResponseTopic = 8,
    CorrelationData = 9,
    SubscriptionIdentifier = 11,
    SessionExpiryInterval = 17,
    AssignedClientIdentifier = 18,
    ServerKeepAlive = 19,
    AuthenticationMethod = 21,
    AuthenticationData = 22,
    RequestProblemInformation = 23,
    WillDelayInterval = 24,
    RequestResponseInformation = 25,
    ResponseInformation = 26,
    ServerReference = 28,
    Reason = 31,
    ReceiveMaximum = 33,
    TopicAliasMaximum = 34,
    TopicAlias = 35,
    MaximumQos = 36,
    RetainAvailable = 37,
    UserProperty = 38,
    MaximumPacketSize = 39,
    WildcardSubscriptionAvailable = 40,
    SubscriptionIdentifierAvailable = 41,
    SharedSubscriptionAvailable = 42,
}

impl TryFrom<u64> for PropertyIdentifier {
    type Error = Error;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::PayloadFormatIndicator),
            2 => Ok(Self::MessageExpiryInterval),
            3 => Ok(Self::ContentType),
            8 => Ok(Self::ResponseTopic),
            9 => Ok(Self::CorrelationData),
            11 => Ok(Self::SubscriptionIdentifier),
            17 => Ok(Self::SessionExpiryInterval),
            18 => Ok(Self::AssignedClientIdentifier),
            19 => Ok(Self::ServerKeepAlive),
            21 => Ok(Self::AuthenticationMethod),
            22 => Ok(Self::AuthenticationData),
            23 => Ok(Self::RequestProblemInformation),
            24 => Ok(Self::WillDelayInterval),
            25 => Ok(Self::RequestResponseInformation),
            26 => Ok(Self::ResponseInformation),
            28 => Ok(Self::ServerReference),
            31 => Ok(Self::Reason),
            33 => Ok(Self::ReceiveMaximum),
            34 => Ok(Self::TopicAliasMaximum),
            35 => Ok(Self::TopicAlias),
            36 => Ok(Self::MaximumQos),
            37 => Ok(Self::RetainAvailable),
            38 => Ok(Self::UserProperty),
            39 => Ok(Self::MaximumPacketSize),
            40 => Ok(Self::WildcardSubscriptionAvailable),
            41 => Ok(Self::SubscriptionIdentifierAvailable),
            42 => Ok(Self::SharedSubscriptionAvailable),
            _ => Err(MalformedPacket::InvalidProperty(value).into()),
        }
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum PayloadFormat {
    Binary,
    Utf8,
}

#[derive(Debug, PartialEq, Clone)]
pub struct UserProperty {
    pub key: String,
    pub value: String,
}

pub(crate) trait Property {
    fn serialize(&self, identifier: crate::PropertyIdentifier, buf: &mut impl BufMut);

    fn property_len(&self) -> usize;
}

impl Property for Option<u8> {
    fn serialize(&self, identifier: crate::PropertyIdentifier, buf: &mut impl BufMut) {
        let identifier = identifier as u8;
        if let Some(value) = self {
            buf.put_u8(identifier);
            buf.put_u8(*value);
        }
    }
    fn property_len(&self) -> usize {
        if self.is_some() {
            2
        } else {
            0
        }
    }
}
impl Property for Option<u16> {
    fn serialize(&self, identifier: crate::PropertyIdentifier, buf: &mut impl BufMut) {
        let identifier = identifier as u8;
        if let Some(value) = self {
            buf.put_u8(identifier);
            buf.put_u16(*value);
        }
    }
    fn property_len(&self) -> usize {
        if self.is_some() {
            3
        } else {
            0
        }
    }
}
impl Property for Option<u32> {
    fn serialize(&self, identifier: crate::PropertyIdentifier, buf: &mut impl BufMut) {
        let identifier = identifier as u8;
        if let Some(value) = self {
            buf.put_u8(identifier);
            buf.put_u32(*value);
        }
    }
    fn property_len(&self) -> usize {
        if self.is_some() {
            5
        } else {
            0
        }
    }
}

impl Property for Option<u64> {
    fn serialize(&self, identifier: crate::PropertyIdentifier, buf: &mut impl BufMut) {
        let identifier = identifier as u8;
        if let Some(value) = self {
            buf.put_u8(identifier);
            buf.put_u64(*value);
        }
    }
    fn property_len(&self) -> usize {
        if self.is_some() {
            9
        } else {
            0
        }
    }
}

impl Property for Option<bool> {
    fn serialize(&self, identifier: crate::PropertyIdentifier, buf: &mut impl BufMut) {
        let property = self.map(|v| v as u8);
        property.serialize(identifier, buf)
    }
    fn property_len(&self) -> usize {
        if self.is_some() {
            2
        } else {
            0
        }
    }
}

impl Property for Vec<UserProperty> {
    fn serialize(&self, identifier: crate::PropertyIdentifier, buf: &mut impl BufMut) {
        let identifier = identifier as u8;
        if !self.is_empty() {
            for property in self {
                buf.put_u8(identifier);
                buf.put_u16(property.key.len() as u16);
                buf.put(property.key.as_bytes());
                buf.put_u16(property.value.len() as u16);
                buf.put(property.value.as_bytes());
            }
        }
    }
    fn property_len(&self) -> usize {
        let mut len = 0;
        for property in self {
            len += 1 + 2 + property.key.len() + 2 + property.value.len();
        }
        len
    }
}
impl Property for Option<String> {
    fn serialize(&self, identifier: crate::PropertyIdentifier, buf: &mut impl BufMut) {
        let identifier = identifier as u8;
        if let Some(value) = self {
            buf.put_u8(identifier);
            crate::util::write_str(value, buf);
        }
    }
    fn property_len(&self) -> usize {
        if let Some(value) = self {
            1 + 2 + value.len()
        } else {
            0
        }
    }
}

impl Property for Bytes {
    fn serialize(&self, identifier: crate::PropertyIdentifier, buf: &mut impl BufMut) {
        assert!(self.len() <= 0xffff);
        let identifier = identifier as u8;
        if !self.is_empty() {
            buf.put_u8(identifier);
            let len = self.len() as u16;
            buf.put_u16(len);
            buf.put(&self[..])
        }
    }
    fn property_len(&self) -> usize {
        assert!(self.len() <= 0xffff);
        if !self.is_empty() {
            1 + 2 + self.len()
        } else {
            0
        }
    }
}

impl Property for Option<Bytes> {
    fn serialize(&self, identifier: crate::PropertyIdentifier, buf: &mut impl BufMut) {
        if let Some(value) = self {
            value.serialize(identifier, buf)
        }
    }
    fn property_len(&self) -> usize {
        if let Some(value) = self {
            value.property_len()
        } else {
            0
        }
    }
}

impl Property for Option<PayloadFormat> {
    fn serialize(&self, identifier: crate::PropertyIdentifier, buf: &mut impl BufMut) {
        let property = self.as_ref().map(|v| *v as u8);
        property.serialize(identifier, buf)
    }
    fn property_len(&self) -> usize {
        if self.is_some() {
            2
        } else {
            0
        }
    }
}

impl Property for Option<Qos> {
    fn serialize(&self, identifier: crate::PropertyIdentifier, buf: &mut impl BufMut) {
        let property = self.as_ref().map(|v| *v as u8);
        property.serialize(identifier, buf)
    }
    fn property_len(&self) -> usize {
        if self.is_some() {
            2
        } else {
            0
        }
    }
}

pub trait IntoPayload {
    fn into_payload(self) -> Bytes;
}

impl IntoPayload for Bytes {
    fn into_payload(self) -> Bytes {
        self
    }
}

impl IntoPayload for Vec<u8> {
    fn into_payload(self) -> Bytes {
        Bytes::from(self)
    }
}

impl IntoPayload for &'static [u8] {
    fn into_payload(self) -> Bytes {
        Bytes::from_static(self)
    }
}

impl<const N: usize> IntoPayload for &'static [u8; N] {
    fn into_payload(self) -> Bytes {
        Bytes::from_static(self)
    }
}

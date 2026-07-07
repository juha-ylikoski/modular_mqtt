//! MQTT Packet Definitions.
//!
//! Quotes within this crate are sourced from the MQTT Version 5.0 OASIS Standard.
//! Copyright © OASIS Open 2019. All Rights Reserved.
//! See the NOTICE file in the project root for the full license text.

use std::io::Write;

pub use auth::Auth;
pub use connack::{ConnAck, ConnectRc};
pub use connect::{Connect, MqttLastWill};
pub use disconnect::Disconnect;
pub use fixed_header::{ControlPacketType, FixedHeader, FixedHeaderError};
pub use only_fixed::{PingReq, PingResp};
pub use packet_identifier_msgs::{PubAck, PubComp, PubRec, PubRel, UnsubAck};
pub use publish::{Publish, ReceivedMessage};
pub use suback::{SubAck, SubRcV3};
pub use subscribe::{Subscribe, TopicSubscription};
pub use unsubscribe::Unsubscribe;
pub use util::{MqttTopic, PacketError, Qos, QosPacketIdentifier};

mod auth;
mod connack;
mod connect;
mod disconnect;
mod fixed_header;
mod only_fixed;
mod packet_identifier_msgs;
mod publish;
mod suback;
mod subscribe;
mod unsubscribe;
mod util;

#[derive(Debug, PartialEq)]
pub struct MqttV3_1_1;
#[derive(Debug, PartialEq)]
pub struct MqttV5_0_0;

pub(crate) const MQTT_VERSION_3_1_1: u8 = 4;
pub(crate) const MQTT_VERSION_5_0_0: u8 = 5;

const SUPPORTED_PROTOCOL_VERSION: &[u8] = &[MQTT_VERSION_3_1_1, MQTT_VERSION_5_0_0];

pub enum MqttPackage<'a, V> {
    Connect(Connect<'a>),
    ConnAck(ConnAck<'a, V>),
    Publish(Publish<'a, V>),
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
    Auth(),
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
}

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

#[derive(Debug, PartialEq)]
pub struct UserProperty<'a> {
    pub key: &'a str,
    pub value: &'a str,
}

#[derive(Debug, PartialEq)]
pub struct ReceivedUserProperty {
    pub key: String,
    pub value: String,
}

pub(crate) trait Property {
    fn serialize(
        &self,
        identifier: crate::PropertyIdentifier,
        writer: &mut impl Write,
    ) -> Result<usize, std::io::Error>;

    fn property_len(&self) -> usize;
}

impl Property for Option<u8> {
    fn serialize(
        &self,
        identifier: crate::PropertyIdentifier,
        writer: &mut impl Write,
    ) -> Result<usize, std::io::Error> {
        let identifier = identifier as u8;
        if let Some(value) = self {
            writer.write_all(&[identifier, *value]).map(|_| 2)
        } else {
            Ok(0)
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
    fn serialize(
        &self,
        identifier: crate::PropertyIdentifier,
        writer: &mut impl Write,
    ) -> Result<usize, std::io::Error> {
        let identifier = identifier as u8;
        if let Some(value) = self {
            let data = value.to_be_bytes();
            writer.write_all(&[identifier, data[0], data[1]]).map(|_| 3)
        } else {
            Ok(0)
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
    fn serialize(
        &self,
        identifier: crate::PropertyIdentifier,
        writer: &mut impl Write,
    ) -> Result<usize, std::io::Error> {
        let identifier = identifier as u8;
        if let Some(value) = self {
            let data = value.to_be_bytes();
            writer
                .write_all(&[identifier, data[0], data[1], data[2], data[3]])
                .map(|_| 5)
        } else {
            Ok(0)
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
    fn serialize(
        &self,
        identifier: crate::PropertyIdentifier,
        writer: &mut impl Write,
    ) -> Result<usize, std::io::Error> {
        let identifier = identifier as u8;
        if let Some(value) = self {
            let data = value.to_be_bytes();
            writer
                .write_all(&[
                    identifier, data[0], data[1], data[2], data[3], data[4], data[5], data[6],
                    data[7],
                ])
                .map(|_| 9)
        } else {
            Ok(0)
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

impl Property for Option<bool> {
    fn serialize(
        &self,
        identifier: crate::PropertyIdentifier,
        writer: &mut impl Write,
    ) -> Result<usize, std::io::Error> {
        let property = self.map(|v| v as u8);
        property.serialize(identifier, writer)
    }
    fn property_len(&self) -> usize {
        if self.is_some() {
            2
        } else {
            0
        }
    }
}
impl<'a> Property for &'a [UserProperty<'a>] {
    fn serialize(
        &self,
        identifier: crate::PropertyIdentifier,
        writer: &mut impl Write,
    ) -> Result<usize, std::io::Error> {
        let identifier = identifier as u8;
        if !self.is_empty() {
            let mut len = 0;
            for property in *self {
                writer.write_all(&[identifier])?;
                len += 1
                    + crate::util::write_str(property.key, writer)?
                    + crate::util::write_str(property.value, writer)?;
            }
            Ok(len)
        } else {
            Ok(0)
        }
    }
    fn property_len(&self) -> usize {
        let mut len = 0;
        for property in *self {
            len += 1 + 2 + property.key.len() + 2 + property.value.len();
        }
        len
    }
}
impl Property for &[ReceivedUserProperty] {
    fn serialize(
        &self,
        identifier: crate::PropertyIdentifier,
        writer: &mut impl Write,
    ) -> Result<usize, std::io::Error> {
        let identifier = identifier as u8;
        if !self.is_empty() {
            let mut len = 0;
            for property in *self {
                writer.write_all(&[identifier])?;
                len += 1
                    + crate::util::write_str(property.key.as_str(), writer)?
                    + crate::util::write_str(property.value.as_str(), writer)?;
            }
            Ok(len)
        } else {
            Ok(0)
        }
    }
    fn property_len(&self) -> usize {
        let mut len = 0;
        for property in *self {
            len += 1 + 2 + property.key.len() + 2 + property.value.len();
        }
        len
    }
}
impl Property for Option<&str> {
    fn serialize(
        &self,
        identifier: crate::PropertyIdentifier,
        writer: &mut impl Write,
    ) -> Result<usize, std::io::Error> {
        let identifier = identifier as u8;
        if let Some(value) = self {
            writer.write_all(&[identifier])?;
            let len = 1 + crate::util::write_str(value, writer)?;
            Ok(len)
        } else {
            Ok(0)
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

impl Property for &[u8] {
    fn serialize(
        &self,
        identifier: crate::PropertyIdentifier,
        writer: &mut impl Write,
    ) -> Result<usize, std::io::Error> {
        assert!(self.len() <= 0xffff);
        let identifier = identifier as u8;
        if !self.is_empty() {
            let len = (self.len() as u16).to_be_bytes();
            writer.write_all(&[identifier, len[0], len[1]])?;
            writer.write_all(self)?;
            Ok(1 + 2 + self.len())
        } else {
            Ok(0)
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

impl Property for Option<Vec<u8>> {
    fn serialize(
        &self,
        identifier: crate::PropertyIdentifier,
        writer: &mut impl Write,
    ) -> Result<usize, std::io::Error> {
        if let Some(value) = self {
            let value: &[u8] = value;
            value.serialize(identifier, writer)
        } else {
            Ok(0)
        }
    }
    fn property_len(&self) -> usize {
        if let Some(value) = self {
            let value: &[u8] = value;
            value.property_len()
        } else {
            0
        }
    }
}

impl Property for Option<PayloadFormat> {
    fn serialize(
        &self,
        identifier: crate::PropertyIdentifier,
        writer: &mut impl Write,
    ) -> Result<usize, std::io::Error> {
        let property = self.as_ref().map(|v| *v as u8);
        property.serialize(identifier, writer)
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
    fn serialize(
        &self,
        identifier: crate::PropertyIdentifier,
        writer: &mut impl Write,
    ) -> Result<usize, std::io::Error> {
        let property = self.as_ref().map(|v| *v as u8);
        property.serialize(identifier, writer)
    }
    fn property_len(&self) -> usize {
        if self.is_some() {
            2
        } else {
            0
        }
    }
}

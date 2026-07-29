use std::any::Any;

use rust_mqtt_protocol::{ConnectRcV3, ConnectRcV5, ControlPacketType};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("Unexpected packet: {0}")]
    UnexpectedPacket(&'static str),
    // #[error("Got error when parsing packet: {0}")]
    // ProtocolError(#[from] PacketError),
    #[error("IO-error")]
    IoError(#[from] std::io::Error),
    // #[error("Invalid fixed header received: {0}")]
    // FixedHeaderError(#[from] FixedHeaderError),
    #[error("Did not receive response from server")]
    Timeout,
    // #[error("Internal channel error with subacks")]
    // SendSubackError(#[from] std::sync::mpsc::SendError<SubAck<V>>),
    // #[error("Internal channel error with unsubacks")]
    // SendUnsubackError(#[from] std::sync::mpsc::SendError<UnsubAck<V>>),
    // #[error("Internal channel error with channels")]
    // InternalChannelError(#[from] std::sync::mpsc::RecvError),
    // #[error("Internal channel error with subacks")]
    // SendInflightError(#[from] std::sync::mpsc::SendError<(u16, Arc<InflightMessage>)>),
    // #[error("Internal channel error with subacks")]
    // SendReceivedPublishError(#[from] std::sync::mpsc::SendError<ReceivedMessage>),
    #[error("Mqtt error")]
    MqttError(#[from] rust_mqtt_protocol::Error),

    #[error("Backend error {0}")]
    BackendError(BackendError),

    #[error("Backend error {0:?}")]
    BackendCrashed(Box<dyn Any + Send + 'static>),
}

#[derive(Debug, Error)]
pub enum ConnectError {
    #[error("Unexpected packet. Expected={expected:?} Received={received:?}")]
    UnexpectedPacket {
        expected: ControlPacketType,
        received: ControlPacketType,
    },
    // #[error("Got error when parsing packet: {0}")]
    // ProtocolError(#[from] PacketError),
    #[error("IO-error")]
    IoError(#[from] std::io::Error),
    #[error("Mqtt broker returned non zero return code {0:?}")]
    ConnectFailedV3(ConnectRcV3),
    #[error("Mqtt broker returned non zero return code {0:?}")]
    ConnectFailedV5(ConnectRcV5),
    #[error("Could not write to stream")]
    WriteError,

    #[error("Mqtt error")]
    MqttError(#[from] rust_mqtt_protocol::Error),
}

#[derive(Debug, Error)]
pub enum BackendError {
    #[error("Internal channel error")]
    ChannelError,
    #[error("IO-error")]
    IoError(#[from] std::io::Error),
    #[error("Mqtt error")]
    MqttError(#[from] rust_mqtt_protocol::Error),

    #[error("Unexpected packet {0}")]
    UnexpectedPacket(&'static str),
}

impl<V> From<std::sync::mpsc::SendError<V>> for BackendError {
    fn from(_: std::sync::mpsc::SendError<V>) -> Self {
        BackendError::ChannelError
    }
}

#[cfg(feature = "async")]
impl<V> From<tokio::sync::mpsc::error::SendError<V>> for BackendError {
    fn from(_: tokio::sync::mpsc::error::SendError<V>) -> Self {
        BackendError::ChannelError
    }
}

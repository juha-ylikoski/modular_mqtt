use std::sync::Arc;

use rust_mqtt_protocol::{
    ConnectRc, FixedHeaderError, PacketError, ReceivedMessage, SubAck, UnsubscribeAck,
};
use thiserror::Error;

use crate::util::InflightMessage;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("Unexpected packet: {0}")]
    UnexpectedPacket(&'static str),
    #[error("Got error when parsing packet: {0}")]
    ProtocolError(#[from] PacketError),
    #[error("IO-error")]
    IoError(#[from] std::io::Error),
    #[error("Invalid fixed header received: {0}")]
    FixedHeaderError(#[from] FixedHeaderError),
    #[error("Did not receive response from server")]
    Timeout,
    #[error("Internal channel error with subacks")]
    SendSubackError(#[from] std::sync::mpsc::SendError<SubAck>),
    #[error("Internal channel error with unsubacks")]
    SendUnsubackError(#[from] std::sync::mpsc::SendError<UnsubscribeAck>),
    #[error("Internal channel error with channels")]
    InternalChannelError(#[from] std::sync::mpsc::RecvError),
    #[error("Internal channel error with subacks")]
    SendInflightError(#[from] std::sync::mpsc::SendError<(u16, Arc<InflightMessage>)>),
    #[error("Internal channel error with subacks")]
    SendReceivedPublishError(#[from] std::sync::mpsc::SendError<ReceivedMessage>),
}

#[derive(Debug, Error)]
pub enum ConnectError {
    #[error("Unexpected packet: {0}")]
    UnexpectedPacket(&'static str),
    #[error("Got error when parsing packet: {0}")]
    ProtocolError(#[from] PacketError),
    #[error("IO-error")]
    IoError(#[from] std::io::Error),
    #[error("Mqtt broker returned non zero return code {0:?}")]
    ConnectFailed(ConnectRc),
    #[error("Could not write to stream")]
    WriteError,
}

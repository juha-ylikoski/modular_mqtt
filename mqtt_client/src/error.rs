use rust_mqtt_protocol::{ConnectRc, FixedHeaderError, PacketError};
use thiserror::Error;

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
}

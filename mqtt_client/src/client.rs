use rust_mqtt_protocol::{
    ConnAck, Connect, ConnectRc, FixedHeader, FixedHeaderError, PacketError, PingReq, PingResp,
};
use std::{
    io::{BufReader, Read, Write},
    net::TcpStream,
    time::Duration,
};

use crate::{
    error::{ClientError, ConnectError},
    util::buf_with_size,
};

enum ReadFinished {
    Success(FixedHeader, Vec<u8>),
    TimedOut,
}

pub struct SyncClient<W: Write, R: Read> {
    pub reader: R,
    pub writer: W,
}

//impl<W: Write + std::marker::Send + 'static, R: Read + std::marker::Send + 'static>
impl SyncClient<TcpStream, BufReader<TcpStream>> {
    pub fn new(config: u64, read_stream: TcpStream, write_stream: TcpStream) -> Self {
        read_stream
            .set_read_timeout(Some(Duration::from_secs(config)))
            .unwrap();
        Self {
            reader: BufReader::new(read_stream),
            writer: write_stream,
        }
    }
    pub fn connect(
        mut self,
    ) -> Result<std::thread::JoinHandle<Result<(), ClientError>>, ConnectError> {
        let msg = Connect::new_v3(false, 15, "mqtt-client-id", None, None, None);
        tracing::trace!("Sending Connect: {msg:?}");
        msg.write_to_stream(&mut self.writer).unwrap();

        let connack = self.handle_connack()?;

        tracing::debug!("Got ConnAck: {connack:?}");

        if connack.connect_rc == ConnectRc::Accepted {
            Ok(std::thread::spawn(|| self.bg_thread()))
        } else {
            Err(ConnectError::ConnectFailed(connack.connect_rc))
        }
    }

    fn read_next_msg(&mut self) -> Result<(FixedHeader, Vec<u8>), FixedHeaderError> {
        let header = FixedHeader::try_read(&mut self.reader)?;
        let mut buf = buf_with_size(header.remaining_length);
        tracing::trace!(
            "Read fixed header: {header:?}. Read next {} bytes",
            header.remaining_length
        );
        self.reader.read_exact(&mut buf)?;
        Ok((header, buf))
    }

    fn handle_connack(&mut self) -> Result<ConnAck, ConnectError> {
        let header = FixedHeader::try_read(&mut self.reader)
            .map_err(|e| ConnectError::ProtocolError(PacketError::from(e)))?;

        tracing::trace!("Got fixed header: {header:?}");
        let header = match &header.control_packet_type {
            rust_mqtt_protocol::ControlPacketType::ConnAck => header,
            _ => {
                tracing::error!("Unexpected packet when expected ConnAck. Got {header:?}");
                return Err(ConnectError::UnexpectedPacket(
                    "Expected ConnAck but did not get it",
                ));
            }
        };
        let mut buf = buf_with_size(header.remaining_length);
        println!("buf size: {}", buf.len());
        self.reader.read_exact(&mut buf)?;
        match ConnAck::try_read(header, &buf) {
            Ok(connack) => Ok(connack),
            Err(e) => {
                tracing::trace!("Invalid payload for ConnAck: {buf:?}. Got error: {e}");
                Err(e)?
            }
        }
    }

    fn read_next_timeout(&mut self) -> Result<ReadFinished, FixedHeaderError> {
        match self.read_next_msg() {
            Ok((header, buf)) => Ok(ReadFinished::Success(header, buf)),
            Err(e) => {
                tracing::trace!("fail read next: {e:?}");
                match &e {
                    FixedHeaderError::IoError(io_error) => match io_error.kind() {
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut => {
                            Ok(ReadFinished::TimedOut)
                        }
                        _ => Err(e),
                    },
                    _ => Err(e),
                }
            }
        }
    }

    fn bg_thread(mut self) -> Result<(), ClientError> {
        tracing::debug!("Start listening for mqtt messages");
        loop {
            match self.read_next_timeout()? {
                ReadFinished::Success(fixed_header, vec) => {
                    match &fixed_header.control_packet_type {
                        rust_mqtt_protocol::ControlPacketType::PingResp => {
                            let resp = PingResp::try_read(fixed_header)?;
                            tracing::debug!("Received PingResp from server: {resp:?}");
                        }
                        rust_mqtt_protocol::ControlPacketType::ConnAck => todo!(),
                        rust_mqtt_protocol::ControlPacketType::SubAck => todo!(),
                        rust_mqtt_protocol::ControlPacketType::UnsubscribeAck => todo!(),
                        rust_mqtt_protocol::ControlPacketType::PubAck => todo!(),
                        // Should never recv
                        rust_mqtt_protocol::ControlPacketType::Connect => todo!(),
                        rust_mqtt_protocol::ControlPacketType::Publish { dup, qos, retain } => {
                            todo!()
                        }
                        rust_mqtt_protocol::ControlPacketType::PubRec => todo!(),
                        rust_mqtt_protocol::ControlPacketType::PubRel => todo!(),
                        rust_mqtt_protocol::ControlPacketType::PubComp => todo!(),
                        rust_mqtt_protocol::ControlPacketType::Subscribe => todo!(),
                        rust_mqtt_protocol::ControlPacketType::Unsubscribe => todo!(),
                        rust_mqtt_protocol::ControlPacketType::PingReq => todo!(),
                        rust_mqtt_protocol::ControlPacketType::Disconnect => todo!(),
                    };
                }
                ReadFinished::TimedOut => {
                    tracing::debug!("Sending ping request to broker");
                    PingReq::write_to_stream(&mut self.writer)?;
                }
            }
        }
        Ok(())
    }
}

//pub struct AsyncClient<W: AsyncWrite, R: AsyncRead> {
//    pub reader: R,
//    pub writer: W,
//}
//
//impl<W: AsyncWrite, R: AsyncRead> AsyncClient<W, R> {
//    pub async fn start_mqtt(&mut self) {
//        let msg = Connect::new_v3(false, 30, "mqtt-client-id", None, None, None);
//        msg.write_to_stream(&mut self.writer).unwrap();
//        self.writer.flush().unwrap();
//    }
//}

#[cfg(test)]
mod test {}

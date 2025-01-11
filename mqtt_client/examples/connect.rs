use std::{
    io::{BufReader, BufWriter, Read},
    net::TcpStream,
};

use mqtt_client::{client::SyncClient, error::ClientError, util::buf_with_size};
use rust_mqtt_protocol::{ConnAck, Connect, FixedHeader, PacketError};
use tracing::{dispatcher::set_global_default, Level};

#[tokio::main()]
async fn main() {
    let collector = tracing_subscriber::fmt::fmt()
        .with_max_level(Level::TRACE)
        .finish();
    set_global_default(collector.into()).unwrap();
    let mut stream = TcpStream::connect("127.0.0.1:1883").unwrap();

    //Connect::new_v3(false, 5, "124", None, None, None)
    //    .write_to_stream(&mut stream)
    //    .unwrap();
    //
    //let header = FixedHeader::try_read(&mut stream).unwrap();
    //
    //let header = match &header.control_packet_type {
    //    rust_mqtt_protocol::ControlPacketType::ConnAck => header,
    //    _ => {
    //        tracing::error!("Unexpected packet when expected ConnAck. Got {header:?}");
    //        todo!();
    //    }
    //};
    //let mut buf = buf_with_size(header.remaining_length);
    //println!("buf size: {}", buf.len());
    //stream.read_exact(&mut buf).unwrap();
    //let connack = ConnAck::try_read(header, &buf).unwrap();
    //
    //println!("buf: {buf:?}");
    //println!("connack: {connack:?}");

    SyncClient::new(10, stream.try_clone().unwrap(), stream) //SyncClient {
        //    reader: stream.try_clone().unwrap(),
        //    writer: stream,
        //}
        .connect()
        .expect("Connection failed")
        .join()
        .expect("Thread panicked")
        .expect("Got when communicating with the mqtt broker");
}

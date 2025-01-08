use std::{
    io::{BufReader, BufWriter},
    net::TcpStream,
};

use mqtt_client::client::{, SyncClient};

#[tokio::main()]
async fn main() {
    let stream = TcpStream::connect("127.0.0.1:1883").unwrap();
    let reader = BufReader::new(&stream);
    let mut client = SyncClient {
        reader,
        writer: stream,
    };
    client.start_mqtt();

    //let (reader, writer) = tokio::net::TcpStream::connect("127.0.0.1:1883")
    //    .await
    //    .unwrap()
    //    .into_split();
    //let mut client = AsyncClient { reader, writer };
    //client.start_mqtt();
}

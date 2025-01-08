use std::io::{BufReader, Read, Write};

use rust_mqtt::mqtt_protocol::connect::{Connect, MqttVersion3_1_1};
use tokio::io::{AsyncRead, AsyncWrite};

pub struct SyncClient<W: Write, R: Read> {
    pub reader: R,
    pub writer: W,
}

impl<W: Write, R: Read> SyncClient<W, R> {
    pub fn start_mqtt(&mut self) {
        let msg = Connect::new_v3(false, 30, "mqtt-client-id", None, None, None);
        msg.write_to_stream(&mut self.writer).unwrap();
        self.writer.flush().unwrap();
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

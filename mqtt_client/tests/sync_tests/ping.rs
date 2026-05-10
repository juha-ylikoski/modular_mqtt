use crate::util;

use std::{net::TcpListener, time::Duration};

use mqtt_client::{
    client::SyncClient,
    client_opts::{ClientOpts, OnDisconnectBehavior},
};
use ntest::timeout;

#[test]
#[timeout(10000)]
fn ping_sequence() {
    util::init_logging();
    let server = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = server.local_addr().unwrap();
    let (tx_close, rx_close) = std::sync::mpsc::channel();

    let handle = std::thread::spawn(move || {
        let (mut stream, _addr) = server.accept().unwrap();
        let (header, data) = util::read_packet(&mut stream);
        let connect = rust_mqtt_protocol::Connect::try_read(header, &data).unwrap();
        assert_eq!(
            connect,
            rust_mqtt_protocol::Connect::new_v3(true, 2, "client-id", None, None, None)
        );
        rust_mqtt_protocol::ConnAck::new(false, rust_mqtt_protocol::ConnectRc::Accepted)
            .write_to_stream(&mut stream)
            .unwrap();

        std::thread::sleep(Duration::from_secs(4));

        let (header, data) = util::read_packet(&mut stream);
        assert_eq!(data.len(), 0);
        let connect = rust_mqtt_protocol::PingReq::try_read(header).unwrap();
        assert_eq!(connect, rust_mqtt_protocol::PingReq::default());

        rust_mqtt_protocol::PingResp::write_to_stream(&mut stream).unwrap();
        rx_close.recv().unwrap();
    });

    let client = SyncClient::connect_tcp(
        ClientOpts {
            client_id: "client-id".to_string(),
            keep_alive: 2,
            clean_session: true,
            will: None,
            username: None,
            password: None,
            on_disconnect: OnDisconnectBehavior::Panic,
        },
        addr.to_string(),
    )
    .unwrap();

    std::thread::sleep(Duration::from_millis(4100));
    client.disconnect().unwrap();
    tx_close.send(()).unwrap();
    handle.join().unwrap();
}

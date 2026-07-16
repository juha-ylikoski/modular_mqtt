use crate::util::{self, write_packet};

use std::{net::TcpListener, time::Duration};

use mqtt_client::{client::SyncClient, client_opts::ClientOpts};
use ntest::timeout;
use rust_mqtt_protocol::{
    ConnAck, Connect, ConnectRcV3, MqttV3_1_1, Packet, PingReq, PingResp, VersionedConnect,
};

#[test]
#[timeout(15000)]
fn ping_sequence() {
    util::init_logging();
    let server = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = server.local_addr().unwrap();
    let (tx_close, rx_close) = std::sync::mpsc::channel();

    let handle = std::thread::spawn(move || {
        let (mut stream, _addr) = server.accept().unwrap();
        let (header, mut data) = util::read_packet(&mut stream);
        let connect = VersionedConnect::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(
            connect,
            VersionedConnect::V3(Connect::new_v3(
                true,
                2,
                "client-id".to_string(),
                None,
                None,
                None
            ))
        );
        write_packet(&mut stream, |buf| {
            ConnAck::new_v3(false, ConnectRcV3::Accepted).write_to_buf(buf)
        });

        std::thread::sleep(Duration::from_secs(4));

        let (header, mut data) = util::read_packet(&mut stream);
        assert_eq!(data.len(), 0);
        let connect = PingReq::try_read(header, &mut data).unwrap();
        assert_eq!(connect, PingReq::default());

        write_packet(&mut stream, |buf| PingResp::default().write_to_buf(buf));
        rx_close.recv().unwrap();
    });

    let client = SyncClient::<MqttV3_1_1>::connect_tcp(
        ClientOpts {
            client_id: "client-id".to_string(),
            keep_alive: 2,
            ..Default::default()
        },
        addr.to_string(),
    )
    .unwrap();

    std::thread::sleep(Duration::from_millis(4100));
    client.disconnect().unwrap();
    tx_close.send(()).unwrap();
    handle.join().unwrap();
}

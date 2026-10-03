use crate::util::{self, write_packet, GenericClientOpts, Harness};

use std::{hash::Hash, net::TcpListener, time::Duration};

use modular_mqtt::{ClientOpts, ClientOptsV3, ClientOptsV5, SyncClient};
use modular_mqtt_protocol::{
    ConnAck, Connect, ConnectRcV3, ConnectRcV5, MqttV3_1_1, MqttV5_0_0, MqttVersion, Packet,
    PingReq, PingResp, VersionedConnect,
};
use ntest::timeout;

async fn test_ping_sequence<H, V, O>(connack_rc: V::ConnackRc)
where
    H: Harness,
    V: MqttVersion,
    O: ClientOpts<V> + From<GenericClientOpts>,
    VersionedConnect: From<Connect<V>>,
{
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
            Connect::<V>::new(true, 2, "client-id".to_string(), None, None, None).into()
        );
        write_packet(&mut stream, |buf| {
            ConnAck::<V>::new(false, connack_rc).write_to_buf(buf)
        });

        let (header, mut data) = util::read_packet(&mut stream);
        assert_eq!(data.len(), 0);
        let connect = PingReq::try_read(header, &mut data).unwrap();
        assert_eq!(connect, PingReq);

        write_packet(&mut stream, |buf| PingResp.write_to_buf(buf));
        rx_close.recv().unwrap();
    });

    let (_, client) = H::connect::<V, O>(
        GenericClientOpts(ClientOptsV5 {
            client_id: "client-id".to_string(),
            keep_alive: 2,
            ..Default::default()
        })
        .into(),
        addr.to_string(),
    )
    .await
    .unwrap();

    H::sleep(Duration::from_millis(4100)).await;
    H::disconnect(client).await.unwrap();
    tx_close.send(()).unwrap();
    handle.join().unwrap();
}

test!(
    ping_sequence,
    test_ping_sequence,
    15000,
    (ConnectRcV3::Accepted),
    (ConnectRcV5::Accepted)
);

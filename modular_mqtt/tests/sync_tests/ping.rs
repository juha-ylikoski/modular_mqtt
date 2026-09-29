use crate::util::{self, write_packet, CommonOperations, GenericClientOpts};

use std::{net::TcpListener, time::Duration};

use modular_mqtt::{ClientOpts, ClientOptsV3, ClientOptsV5, SyncClient};
use modular_mqtt_protocol::{
    ConnAck, Connect, ConnectRcV3, ConnectRcV5, MqttV3_1_1, MqttV5_0_0, MqttVersion, Packet,
    PingReq, PingResp, VersionedConnect,
};
use ntest::timeout;

fn ping_sequence<V, O>(connack_rc: V::ConnackRc)
where
    V: MqttVersion,
    O: ClientOpts<V> + From<GenericClientOpts>,
    SyncClient<V, O>: CommonOperations<V>,
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

        std::thread::sleep(Duration::from_secs(4));

        let (header, mut data) = util::read_packet(&mut stream);
        assert_eq!(data.len(), 0);
        let connect = PingReq::try_read(header, &mut data).unwrap();
        assert_eq!(connect, PingReq);

        write_packet(&mut stream, |buf| PingResp.write_to_buf(buf));
        rx_close.recv().unwrap();
    });

    let (_, client) = SyncClient::<V, O>::connect_tcp(
        GenericClientOpts(ClientOptsV5 {
            client_id: "client-id".to_string(),
            keep_alive: 2,
            ..Default::default()
        })
        .into(),
        addr.to_string(),
    )
    .unwrap();

    std::thread::sleep(Duration::from_millis(4100));
    client.disconnect().unwrap();
    tx_close.send(()).unwrap();
    handle.join().unwrap();
}

#[test]
#[timeout(15000)]
fn v3() {
    ping_sequence::<MqttV3_1_1, ClientOptsV3>(ConnectRcV3::Accepted);
}

#[test]
#[timeout(15000)]
fn v5() {
    ping_sequence::<MqttV5_0_0, ClientOptsV5>(ConnectRcV5::Accepted);
}

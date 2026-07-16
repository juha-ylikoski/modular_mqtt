use crate::util::{self, write_packet};

use std::{net::TcpListener, time::Duration};

use bytes::Bytes;
use mqtt_client::{
    client::{MqttClient, SyncClient},
    client_opts::ClientOpts,
    error::ConnectError,
};
use rust_mqtt_protocol::{
    ConnAck, Connect, ConnectRcV3, ConnectRcV5, Disconnect, DisconnectReasonCode, MqttLastWill,
    MqttTopic, MqttVersion, Packet, Publish, Qos, VersionedConnect,
};

fn test_connect_no_server<V>()
where
    V: MqttVersion,
    SyncClient<V>: MqttClient<V>,
    ClientOpts<V>: Default,
{
    util::init_logging();
    let opts: ClientOpts<V> = ClientOpts {
        client_id: "client-id".to_string(),
        keep_alive: 1,
        ..Default::default()
    };
    match SyncClient::connect_tcp(opts, "127.0.0.1:1234".to_string()) {
        Ok(_) => panic!("Should not happen"),
        Err(e) => match e {
            mqtt_client::error::ConnectError::IoError(_) => (),
            _ => panic!("Should not happen"),
        },
    }
}
test!(connect_no_server, test_connect_no_server, 5000, (), ());

fn test_connect<V>(connact_rc: V::ConnackRc)
where
    V: MqttVersion,
    SyncClient<V>: MqttClient<V>,
    ClientOpts<V>: Default,
    VersionedConnect: From<rust_mqtt_protocol::Connect<V>>,
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
            Connect::<V>::new(true, 1, "client-id".to_string(), None, None, None).into()
        );
        write_packet(&mut stream, |buf| {
            ConnAck::<V>::new(false, connact_rc).write_to_buf(buf)
        });
        rx_close.recv().unwrap();
    });

    let client = SyncClient::<V>::connect_tcp(
        ClientOpts {
            client_id: "client-id".to_string(),
            keep_alive: 1,
            ..Default::default()
        },
        addr.to_string(),
    )
    .unwrap();
    client.disconnect().unwrap();
    tx_close.send(()).unwrap();
    handle.join().unwrap();
}
test!(
    connect,
    test_connect,
    5000,
    (ConnectRcV3::Accepted),
    (ConnectRcV5::Accepted)
);

fn test_connect_username_password<V>(connact_rc: V::ConnackRc)
where
    V: MqttVersion,
    SyncClient<V>: MqttClient<V>,
    ClientOpts<V>: Default,
    VersionedConnect: From<rust_mqtt_protocol::Connect<V>>,
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
            Connect::new(
                true,
                1,
                "".to_string(),
                None,
                Some("username".to_string()),
                Some(Bytes::from_static(b"password"))
            )
            .into()
        );
        write_packet(&mut stream, |buf| {
            ConnAck::<V>::new(false, connact_rc).write_to_buf(buf)
        });
        rx_close.recv().unwrap();
    });

    let client = SyncClient::<V>::connect_tcp(
        ClientOpts {
            client_id: "".to_string(),
            keep_alive: 1,
            username: Some("username".to_string()),
            password: Some(Bytes::from_static(b"password")),
            ..Default::default()
        },
        addr.to_string(),
    )
    .unwrap();
    client.disconnect().unwrap();
    tx_close.send(()).unwrap();

    handle.join().unwrap();
}
test!(
    connect_username_password,
    test_connect_username_password,
    5000,
    (ConnectRcV3::Accepted),
    (ConnectRcV5::Accepted)
);

fn test_connect_last_will<V>(connact_rc: V::ConnackRc)
where
    V: MqttVersion,
    SyncClient<V>: MqttClient<V>,
    ClientOpts<V>: Default,
    VersionedConnect: From<rust_mqtt_protocol::Connect<V>>,
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
            Connect::new(
                true,
                1,
                "".to_string(),
                Some(V::LastWill::new(
                    MqttTopic::try_from("last-will-topic").unwrap(),
                    Bytes::from_static(b"payload"),
                    Qos::AtMostOnce,
                    false,
                )),
                None,
                None,
            )
            .into()
        );
        write_packet(&mut stream, |buf| {
            ConnAck::<V>::new(false, connact_rc).write_to_buf(buf)
        });
        rx_close.recv().unwrap();
    });

    let client = SyncClient::connect_tcp(
        ClientOpts {
            client_id: "".to_string(),
            keep_alive: 1,
            will: Some(V::LastWill::new(
                MqttTopic::try_from("last-will-topic").unwrap(),
                Bytes::from_static(b"payload"),
                Qos::AtMostOnce,
                false,
            )),
            ..Default::default()
        },
        addr.to_string(),
    )
    .unwrap();
    client.disconnect().unwrap();
    tx_close.send(()).unwrap();
    handle.join().unwrap();
}
test!(
    connect_last_will,
    test_connect_last_will,
    5000,
    (ConnectRcV3::Accepted),
    (ConnectRcV5::Accepted)
);

fn test_connect_refused<V>(connact_rc: V::ConnackRc, tester: impl FnOnce(ConnectError))
where
    V: MqttVersion,
    SyncClient<V>: MqttClient<V>,
    ClientOpts<V>: Default,
    VersionedConnect: From<rust_mqtt_protocol::Connect<V>>,
{
    util::init_logging();
    let server = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = server.local_addr().unwrap();

    let handle = std::thread::spawn(move || {
        let (mut stream, _addr) = server.accept().unwrap();
        let (header, mut data) = util::read_packet(&mut stream);
        let connect = VersionedConnect::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(
            connect,
            Connect::new(true, 1, "".to_string(), None, None, None,).into()
        );
        write_packet(&mut stream, |buf| {
            ConnAck::<V>::new(false, connact_rc).write_to_buf(buf)
        });
    });

    match SyncClient::<V>::connect_tcp(
        ClientOpts {
            client_id: "".to_string(),
            keep_alive: 1,
            ..Default::default()
        },
        addr.to_string(),
    ) {
        Ok(_) => panic!("Should not get here"),
        Err(e) => tester(e),
    }
    handle.join().unwrap();
}
test!(
    connect_refused,
    test_connect_refused,
    5000,
    (ConnectRcV3::Refused, |e| match e {
        mqtt_client::error::ConnectError::ConnectFailedV3(connect_rc) => {
            assert_eq!(connect_rc, ConnectRcV3::Refused)
        }
        _ => panic!("Should not get here"),
    }),
    (ConnectRcV5::NotAuthorized, |e| match e {
        mqtt_client::error::ConnectError::ConnectFailedV5(connect_rc) => {
            assert_eq!(connect_rc, ConnectRcV5::NotAuthorized)
        }
        _ => unreachable!(),
    })
);

fn test_disconnect<V>(connact_rc: V::ConnackRc, disconnect: Disconnect<V>)
where
    V: MqttVersion,
    SyncClient<V>: MqttClient<V>,
    ClientOpts<V>: Default,
    VersionedConnect: From<rust_mqtt_protocol::Connect<V>>,
{
    util::init_logging();
    let server = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = server.local_addr().unwrap();

    let handle = std::thread::spawn(move || {
        let (mut stream, _addr) = server.accept().unwrap();
        let (header, mut data) = util::read_packet(&mut stream);
        let connect = VersionedConnect::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(
            connect,
            Connect::new(true, 1, "client-id".to_string(), None, None, None).into()
        );
        write_packet(&mut stream, |buf| {
            ConnAck::<V>::new(false, connact_rc).write_to_buf(buf);
            disconnect.write_to_buf(buf);
        });
    });

    let client = SyncClient::connect_tcp(
        ClientOpts::<V> {
            client_id: "client-id".to_string(),
            keep_alive: 1,
            ..Default::default()
        },
        addr.to_string(),
    )
    .unwrap();
    handle.join().unwrap();
    std::thread::sleep(Duration::from_secs(1));
    assert!(!client.online());
    client
        .publish(Publish::new(
            "foo".try_into().unwrap(),
            b"bar",
            Qos::AtMostOnce,
            false,
        ))
        .unwrap();
}

mod disconnect {
    use super::*;
    #[test]
    #[ntest::timeout(5000)]
    #[should_panic]
    fn v3() {
        test_disconnect::<rust_mqtt_protocol::MqttV3_1_1>(
            ConnectRcV3::Accepted,
            Disconnect::new_v3(),
        )
    }
    #[test]
    #[ntest::timeout(5000)]
    #[should_panic]
    fn v5() {
        test_disconnect::<rust_mqtt_protocol::MqttV5_0_0>(
            ConnectRcV5::Accepted,
            Disconnect::new_v5(DisconnectReasonCode::Normal, None, None, Vec::new(), None),
        )
    }
}

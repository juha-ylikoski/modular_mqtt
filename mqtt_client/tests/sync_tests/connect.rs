use crate::util::{self, write_packet};

use std::{net::TcpListener, time::Duration};

use bytes::Bytes;
use mqtt_client::{client::SyncClient, client_opts::ClientOpts};
use ntest::timeout;
use rust_mqtt_protocol::{
    ConnAck, Connect, ConnectRcV3, Disconnect, MqttLastWill, MqttLastWill3_1_1, MqttTopic,
    MqttV3_1_1, Packet, Publish, Qos, VersionedConnect,
};

#[test]
#[timeout(5000)]
fn connect_no_server() {
    util::init_logging();
    match SyncClient::<MqttV3_1_1>::connect_tcp(
        ClientOpts {
            client_id: "client-id".to_string(),
            keep_alive: 1,
            ..Default::default()
        },
        "127.0.0.1:1234".to_string(),
    ) {
        Ok(_) => panic!("Should not happen"),
        Err(e) => match e {
            mqtt_client::error::ConnectError::IoError(_) => (),
            _ => panic!("Should not happen"),
        },
    }
}

#[test]
#[timeout(5000)]
fn connect() {
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
                1,
                "client-id".to_string(),
                None,
                None,
                None
            ))
        );
        write_packet(&mut stream, |buf| {
            ConnAck::new_v3(false, ConnectRcV3::Accepted).write_to_buf(buf)
        });
        rx_close.recv().unwrap();
    });

    let client = SyncClient::<MqttV3_1_1>::connect_tcp(
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

#[test]
#[timeout(5000)]
fn connect_username_password() {
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
                1,
                "".to_string(),
                None,
                Some("username".to_string()),
                Some(Bytes::from_static(b"password"))
            ))
        );
        write_packet(&mut stream, |buf| {
            ConnAck::new_v3(false, rust_mqtt_protocol::ConnectRcV3::Accepted).write_to_buf(buf)
        });
        rx_close.recv().unwrap();
    });

    let client = SyncClient::<MqttV3_1_1>::connect_tcp(
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

#[test]
#[timeout(5000)]
fn connect_last_will() {
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
                1,
                "".to_string(),
                Some(MqttLastWill3_1_1::new(
                    MqttTopic::try_from("last-will-topic").unwrap(),
                    Bytes::from_static(b"payload"),
                    Qos::AtMostOnce,
                    false,
                )),
                None,
                None,
            ))
        );
        write_packet(&mut stream, |buf| {
            ConnAck::new_v3(false, rust_mqtt_protocol::ConnectRcV3::Accepted).write_to_buf(buf)
        });
        rx_close.recv().unwrap();
    });

    let client = SyncClient::connect_tcp(
        ClientOpts {
            client_id: "".to_string(),
            keep_alive: 1,
            will: Some(
                MqttLastWill3_1_1::new(
                    MqttTopic::try_from("last-will-topic").unwrap(),
                    Bytes::from_static(b"payload"),
                    Qos::AtMostOnce,
                    false,
                )
                .into(),
            ),
            ..Default::default()
        },
        addr.to_string(),
    )
    .unwrap();
    client.disconnect().unwrap();
    tx_close.send(()).unwrap();
    handle.join().unwrap();
}

#[test]
#[timeout(5000)]
fn connect_refused() {
    util::init_logging();
    let server = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = server.local_addr().unwrap();

    let handle = std::thread::spawn(move || {
        let (mut stream, _addr) = server.accept().unwrap();
        let (header, mut data) = util::read_packet(&mut stream);
        let connect = VersionedConnect::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(
            connect,
            VersionedConnect::V3(Connect::new_v3(true, 1, "".to_string(), None, None, None,))
        );
        write_packet(&mut stream, |buf| {
            ConnAck::new_v3(false, rust_mqtt_protocol::ConnectRcV3::Refused).write_to_buf(buf)
        });
    });

    match SyncClient::<MqttV3_1_1>::connect_tcp(
        ClientOpts {
            client_id: "".to_string(),
            keep_alive: 1,
            ..Default::default()
        },
        addr.to_string(),
    ) {
        Ok(_) => panic!("Should not get here"),
        Err(e) => match e {
            mqtt_client::error::ConnectError::ConnectFailedV3(connect_rc) => {
                assert_eq!(connect_rc, ConnectRcV3::Refused)
            }
            _ => panic!("Should not get here"),
        },
    }
    handle.join().unwrap();
}

#[test]
#[timeout(5000)]
#[should_panic]
fn disconnect() {
    util::init_logging();
    let server = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = server.local_addr().unwrap();

    let handle = std::thread::spawn(move || {
        let (mut stream, _addr) = server.accept().unwrap();
        let (header, mut data) = util::read_packet(&mut stream);
        let connect = VersionedConnect::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(
            connect,
            VersionedConnect::V3(Connect::new_v3(
                true,
                1,
                "client-id".to_string(),
                None,
                None,
                None
            ))
        );
        write_packet(&mut stream, |buf| {
            ConnAck::new_v3(false, rust_mqtt_protocol::ConnectRcV3::Accepted).write_to_buf(buf);
            Disconnect::new_v3().write_to_buf(buf);
        });
    });

    let client = SyncClient::connect_tcp(
        ClientOpts {
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
        .publish(Publish::new_v3(
            "foo".try_into().unwrap(),
            b"bar",
            Qos::AtMostOnce,
            false,
        ))
        .unwrap();
}

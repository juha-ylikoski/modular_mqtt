mod util;

use std::{net::TcpListener, time::Duration};

use mqtt_client::client::MqttClient;
use mqtt_client::util::Message;
use mqtt_client::{
    client::SyncClient,
    client_opts::{ClientOpts, OnDisconnectBehavior},
};
use ntest::timeout;
use rust_mqtt_protocol::{MqttLastWill, Qos};

#[test]
#[timeout(5000)]
fn connect_no_server() {
    util::init_logging();
    match SyncClient::connect(
        ClientOpts {
            client_id: "client-id".to_string(),
            keep_alive: 1,
            clean_session: true,
            will: None,
            username: None,
            password: None,
            on_disconnect: OnDisconnectBehavior::Panic,
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

    let handle = std::thread::spawn(move || {
        let (mut stream, _addr) = server.accept().unwrap();
        let (header, data) = util::read_packet(&mut stream);
        let connect = rust_mqtt_protocol::Connect::try_read(header, &data).unwrap();
        assert_eq!(
            connect,
            rust_mqtt_protocol::Connect::new_v3(true, 1, "client-id", None, None, None)
        );
        rust_mqtt_protocol::ConnAck::new(false, rust_mqtt_protocol::ConnectRc::Accepted)
            .write_to_stream(&mut stream)
            .unwrap();
    });

    SyncClient::connect(
        ClientOpts {
            client_id: "client-id".to_string(),
            keep_alive: 1,
            clean_session: true,
            will: None,
            username: None,
            password: None,
            on_disconnect: OnDisconnectBehavior::Panic,
        },
        addr.to_string(),
    )
    .unwrap();
    handle.join().unwrap();
}

#[test]
#[timeout(5000)]
fn connect_username_password() {
    util::init_logging();
    let server = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = server.local_addr().unwrap();

    let handle = std::thread::spawn(move || {
        let (mut stream, _addr) = server.accept().unwrap();
        let (header, data) = util::read_packet(&mut stream);
        let connect = rust_mqtt_protocol::Connect::try_read(header, &data).unwrap();
        assert_eq!(
            connect,
            rust_mqtt_protocol::Connect::new_v3(
                true,
                1,
                "",
                None,
                Some("username"),
                Some(b"password")
            )
        );
        rust_mqtt_protocol::ConnAck::new(false, rust_mqtt_protocol::ConnectRc::Accepted)
            .write_to_stream(&mut stream)
            .unwrap();
    });

    SyncClient::connect(
        ClientOpts {
            client_id: "".to_string(),
            keep_alive: 1,
            clean_session: true,
            will: None,
            username: Some("username".to_string()),
            password: Some(b"password".to_vec()),

            on_disconnect: OnDisconnectBehavior::Panic,
        },
        addr.to_string(),
    )
    .unwrap();
    handle.join().unwrap();
}

#[test]
#[timeout(5000)]
fn connect_last_will() {
    util::init_logging();
    let server = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = server.local_addr().unwrap();

    let handle = std::thread::spawn(move || {
        let (mut stream, _addr) = server.accept().unwrap();
        let (header, data) = util::read_packet(&mut stream);
        let connect = rust_mqtt_protocol::Connect::try_read(header, &data).unwrap();
        assert_eq!(
            connect,
            rust_mqtt_protocol::Connect::new_v3(
                true,
                1,
                "",
                Some(MqttLastWill::new(
                    "last-will-topic",
                    b"payload",
                    false,
                    rust_mqtt_protocol::Qos::AtMostOnce
                )),
                None,
                None,
            )
        );
        rust_mqtt_protocol::ConnAck::new(false, rust_mqtt_protocol::ConnectRc::Accepted)
            .write_to_stream(&mut stream)
            .unwrap();
    });

    SyncClient::connect(
        ClientOpts {
            client_id: "".to_string(),
            keep_alive: 1,
            clean_session: true,
            will: Some(mqtt_client::client_opts::LastWill {
                topic: "last-will-topic".to_string(),
                payload: b"payload".into(),
                retain: false,
                qos: rust_mqtt_protocol::Qos::AtMostOnce,
            }),
            username: None,
            password: None,
            on_disconnect: OnDisconnectBehavior::Panic,
        },
        addr.to_string(),
    )
    .unwrap();
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
        let (header, data) = util::read_packet(&mut stream);
        let connect = rust_mqtt_protocol::Connect::try_read(header, &data).unwrap();
        assert_eq!(
            connect,
            rust_mqtt_protocol::Connect::new_v3(true, 1, "", None, None, None,)
        );
        rust_mqtt_protocol::ConnAck::new(false, rust_mqtt_protocol::ConnectRc::Refused)
            .write_to_stream(&mut stream)
            .unwrap();
    });

    match SyncClient::connect(
        ClientOpts {
            client_id: "".to_string(),
            keep_alive: 1,
            clean_session: true,
            will: None,
            username: None,
            password: None,
            on_disconnect: OnDisconnectBehavior::Panic,
        },
        addr.to_string(),
    ) {
        Ok(_) => panic!("Should not get here"),
        Err(e) => match e {
            mqtt_client::error::ConnectError::ConnectFailed(connect_rc) => {
                assert_eq!(connect_rc, rust_mqtt_protocol::ConnectRc::Refused)
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
        let (header, data) = util::read_packet(&mut stream);
        let connect = rust_mqtt_protocol::Connect::try_read(header, &data).unwrap();
        assert_eq!(
            connect,
            rust_mqtt_protocol::Connect::new_v3(true, 1, "client-id", None, None, None)
        );
        rust_mqtt_protocol::ConnAck::new(false, rust_mqtt_protocol::ConnectRc::Accepted)
            .write_to_stream(&mut stream)
            .unwrap();
        rust_mqtt_protocol::Disconnect::write_to_stream(&mut stream).unwrap();
    });

    let mut client = SyncClient::connect(
        ClientOpts {
            client_id: "client-id".to_string(),
            keep_alive: 1,
            clean_session: true,
            will: None,
            username: None,
            password: None,
            on_disconnect: OnDisconnectBehavior::Panic,
        },
        addr.to_string(),
    )
    .unwrap();
    handle.join().unwrap();
    std::thread::sleep(Duration::from_secs(1));
    assert_eq!(client.online(), false);
    client
        .publish(Message::new(
            "foo".try_into().unwrap(),
            b"bar",
            Qos::AtMostOnce,
        ))
        .unwrap();
}

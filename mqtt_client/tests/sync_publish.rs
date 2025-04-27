mod util;

use std::{net::TcpListener, time::Duration};

use mqtt_client::{
    client::{MqttClient, SyncClient},
    client_opts::ClientOpts,
    util::Message,
};
use rust_mqtt_protocol::{ControlPacketType, MqttTopic};

#[test]
fn publish_qos0() {
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

        let (header, data) = util::read_packet(&mut stream);
        let msg = rust_mqtt_protocol::ReceivedMessage::try_read(header, data).unwrap();
        assert_eq!(
            msg,
            rust_mqtt_protocol::ReceivedMessage {
                flags: 0,
                topic: "topic".to_string(),
                packet_identifier: None,
                payload: b"payload".to_vec()
            }
        );
    });

    let mut client = SyncClient::connect(
        ClientOpts {
            client_id: "client-id".to_string(),
            keep_alive: 1,
            clean_session: true,
            will: None,
            username: None,
            password: None,
        },
        addr.to_string(),
    )
    .unwrap();
    assert!(client
        .publish(Message::new(
            MqttTopic::try_from("topic").unwrap(),
            b"payload",
            rust_mqtt_protocol::Qos::AtMostOnce,
        ))
        .unwrap()
        .is_none());
    handle.join().unwrap();
}

#[test]
fn publish_qos1() {
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

        let (header, data) = util::read_packet(&mut stream);
        let msg = rust_mqtt_protocol::ReceivedMessage::try_read(header, data).unwrap();
        assert!(msg.packet_identifier.is_some());
        assert_eq!(
            msg,
            rust_mqtt_protocol::ReceivedMessage {
                flags: ControlPacketType::Publish {
                    dup: false,
                    qos: rust_mqtt_protocol::Qos::AtLeastOnce,
                    retain: false
                }
                .flags(),
                topic: "topic".to_string(),
                packet_identifier: msg.packet_identifier,
                payload: b"payload".to_vec()
            }
        );
        rust_mqtt_protocol::PubAck::new(msg.packet_identifier.unwrap())
            .write_to_stream(&mut stream)
            .unwrap();
    });

    let mut client = SyncClient::connect(
        ClientOpts {
            client_id: "client-id".to_string(),
            keep_alive: 1,
            clean_session: true,
            will: None,
            username: None,
            password: None,
        },
        addr.to_string(),
    )
    .unwrap();
    let msg = client
        .publish(Message::new(
            MqttTopic::try_from("topic").unwrap(),
            b"payload",
            rust_mqtt_protocol::Qos::AtLeastOnce,
        ))
        .unwrap()
        .unwrap();
    handle.join().unwrap();
    msg.wait_until_delivered();
}

#[test]
fn publish_qos2() {
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

        let (header, data) = util::read_packet(&mut stream);
        let msg = rust_mqtt_protocol::ReceivedMessage::try_read(header, data).unwrap();
        assert!(msg.packet_identifier.is_some());
        assert_eq!(
            msg,
            rust_mqtt_protocol::ReceivedMessage {
                flags: ControlPacketType::Publish {
                    dup: false,
                    qos: rust_mqtt_protocol::Qos::ExactlyOnce,
                    retain: false
                }
                .flags(),
                topic: "topic".to_string(),
                packet_identifier: msg.packet_identifier,
                payload: b"payload".to_vec()
            }
        );
        rust_mqtt_protocol::PubRec::new(msg.packet_identifier.unwrap())
            .write_to_stream(&mut stream)
            .unwrap();

        let (header, data) = util::read_packet(&mut stream);
        let pub_rel = rust_mqtt_protocol::PubRel::try_read(header, &data);
        assert_eq!(msg.packet_identifier.unwrap(), pub_rel.packet_identifier);
        assert_eq!(
            pub_rel,
            rust_mqtt_protocol::PubRel::new(msg.packet_identifier.unwrap())
        );
        rust_mqtt_protocol::PubComp::new(msg.packet_identifier.unwrap())
            .write_to_stream(&mut stream)
            .unwrap();
    });

    let mut client = SyncClient::connect(
        ClientOpts {
            client_id: "client-id".to_string(),
            keep_alive: 1,
            clean_session: true,
            will: None,
            username: None,
            password: None,
        },
        addr.to_string(),
    )
    .unwrap();
    let msg = client
        .publish(Message::new(
            MqttTopic::try_from("topic").unwrap(),
            b"payload",
            rust_mqtt_protocol::Qos::ExactlyOnce,
        ))
        .unwrap()
        .unwrap();
    handle.join().unwrap();
    msg.wait_until_delivered();
}

#[test]
fn publish_resend_qos1() {
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

        let (header, data) = util::read_packet(&mut stream);
        let msg = rust_mqtt_protocol::ReceivedMessage::try_read(header, data).unwrap();
        assert!(msg.packet_identifier.is_some());
        assert_eq!(
            msg,
            rust_mqtt_protocol::ReceivedMessage {
                flags: ControlPacketType::Publish {
                    dup: false,
                    qos: rust_mqtt_protocol::Qos::AtLeastOnce,
                    retain: false
                }
                .flags(),
                topic: "topic".to_string(),
                packet_identifier: msg.packet_identifier,
                payload: b"payload".to_vec()
            }
        );
        let (header, data) = loop {
            let (header, data) = util::read_packet(&mut stream);
            match &header.control_packet_type {
                ControlPacketType::PingReq => {
                    assert_eq!(data.len(), 0);
                    let connect = rust_mqtt_protocol::PingReq::try_read(header).unwrap();
                    assert_eq!(connect, rust_mqtt_protocol::PingReq::default());

                    rust_mqtt_protocol::PingResp::write_to_stream(&mut stream).unwrap();
                }
                ControlPacketType::Publish { .. } => {
                    break (header, data);
                }
                _ => panic!("Should not get here"),
            }
        };
        let msg = rust_mqtt_protocol::ReceivedMessage::try_read(header, data).unwrap();
        assert_eq!(
            msg,
            rust_mqtt_protocol::ReceivedMessage {
                flags: ControlPacketType::Publish {
                    dup: true,
                    qos: rust_mqtt_protocol::Qos::AtLeastOnce,
                    retain: false
                }
                .flags(),
                topic: "topic".to_string(),
                packet_identifier: msg.packet_identifier,
                payload: b"payload".to_vec()
            }
        );
        rust_mqtt_protocol::PubAck::new(msg.packet_identifier.unwrap())
            .write_to_stream(&mut stream)
            .unwrap();
        std::thread::sleep(Duration::from_millis(100));
    });

    let mut client = SyncClient::connect(
        ClientOpts {
            client_id: "client-id".to_string(),
            keep_alive: 1,
            clean_session: true,
            will: None,
            username: None,
            password: None,
        },
        addr.to_string(),
    )
    .unwrap();
    let msg = client
        .publish(Message::new(
            MqttTopic::try_from("topic").unwrap(),
            b"payload",
            rust_mqtt_protocol::Qos::AtLeastOnce,
        ))
        .unwrap()
        .unwrap();
    handle.join().unwrap();
    msg.wait_until_delivered();
}

use crate::util;

use std::net::TcpListener;

use mqtt_client::{
    client::SyncClient,
    client_opts::{ClientOpts, OnDisconnectBehavior},
    util::Message,
};
use ntest::timeout;
use rust_mqtt_protocol::{ControlPacketType, FixedHeader, MqttTopic};

#[test]
#[timeout(5000)]
fn publish_qos0() {
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
            rust_mqtt_protocol::Connect::new_v3(true, 1, "client-id", None, None, None)
        );
        rust_mqtt_protocol::ConnAck::new(false, rust_mqtt_protocol::ConnectRc::Accepted)
            .write_to_stream(&mut stream)
            .unwrap();

        let (header, data) = util::read_packet(&mut stream);
        let recv_msg = rust_mqtt_protocol::ReceivedMessage::try_read(header, data).unwrap();
        assert_eq!(
            recv_msg,
            rust_mqtt_protocol::ReceivedMessage {
                flags: 0,
                topic: "topic".to_string(),
                packet_identifier: None,
                payload: b"payload".to_vec()
            }
        );
        rx_close.recv().unwrap();
    });

    let client = SyncClient::connect_tcp(
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
    assert!(client
        .publish(Message::new(
            MqttTopic::try_from("topic").unwrap(),
            b"payload",
            rust_mqtt_protocol::Qos::AtMostOnce,
        ))
        .unwrap()
        .is_none());
    client.disconnect().unwrap();
    tx_close.send(()).unwrap();
    handle.join().unwrap();
}

#[test]
#[timeout(5000)]
fn publish_qos1() {
    util::init_logging();
    let server = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = server.local_addr().unwrap();
    let (send, recv) = std::sync::mpsc::channel();
    let (tx_close, rx_close) = std::sync::mpsc::channel();

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
        let recv_msg = rust_mqtt_protocol::ReceivedMessage::try_read(header, data).unwrap();
        assert!(recv_msg.packet_identifier.is_some());

        let expected_packet_identifier = recv.recv().unwrap();

        assert_eq!(
            recv_msg,
            rust_mqtt_protocol::ReceivedMessage {
                flags: ControlPacketType::Publish {
                    dup: false,
                    qos: rust_mqtt_protocol::Qos::AtLeastOnce,
                    retain: false
                }
                .flags(),
                topic: "topic".to_string(),
                packet_identifier: Some(expected_packet_identifier),
                payload: b"payload".to_vec()
            }
        );
        rust_mqtt_protocol::PubAck::new(recv_msg.packet_identifier.unwrap())
            .write_to_stream(&mut stream)
            .unwrap();
        rx_close.recv().unwrap();
    });

    let client = SyncClient::connect_tcp(
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
    let msg = client
        .publish(Message::new(
            MqttTopic::try_from("topic").unwrap(),
            b"payload",
            rust_mqtt_protocol::Qos::AtLeastOnce,
        ))
        .unwrap()
        .unwrap();
    send.send(msg.packet_identifier()).unwrap();
    msg.wait_until_delivered();

    client.disconnect().unwrap();
    tx_close.send(()).unwrap();
    handle.join().unwrap();
}

#[test]
#[timeout(5000)]
fn publish_qos2() {
    util::init_logging();
    let server = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = server.local_addr().unwrap();
    let (send, recv) = std::sync::mpsc::channel();
    let (tx_close, rx_close) = std::sync::mpsc::channel();

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
        let recv_pub = rust_mqtt_protocol::ReceivedMessage::try_read(header, data).unwrap();
        assert!(recv_pub.packet_identifier.is_some());
        let expected_packet_identifier = recv.recv().unwrap();
        assert_eq!(
            recv_pub,
            rust_mqtt_protocol::ReceivedMessage {
                flags: ControlPacketType::Publish {
                    dup: false,
                    qos: rust_mqtt_protocol::Qos::ExactlyOnce,
                    retain: false
                }
                .flags(),
                topic: "topic".to_string(),
                packet_identifier: Some(expected_packet_identifier),
                payload: b"payload".to_vec()
            }
        );
        rust_mqtt_protocol::PubRec::new(recv_pub.packet_identifier.unwrap())
            .write_to_stream(&mut stream)
            .unwrap();

        let (header, data) = util::read_packet(&mut stream);
        let pub_rel = rust_mqtt_protocol::PubRel::try_read(header, &data).unwrap();
        assert_eq!(
            pub_rel,
            rust_mqtt_protocol::PubRel::new(expected_packet_identifier)
        );
        rust_mqtt_protocol::PubComp::new(recv_pub.packet_identifier.unwrap())
            .write_to_stream(&mut stream)
            .unwrap();
        rx_close.recv().unwrap();
    });

    let client = SyncClient::connect_tcp(
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
    let msg = client
        .publish(Message::new(
            MqttTopic::try_from("topic").unwrap(),
            b"payload",
            rust_mqtt_protocol::Qos::ExactlyOnce,
        ))
        .unwrap()
        .unwrap();
    send.send(msg.packet_identifier()).unwrap();
    msg.wait_until_delivered();

    client.disconnect().unwrap();
    tx_close.send(()).unwrap();
    handle.join().unwrap();
}

#[test]
#[timeout(15000)]
fn publish_resend_qos1() {
    util::init_logging();

    let server = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = server.local_addr().unwrap();
    let (send, recv) = std::sync::mpsc::channel();
    let (tx_close, rx_close) = std::sync::mpsc::channel();

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
        let recv_pub = rust_mqtt_protocol::ReceivedMessage::try_read(header, data).unwrap();
        assert!(recv_pub.packet_identifier.is_some());
        let expected_packet_identifier = recv.recv().unwrap();
        assert_eq!(
            recv_pub,
            rust_mqtt_protocol::ReceivedMessage {
                flags: ControlPacketType::Publish {
                    dup: false,
                    qos: rust_mqtt_protocol::Qos::AtLeastOnce,
                    retain: false
                }
                .flags(),
                topic: "topic".to_string(),
                packet_identifier: Some(expected_packet_identifier),
                payload: b"payload".to_vec()
            }
        );
        let (header, data) = loop {
            let (header, data) = util::read_packet(&mut stream);
            match &header.control_packet_type {
                ControlPacketType::PingReq => {
                    assert_eq!(data.len(), 0);
                    let ping_req = rust_mqtt_protocol::PingReq::try_read(header).unwrap();
                    assert_eq!(ping_req, rust_mqtt_protocol::PingReq::default());

                    rust_mqtt_protocol::PingResp::write_to_stream(&mut stream).unwrap();
                }
                ControlPacketType::Publish { .. } => {
                    break (header, data);
                }
                _ => panic!("Should not get here"),
            }
        };
        let recv_pub2 = rust_mqtt_protocol::ReceivedMessage::try_read(header, data).unwrap();
        assert_eq!(
            recv_pub2,
            rust_mqtt_protocol::ReceivedMessage {
                flags: ControlPacketType::Publish {
                    dup: true,
                    qos: rust_mqtt_protocol::Qos::AtLeastOnce,
                    retain: false
                }
                .flags(),
                topic: "topic".to_string(),
                packet_identifier: recv_pub2.packet_identifier,
                payload: b"payload".to_vec()
            }
        );
        rust_mqtt_protocol::PubAck::new(recv_pub2.packet_identifier.unwrap())
            .write_to_stream(&mut stream)
            .unwrap();
        rx_close.recv().unwrap();
    });

    let client = SyncClient::connect_tcp(
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
    let msg = client
        .publish(Message::new(
            MqttTopic::try_from("topic").unwrap(),
            b"payload",
            rust_mqtt_protocol::Qos::AtLeastOnce,
        ))
        .unwrap()
        .unwrap();
    send.send(msg.packet_identifier()).unwrap();
    msg.wait_until_delivered();

    client.disconnect().unwrap();
    tx_close.send(()).unwrap();
    handle.join().unwrap();
}

#[test]
#[timeout(15000)]
fn publish_resend_pub_qos2() {
    util::init_logging();
    let server = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = server.local_addr().unwrap();
    let (send, recv) = std::sync::mpsc::channel();
    let (tx_close, rx_close) = std::sync::mpsc::channel();

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
        let recv_pub = rust_mqtt_protocol::ReceivedMessage::try_read(header, data).unwrap();
        assert!(recv_pub.packet_identifier.is_some());
        let expected_packet_identifier = recv.recv().unwrap();
        assert_eq!(
            recv_pub,
            rust_mqtt_protocol::ReceivedMessage {
                flags: ControlPacketType::Publish {
                    dup: false,
                    qos: rust_mqtt_protocol::Qos::ExactlyOnce,
                    retain: false
                }
                .flags(),
                topic: "topic".to_string(),
                packet_identifier: recv_pub.packet_identifier,
                payload: b"payload".to_vec()
            }
        );
        tracing::info!("HERE");
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
        tracing::info!("HERE 2");
        let recv_pub2 = rust_mqtt_protocol::ReceivedMessage::try_read(header, data).unwrap();
        assert_eq!(
            recv_pub2,
            rust_mqtt_protocol::ReceivedMessage {
                flags: ControlPacketType::Publish {
                    dup: true,
                    qos: rust_mqtt_protocol::Qos::ExactlyOnce,
                    retain: false
                }
                .flags(),
                topic: "topic".to_string(),
                packet_identifier: Some(expected_packet_identifier),
                payload: b"payload".to_vec()
            }
        );

        rust_mqtt_protocol::PubRec::new(recv_pub2.packet_identifier.unwrap())
            .write_to_stream(&mut stream)
            .unwrap();

        let (header, data) = util::read_packet(&mut stream);
        let pub_rel = rust_mqtt_protocol::PubRel::try_read(header, &data).unwrap();
        assert_eq!(
            pub_rel,
            rust_mqtt_protocol::PubRel::new(expected_packet_identifier)
        );
        rust_mqtt_protocol::PubComp::new(recv_pub2.packet_identifier.unwrap())
            .write_to_stream(&mut stream)
            .unwrap();
        rx_close.recv().unwrap();
    });

    let client = SyncClient::connect_tcp(
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
    let msg = client
        .publish(Message::new(
            MqttTopic::try_from("topic").unwrap(),
            b"payload",
            rust_mqtt_protocol::Qos::ExactlyOnce,
        ))
        .unwrap()
        .unwrap();
    send.send(msg.packet_identifier()).unwrap();
    msg.wait_until_delivered();

    client.disconnect().unwrap();
    tx_close.send(()).unwrap();
    handle.join().unwrap();
}

#[test]
#[timeout(15000)]
fn publish_resend_pubrel_qos2() {
    util::init_logging();
    let server = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = server.local_addr().unwrap();
    let (send, recv) = std::sync::mpsc::channel();
    let (tx_close, rx_close) = std::sync::mpsc::channel();

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
        let recv_msg = rust_mqtt_protocol::ReceivedMessage::try_read(header, data).unwrap();
        assert!(recv_msg.packet_identifier.is_some());
        let expected_packet_identifier = recv.recv().unwrap();
        assert_eq!(
            recv_msg,
            rust_mqtt_protocol::ReceivedMessage {
                flags: ControlPacketType::Publish {
                    dup: false,
                    qos: rust_mqtt_protocol::Qos::ExactlyOnce,
                    retain: false
                }
                .flags(),
                topic: "topic".to_string(),
                packet_identifier: Some(expected_packet_identifier),
                payload: b"payload".to_vec()
            }
        );

        rust_mqtt_protocol::PubRec::new(recv_msg.packet_identifier.unwrap())
            .write_to_stream(&mut stream)
            .unwrap();

        let (header, data) = util::read_packet(&mut stream);
        let pub_rel = rust_mqtt_protocol::PubRel::try_read(header, &data).unwrap();
        assert_eq!(
            recv_msg.packet_identifier.unwrap(),
            pub_rel.packet_identifier
        );
        assert_eq!(
            pub_rel,
            rust_mqtt_protocol::PubRel::new(recv_msg.packet_identifier.unwrap())
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
                ControlPacketType::PubRel { .. } => {
                    break (header, data);
                }
                _ => panic!("Should not get here"),
            }
        };
        let pub_rel2 = rust_mqtt_protocol::PubRel::try_read(header, &data).unwrap();
        assert_eq!(
            pub_rel2,
            rust_mqtt_protocol::PubRel {
                fixed_header: FixedHeader::new(ControlPacketType::PubRel, 2),
                packet_identifier: pub_rel2.packet_identifier,
            }
        );
        rust_mqtt_protocol::PubComp::new(expected_packet_identifier)
            .write_to_stream(&mut stream)
            .unwrap();
        rx_close.recv().unwrap();
    });

    let client = SyncClient::connect_tcp(
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
    let msg = client
        .publish(Message::new(
            MqttTopic::try_from("topic").unwrap(),
            b"payload",
            rust_mqtt_protocol::Qos::ExactlyOnce,
        ))
        .unwrap()
        .unwrap();
    send.send(msg.packet_identifier()).unwrap();
    msg.wait_until_delivered();

    client.disconnect().unwrap();
    tx_close.send(()).unwrap();
    handle.join().unwrap();
}

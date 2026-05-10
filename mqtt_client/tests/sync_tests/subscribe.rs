use crate::util;

use std::net::TcpListener;

use mqtt_client::{
    client::SyncClient,
    client_opts::{ClientOpts, OnDisconnectBehavior},
};
use ntest::timeout;
use rust_mqtt_protocol::{
    MqttTopic, PubAck, PubComp, PubRec, PubRel, Qos, QosPacketIdentifier, ReceivedMessage, SubAck,
    SubRc, TopicSubscription,
};

#[test]
#[timeout(5000)]
fn sub_qos0() {
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
        let sub = rust_mqtt_protocol::Subscribe::try_read(header, &data).unwrap();
        assert_eq!(
            sub,
            rust_mqtt_protocol::Subscribe::new(
                sub.packet_identifier(),
                vec![TopicSubscription::new(
                    "topic".try_into().unwrap(),
                    Qos::AtMostOnce
                )]
            )
        );
        send.send(sub.packet_identifier()).unwrap();
        rust_mqtt_protocol::SubAck::new(sub.packet_identifier(), vec![SubRc::SuccessQos0])
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
    let suback = client
        .subscribe(vec!["topic".try_into().unwrap()], Qos::AtMostOnce)
        .unwrap();
    let packet_identifier = recv.recv().unwrap();
    assert_eq!(
        suback,
        SubAck::new(packet_identifier, vec![SubRc::SuccessQos0])
    );
    client.disconnect().unwrap();
    tx_close.send(()).unwrap();
    handle.join().unwrap();
}

#[test]
#[timeout(5000)]
fn sub_qos1() {
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
        let sub = rust_mqtt_protocol::Subscribe::try_read(header, &data).unwrap();
        assert_eq!(
            sub,
            rust_mqtt_protocol::Subscribe::new(
                sub.packet_identifier(),
                vec![TopicSubscription::new(
                    "topic".try_into().unwrap(),
                    Qos::AtLeastOnce
                )]
            )
        );
        send.send(sub.packet_identifier()).unwrap();
        rust_mqtt_protocol::SubAck::new(sub.packet_identifier(), vec![SubRc::SuccessQos0])
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
    let suback = client
        .subscribe(vec!["topic".try_into().unwrap()], Qos::AtLeastOnce)
        .unwrap();
    let packet_identifier = recv.recv().unwrap();
    assert_eq!(
        suback,
        SubAck::new(packet_identifier, vec![SubRc::SuccessQos0])
    );
    client.disconnect().unwrap();
    tx_close.send(()).unwrap();
    handle.join().unwrap();
}

#[test]
#[timeout(5000)]
fn sub_qos2() {
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
        let sub = rust_mqtt_protocol::Subscribe::try_read(header, &data).unwrap();
        assert_eq!(
            sub,
            rust_mqtt_protocol::Subscribe::new(
                sub.packet_identifier(),
                vec![TopicSubscription::new(
                    "topic".try_into().unwrap(),
                    Qos::ExactlyOnce
                )]
            )
        );
        send.send(sub.packet_identifier()).unwrap();
        rust_mqtt_protocol::SubAck::new(sub.packet_identifier(), vec![SubRc::SuccessQos0])
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
    let suback = client
        .subscribe(vec!["topic".try_into().unwrap()], Qos::ExactlyOnce)
        .unwrap();
    let packet_identifier = recv.recv().unwrap();
    assert_eq!(
        suback,
        SubAck::new(packet_identifier, vec![SubRc::SuccessQos0])
    );
    client.disconnect().unwrap();
    tx_close.send(()).unwrap();
    handle.join().unwrap();
}

#[test]
#[timeout(5000)]
fn sub_qos0_receive_packet() {
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
        let sub = rust_mqtt_protocol::Subscribe::try_read(header, &data).unwrap();
        assert_eq!(
            sub,
            rust_mqtt_protocol::Subscribe::new(
                sub.packet_identifier(),
                vec![TopicSubscription::new(
                    "topic".try_into().unwrap(),
                    Qos::AtMostOnce
                )]
            )
        );
        send.send(sub.packet_identifier()).unwrap();
        rust_mqtt_protocol::SubAck::new(sub.packet_identifier(), vec![SubRc::SuccessQos0])
            .write_to_stream(&mut stream)
            .unwrap();
        let topic = MqttTopic::try_from("topic").unwrap();
        rust_mqtt_protocol::Publish::new(
            false,
            rust_mqtt_protocol::QosPacketIdentifier::AtMostOnce,
            false,
            &topic,
            b"test",
        )
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

    let stream = client.stream();

    let suback = client
        .subscribe(vec!["topic".try_into().unwrap()], Qos::AtMostOnce)
        .unwrap();
    let packet_identifier = recv.recv().unwrap();
    assert_eq!(
        suback,
        SubAck::new(packet_identifier, vec![SubRc::SuccessQos0])
    );
    let msg = stream.recv().unwrap();
    assert_eq!(
        msg,
        ReceivedMessage {
            flags: 0,
            topic: "topic".to_string(),
            packet_identifier: None,
            payload: b"test".to_vec()
        }
    );
    client.disconnect().unwrap();
    tx_close.send(()).unwrap();
    handle.join().unwrap();
}

#[test]
#[timeout(5000)]
fn sub_qos1_receive_packet() {
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
        let sub = rust_mqtt_protocol::Subscribe::try_read(header, &data).unwrap();
        assert_eq!(
            sub,
            rust_mqtt_protocol::Subscribe::new(
                sub.packet_identifier(),
                vec![TopicSubscription::new(
                    "topic".try_into().unwrap(),
                    Qos::AtLeastOnce
                )]
            )
        );
        send.send(sub.packet_identifier()).unwrap();
        rust_mqtt_protocol::SubAck::new(sub.packet_identifier(), vec![SubRc::SuccessQos1])
            .write_to_stream(&mut stream)
            .unwrap();
        let topic = MqttTopic::try_from("topic").unwrap();
        rust_mqtt_protocol::Publish::new(
            false,
            rust_mqtt_protocol::QosPacketIdentifier::AtLeastOnce(42),
            false,
            &topic,
            b"test",
        )
        .write_to_stream(&mut stream)
        .unwrap();
        let (header, data) = util::read_packet(&mut stream);
        let ack = rust_mqtt_protocol::PubAck::try_read(header, &data).unwrap();
        assert_eq!(ack, PubAck::new(42));
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

    let stream = client.stream();

    let suback = client
        .subscribe(vec!["topic".try_into().unwrap()], Qos::AtLeastOnce)
        .unwrap();
    let packet_identifier = recv.recv().unwrap();
    assert_eq!(
        suback,
        SubAck::new(packet_identifier, vec![SubRc::SuccessQos1])
    );
    let msg = stream.recv().unwrap();
    assert_eq!(
        msg,
        ReceivedMessage {
            flags: 2,
            topic: "topic".to_string(),
            packet_identifier: Some(42),
            payload: b"test".to_vec()
        }
    );
    client.disconnect().unwrap();
    tx_close.send(()).unwrap();
    handle.join().unwrap();
}

#[test]
#[timeout(5000)]
fn sub_qos2_receive_packet() {
    util::init_logging();
    let server = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = server.local_addr().unwrap();
    let (send, recv) = std::sync::mpsc::channel();
    let (pub_done_tx, pub_done_rx) = std::sync::mpsc::channel();
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
        let sub = rust_mqtt_protocol::Subscribe::try_read(header, &data).unwrap();
        assert_eq!(
            sub,
            rust_mqtt_protocol::Subscribe::new(
                sub.packet_identifier(),
                vec![TopicSubscription::new(
                    "topic".try_into().unwrap(),
                    Qos::ExactlyOnce
                )]
            )
        );
        send.send(sub.packet_identifier()).unwrap();
        rust_mqtt_protocol::SubAck::new(sub.packet_identifier(), vec![SubRc::SuccessQos2])
            .write_to_stream(&mut stream)
            .unwrap();
        let topic = MqttTopic::try_from("topic").unwrap();
        rust_mqtt_protocol::Publish::new(
            false,
            QosPacketIdentifier::ExactlyOnce(42),
            false,
            &topic,
            b"test",
        )
        .write_to_stream(&mut stream)
        .unwrap();
        let (header, data) = util::read_packet(&mut stream);
        let rec = PubRec::try_read(header, &data).unwrap();
        assert_eq!(rec, PubRec::new(42));

        PubRel::new(42).write_to_stream(&mut stream).unwrap();
        let (header, data) = util::read_packet(&mut stream);
        let rec = PubComp::try_read(header, &data).unwrap();
        assert_eq!(rec, PubComp::new(42));
        pub_done_tx.send(()).unwrap();
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

    let stream = client.stream();

    let suback = client
        .subscribe(vec!["topic".try_into().unwrap()], Qos::ExactlyOnce)
        .unwrap();
    let packet_identifier = recv.recv().unwrap();
    assert_eq!(
        suback,
        SubAck::new(packet_identifier, vec![SubRc::SuccessQos2])
    );
    let msg = stream.recv().unwrap();
    assert_eq!(
        msg,
        ReceivedMessage {
            flags: 4,
            topic: "topic".to_string(),
            packet_identifier: Some(42),
            payload: b"test".to_vec()
        }
    );
    pub_done_rx.recv().unwrap();
    client.disconnect().unwrap();
    tx_close.send(()).unwrap();
    handle.join().unwrap();
}

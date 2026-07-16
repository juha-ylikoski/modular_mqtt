use crate::util::{self, write_packet};

use std::net::TcpListener;

use mqtt_client::{client::SyncClient, client_opts::ClientOpts};
use ntest::timeout;
use rust_mqtt_protocol::{
    ConnAck, Connect, MqttTopic, Packet, PubAck, PubComp, PubRec, PubRel, Publish, Qos, SubAck,
    SubRcV3, Subscribe, TopicSubscriptionV3, VersionedConnect,
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
            ConnAck::new_v3(false, rust_mqtt_protocol::ConnectRcV3::Accepted).write_to_buf(buf)
        });

        let (header, mut data) = util::read_packet(&mut stream);
        let sub = Subscribe::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(
            sub,
            Subscribe::new_v3(
                sub.packet_identifier(),
                vec![TopicSubscriptionV3::new(
                    "topic".try_into().unwrap(),
                    Qos::AtMostOnce
                )]
            )
        );
        send.send(sub.packet_identifier()).unwrap();
        write_packet(&mut stream, |buf| {
            SubAck::new_v3(sub.packet_identifier(), vec![SubRcV3::SuccessQos0]).write_to_buf(buf)
        });
        rx_close.recv().unwrap();
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
    let suback = client
        .subscribe(vec![MqttTopic::try_from("topic").unwrap()], Qos::AtMostOnce)
        .unwrap();
    let packet_identifier = recv.recv().unwrap();
    assert_eq!(
        suback,
        SubAck::new_v3(packet_identifier, vec![SubRcV3::SuccessQos0])
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
            ConnAck::new_v3(false, rust_mqtt_protocol::ConnectRcV3::Accepted).write_to_buf(buf)
        });

        let (header, mut data) = util::read_packet(&mut stream);
        let sub = Subscribe::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(
            sub,
            Subscribe::new_v3(
                sub.packet_identifier(),
                vec![TopicSubscriptionV3::new(
                    "topic".try_into().unwrap(),
                    Qos::AtLeastOnce
                )]
            )
        );
        send.send(sub.packet_identifier()).unwrap();
        write_packet(&mut stream, |buf| {
            SubAck::new_v3(sub.packet_identifier(), vec![SubRcV3::SuccessQos0]).write_to_buf(buf)
        });
        rx_close.recv().unwrap();
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
    let suback = client
        .subscribe(
            vec![MqttTopic::try_from("topic").unwrap()],
            Qos::AtLeastOnce,
        )
        .unwrap();
    let packet_identifier = recv.recv().unwrap();
    assert_eq!(
        suback,
        SubAck::new_v3(packet_identifier, vec![SubRcV3::SuccessQos0])
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
            ConnAck::new_v3(false, rust_mqtt_protocol::ConnectRcV3::Accepted).write_to_buf(buf)
        });

        let (header, mut data) = util::read_packet(&mut stream);
        let sub = Subscribe::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(
            sub,
            Subscribe::new_v3(
                sub.packet_identifier(),
                vec![TopicSubscriptionV3::new(
                    "topic".try_into().unwrap(),
                    Qos::ExactlyOnce
                )]
            )
        );
        send.send(sub.packet_identifier()).unwrap();
        write_packet(&mut stream, |buf| {
            SubAck::new_v3(sub.packet_identifier(), vec![SubRcV3::SuccessQos0]).write_to_buf(buf)
        });
        rx_close.recv().unwrap();
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
    let suback = client
        .subscribe(
            vec![MqttTopic::try_from("topic").unwrap()],
            Qos::ExactlyOnce,
        )
        .unwrap();
    let packet_identifier = recv.recv().unwrap();
    assert_eq!(
        suback,
        SubAck::new_v3(packet_identifier, vec![SubRcV3::SuccessQos0])
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
            ConnAck::new_v3(false, rust_mqtt_protocol::ConnectRcV3::Accepted).write_to_buf(buf)
        });

        let (header, mut data) = util::read_packet(&mut stream);
        let sub = Subscribe::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(
            sub,
            Subscribe::new_v3(
                sub.packet_identifier(),
                vec![TopicSubscriptionV3::new(
                    "topic".try_into().unwrap(),
                    Qos::AtMostOnce
                )]
            )
        );
        send.send(sub.packet_identifier()).unwrap();
        write_packet(&mut stream, |buf| {
            SubAck::new_v3(sub.packet_identifier(), vec![SubRcV3::SuccessQos0]).write_to_buf(buf)
        });
        let topic = MqttTopic::try_from("topic").unwrap();
        write_packet(&mut stream, |buf| {
            Publish::new_v3(topic, b"test", Qos::AtMostOnce, false)
                .assign_packet_identifier(|| 1, false)
                .write_to_buf(buf)
        });
        rx_close.recv().unwrap();
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

    let stream = client.stream();

    let suback = client
        .subscribe(vec![MqttTopic::try_from("topic").unwrap()], Qos::AtMostOnce)
        .unwrap();
    let packet_identifier = recv.recv().unwrap();
    assert_eq!(
        suback,
        SubAck::new_v3(packet_identifier, vec![SubRcV3::SuccessQos0])
    );
    let msg = stream.recv().unwrap();
    assert_eq!(
        msg,
        Publish::new_v3(
            MqttTopic::try_from("topic").unwrap(),
            b"test",
            Qos::AtMostOnce,
            false
        )
        .assign_packet_identifier(|| 1, false)
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
            ConnAck::new_v3(false, rust_mqtt_protocol::ConnectRcV3::Accepted).write_to_buf(buf)
        });

        let (header, mut data) = util::read_packet(&mut stream);
        let sub = Subscribe::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(
            sub,
            Subscribe::new_v3(
                sub.packet_identifier(),
                vec![TopicSubscriptionV3::new(
                    "topic".try_into().unwrap(),
                    Qos::AtLeastOnce
                )]
            )
        );
        send.send(sub.packet_identifier()).unwrap();
        write_packet(&mut stream, |buf| {
            SubAck::new_v3(sub.packet_identifier(), vec![SubRcV3::SuccessQos1]).write_to_buf(buf)
        });
        let topic = MqttTopic::try_from("topic").unwrap();
        write_packet(&mut stream, |buf| {
            Publish::new_v3(topic, b"test", Qos::AtLeastOnce, false)
                .assign_packet_identifier(|| 42, false)
                .write_to_buf(buf)
        });
        let (header, mut data) = util::read_packet(&mut stream);
        let ack = PubAck::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(ack, PubAck::new_v3(42));
        rx_close.recv().unwrap();
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

    let stream = client.stream();

    let suback = client
        .subscribe(
            vec![MqttTopic::try_from("topic").unwrap()],
            Qos::AtLeastOnce,
        )
        .unwrap();
    let packet_identifier = recv.recv().unwrap();
    assert_eq!(
        suback,
        SubAck::new_v3(packet_identifier, vec![SubRcV3::SuccessQos1])
    );
    let msg = stream.recv().unwrap();
    assert_eq!(
        msg,
        Publish::new_v3(
            MqttTopic::try_from("topic").unwrap(),
            b"test",
            Qos::AtLeastOnce,
            false
        )
        .assign_packet_identifier(|| 42, false)
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
            ConnAck::new_v3(false, rust_mqtt_protocol::ConnectRcV3::Accepted).write_to_buf(buf)
        });

        let (header, mut data) = util::read_packet(&mut stream);
        let sub = Subscribe::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(
            sub,
            Subscribe::new_v3(
                sub.packet_identifier(),
                vec![TopicSubscriptionV3::new(
                    "topic".try_into().unwrap(),
                    Qos::ExactlyOnce
                )]
            )
        );
        send.send(sub.packet_identifier()).unwrap();
        write_packet(&mut stream, |buf| {
            SubAck::new_v3(sub.packet_identifier(), vec![SubRcV3::SuccessQos2]).write_to_buf(buf)
        });
        let topic = MqttTopic::try_from("topic").unwrap();
        write_packet(&mut stream, |buf| {
            Publish::new_v3(topic, b"test", Qos::ExactlyOnce, false)
                .assign_packet_identifier(|| 42, false)
                .write_to_buf(buf)
        });
        let (header, mut data) = util::read_packet(&mut stream);
        let rec = PubRec::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(rec, PubRec::new_v3(42));

        write_packet(&mut stream, |buf| PubRel::new_v3(42).write_to_buf(buf));
        let (header, mut data) = util::read_packet(&mut stream);
        let rec = PubComp::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(rec, PubComp::new_v3(42));
        pub_done_tx.send(()).unwrap();
        rx_close.recv().unwrap();
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

    let stream = client.stream();

    let suback = client
        .subscribe(
            vec![MqttTopic::try_from("topic").unwrap()],
            Qos::ExactlyOnce,
        )
        .unwrap();
    let packet_identifier = recv.recv().unwrap();
    assert_eq!(
        suback,
        SubAck::new_v3(packet_identifier, vec![SubRcV3::SuccessQos2])
    );
    let msg = stream.recv().unwrap();
    assert_eq!(
        msg,
        Publish::new_v3(
            MqttTopic::try_from("topic").unwrap(),
            b"test",
            Qos::ExactlyOnce,
            false
        )
        .assign_packet_identifier(|| 42, false)
    );
    pub_done_rx.recv().unwrap();
    client.disconnect().unwrap();
    tx_close.send(()).unwrap();
    handle.join().unwrap();
}

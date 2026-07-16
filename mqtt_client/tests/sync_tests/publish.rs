use crate::util::{self, write_packet};

use std::net::TcpListener;

use mqtt_client::{
    client::{MqttClient, SyncClient},
    client_opts::ClientOpts,
};
use rust_mqtt_protocol::{
    ConnAck, Connect, ConnectRcV3, ConnectRcV5, ControlPacketType, MqttTopic, MqttVersion, Packet,
    PingReq, PingResp, PubAck, PubComp, PubRec, PubRel, Publish, Qos, QosPacketIdentifier,
    VersionedConnect,
};

fn test_publish_qos0<V>(connack_rc: V::ConnackRc)
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
            Connect::new(true, 1, "client-id".to_string(), None, None, None).into()
        );
        write_packet(&mut stream, |buf| {
            ConnAck::<V>::new(false, connack_rc).write_to_buf(buf)
        });

        let (header, mut data) = util::read_packet(&mut stream);
        let recv_msg =
            Publish::<V, QosPacketIdentifier>::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(
            recv_msg,
            Publish::new(
                MqttTopic::try_from("topic").unwrap(),
                b"payload",
                Qos::AtMostOnce,
                false
            )
            .assign_packet_identifier(|| 1, false)
        );
        rx_close.recv().unwrap();
    });

    let client: SyncClient<V> = SyncClient::connect_tcp(
        ClientOpts {
            client_id: "client-id".to_string(),
            keep_alive: 1,
            ..Default::default()
        },
        addr.to_string(),
    )
    .unwrap();
    assert!(client
        .publish(Publish::new(
            MqttTopic::try_from("topic").unwrap(),
            b"payload",
            Qos::AtMostOnce,
            false
        ))
        .unwrap()
        .is_none());
    client.disconnect().unwrap();
    tx_close.send(()).unwrap();
    handle.join().unwrap();
}
test!(
    publish_qos0,
    test_publish_qos0,
    5000,
    (ConnectRcV3::Accepted),
    (ConnectRcV5::Accepted)
);

fn test_publish_qos1<V>(connack_rc: V::ConnackRc)
where
    V: MqttVersion,
    SyncClient<V>: MqttClient<V>,
    ClientOpts<V>: Default,
    VersionedConnect: From<rust_mqtt_protocol::Connect<V>>,
{
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
            Connect::new(true, 1, "client-id".to_string(), None, None, None).into()
        );
        write_packet(&mut stream, |buf| {
            ConnAck::<V>::new(false, connack_rc).write_to_buf(buf)
        });

        let (header, mut data) = util::read_packet(&mut stream);
        let recv_msg =
            Publish::<V, QosPacketIdentifier>::try_read_entire_buf(header, &mut data).unwrap();
        assert!(recv_msg.packet_identifier().is_some());

        let expected_packet_identifier = recv.recv().unwrap();

        assert_eq!(
            recv_msg,
            Publish::new(
                MqttTopic::try_from("topic").unwrap(),
                b"payload",
                Qos::AtLeastOnce,
                false
            )
            .assign_packet_identifier(|| expected_packet_identifier, false)
        );
        write_packet(&mut stream, |buf| {
            PubAck::<V>::new_ok(recv_msg.packet_identifier().unwrap()).write_to_buf(buf)
        });
        rx_close.recv().unwrap();
    });

    let client: SyncClient<V> = SyncClient::connect_tcp(
        ClientOpts {
            client_id: "client-id".to_string(),
            keep_alive: 1,
            ..Default::default()
        },
        addr.to_string(),
    )
    .unwrap();
    let msg = client
        .publish(Publish::new(
            MqttTopic::try_from("topic").unwrap(),
            b"payload",
            Qos::AtLeastOnce,
            false,
        ))
        .unwrap()
        .unwrap();
    send.send(msg.packet_identifier()).unwrap();
    msg.wait_until_delivered();

    client.disconnect().unwrap();
    tx_close.send(()).unwrap();
    handle.join().unwrap();
}
test!(
    publish_qos1,
    test_publish_qos1,
    5000,
    (ConnectRcV3::Accepted),
    (ConnectRcV5::Accepted)
);

fn test_publish_qos2<V>(connack_rc: V::ConnackRc)
where
    V: MqttVersion,
    SyncClient<V>: MqttClient<V>,
    ClientOpts<V>: Default,
    VersionedConnect: From<rust_mqtt_protocol::Connect<V>>,
{
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
            Connect::new(true, 1, "client-id".to_string(), None, None, None).into()
        );
        write_packet(&mut stream, |buf| {
            ConnAck::<V>::new(false, connack_rc).write_to_buf(buf)
        });

        let (header, mut data) = util::read_packet(&mut stream);
        let recv_pub =
            Publish::<V, QosPacketIdentifier>::try_read_entire_buf(header, &mut data).unwrap();
        assert!(recv_pub.packet_identifier().is_some());
        let expected_packet_identifier = recv.recv().unwrap();
        assert_eq!(
            recv_pub,
            Publish::new(
                MqttTopic::try_from("topic").unwrap(),
                b"payload",
                Qos::ExactlyOnce,
                false
            )
            .assign_packet_identifier(|| expected_packet_identifier, false)
        );
        write_packet(&mut stream, |buf| {
            PubRec::<V>::new_ok(recv_pub.packet_identifier().unwrap()).write_to_buf(buf)
        });

        let (header, mut data) = util::read_packet(&mut stream);
        let pub_rel = PubRel::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(pub_rel, PubRel::<V>::new_ok(expected_packet_identifier));
        write_packet(&mut stream, |buf| {
            PubComp::<V>::new_ok(recv_pub.packet_identifier().unwrap()).write_to_buf(buf)
        });
        rx_close.recv().unwrap();
    });

    let client: SyncClient<V> = SyncClient::connect_tcp(
        ClientOpts {
            client_id: "client-id".to_string(),
            keep_alive: 1,
            ..Default::default()
        },
        addr.to_string(),
    )
    .unwrap();
    let msg = client
        .publish(Publish::new(
            MqttTopic::try_from("topic").unwrap(),
            b"payload",
            Qos::ExactlyOnce,
            false,
        ))
        .unwrap()
        .unwrap();
    send.send(msg.packet_identifier()).unwrap();
    msg.wait_until_delivered();

    client.disconnect().unwrap();
    tx_close.send(()).unwrap();
    handle.join().unwrap();
}
test!(
    publish_qos2,
    test_publish_qos2,
    5000,
    (ConnectRcV3::Accepted),
    (ConnectRcV5::Accepted)
);

fn test_publish_resend_qos1<V>(connack_rc: V::ConnackRc)
where
    V: MqttVersion,
    SyncClient<V>: MqttClient<V>,
    ClientOpts<V>: Default,
    VersionedConnect: From<rust_mqtt_protocol::Connect<V>>,
{
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
            Connect::new(true, 1, "client-id".to_string(), None, None, None).into()
        );
        write_packet(&mut stream, |buf| {
            ConnAck::<V>::new(false, connack_rc).write_to_buf(buf)
        });

        let (header, mut data) = util::read_packet(&mut stream);
        let recv_pub =
            Publish::<V, QosPacketIdentifier>::try_read_entire_buf(header, &mut data).unwrap();
        assert!(recv_pub.packet_identifier().is_some());
        let expected_packet_identifier = recv.recv().unwrap();
        assert_eq!(
            recv_pub,
            Publish::new(
                MqttTopic::try_from("topic").unwrap(),
                b"payload",
                Qos::AtLeastOnce,
                false
            )
            .assign_packet_identifier(|| expected_packet_identifier, false)
        );
        let (header, mut data) = loop {
            let (header, mut data) = util::read_packet(&mut stream);
            match &header.control_packet_type {
                ControlPacketType::PingReq => {
                    assert_eq!(data.len(), 0);
                    let ping_req = PingReq::try_read_entire_buf(header, &mut data).unwrap();
                    assert_eq!(ping_req, PingReq);

                    write_packet(&mut stream, |buf| PingResp.write_to_buf(buf));
                }
                ControlPacketType::Publish { .. } => {
                    break (header, data);
                }
                _ => panic!("Should not get here"),
            }
        };
        let recv_pub2 =
            Publish::<V, QosPacketIdentifier>::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(
            recv_pub2,
            Publish::new(
                MqttTopic::try_from("topic").unwrap(),
                b"payload",
                Qos::AtLeastOnce,
                false
            )
            .assign_packet_identifier(|| recv_pub2.packet_identifier().unwrap(), false),
        );

        write_packet(&mut stream, |buf| {
            PubAck::<V>::new_ok(recv_pub2.packet_identifier().unwrap()).write_to_buf(buf)
        });
        rx_close.recv().unwrap();
    });

    let client: SyncClient<V> = SyncClient::connect_tcp(
        ClientOpts {
            client_id: "client-id".to_string(),
            keep_alive: 1,
            ..Default::default()
        },
        addr.to_string(),
    )
    .unwrap();
    let msg = client
        .publish(Publish::new(
            MqttTopic::try_from("topic").unwrap(),
            b"payload",
            Qos::AtLeastOnce,
            false,
        ))
        .unwrap()
        .unwrap();
    send.send(msg.packet_identifier()).unwrap();
    msg.wait_until_delivered();

    client.disconnect().unwrap();
    tx_close.send(()).unwrap();
    handle.join().unwrap();
}
test!(
    publish_resend_qos1,
    test_publish_resend_qos1,
    15000,
    (ConnectRcV3::Accepted),
    (ConnectRcV5::Accepted)
);

fn test_publish_resend_qos2<V>(connack_rc: V::ConnackRc)
where
    V: MqttVersion,
    SyncClient<V>: MqttClient<V>,
    ClientOpts<V>: Default,
    VersionedConnect: From<rust_mqtt_protocol::Connect<V>>,
{
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
            Connect::new(true, 1, "client-id".to_string(), None, None, None).into()
        );
        write_packet(&mut stream, |buf| {
            ConnAck::<V>::new(false, connack_rc).write_to_buf(buf)
        });

        let (header, mut data) = util::read_packet(&mut stream);
        let recv_pub =
            Publish::<V, QosPacketIdentifier>::try_read_entire_buf(header, &mut data).unwrap();
        assert!(recv_pub.packet_identifier().is_some());
        let expected_packet_identifier = recv.recv().unwrap();
        assert_eq!(
            recv_pub,
            Publish::new(
                MqttTopic::try_from("topic").unwrap(),
                b"payload",
                Qos::ExactlyOnce,
                false
            )
            .assign_packet_identifier(|| recv_pub.packet_identifier().unwrap(), false)
        );
        let (header, mut data) = loop {
            let (header, mut data) = util::read_packet(&mut stream);
            match &header.control_packet_type {
                ControlPacketType::PingReq => {
                    assert_eq!(data.len(), 0);
                    let connect = PingReq::try_read_entire_buf(header, &mut data).unwrap();
                    assert_eq!(connect, PingReq);

                    write_packet(&mut stream, |buf| {
                        PingResp.write_to_buf(buf);
                    });
                }
                ControlPacketType::Publish { .. } => {
                    break (header, data);
                }
                _ => panic!("Should not get here"),
            }
        };
        let recv_pub2 =
            Publish::<V, QosPacketIdentifier>::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(
            recv_pub2,
            Publish::new(
                MqttTopic::try_from("topic").unwrap(),
                b"payload",
                Qos::ExactlyOnce,
                false
            )
            .assign_packet_identifier(|| expected_packet_identifier, false)
        );

        write_packet(&mut stream, |buf| {
            PubRec::<V>::new_ok(recv_pub2.packet_identifier().unwrap()).write_to_buf(buf)
        });

        let (header, mut data) = util::read_packet(&mut stream);
        let pub_rel = PubRel::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(pub_rel, PubRel::<V>::new_ok(expected_packet_identifier));
        write_packet(&mut stream, |buf| {
            PubComp::<V>::new_ok(recv_pub2.packet_identifier().unwrap()).write_to_buf(buf)
        });
        rx_close.recv().unwrap();
    });

    let client: SyncClient<V> = SyncClient::connect_tcp(
        ClientOpts {
            client_id: "client-id".to_string(),
            keep_alive: 1,
            ..Default::default()
        },
        addr.to_string(),
    )
    .unwrap();
    let msg = client
        .publish(Publish::new(
            MqttTopic::try_from("topic").unwrap(),
            b"payload",
            Qos::ExactlyOnce,
            false,
        ))
        .unwrap()
        .unwrap();
    send.send(msg.packet_identifier()).unwrap();
    msg.wait_until_delivered();

    client.disconnect().unwrap();
    tx_close.send(()).unwrap();
    handle.join().unwrap();
}
test!(
    publish_resend_qos2,
    test_publish_resend_qos2,
    15000,
    (ConnectRcV3::Accepted),
    (ConnectRcV5::Accepted)
);

fn test_publish_resend_pubrel_qos2<V>(connack_rc: V::ConnackRc)
where
    V: MqttVersion,
    SyncClient<V>: MqttClient<V>,
    ClientOpts<V>: Default,
    VersionedConnect: From<rust_mqtt_protocol::Connect<V>>,
{
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
            Connect::<V>::new(true, 1, "client-id".to_string(), None, None, None).into()
        );
        write_packet(&mut stream, |buf| {
            ConnAck::<V>::new(false, connack_rc).write_to_buf(buf)
        });

        let (header, mut data) = util::read_packet(&mut stream);
        let recv_msg =
            Publish::<V, QosPacketIdentifier>::try_read_entire_buf(header, &mut data).unwrap();
        assert!(recv_msg.packet_identifier().is_some());
        let expected_packet_identifier = recv.recv().unwrap();
        assert_eq!(
            recv_msg,
            Publish::new(
                MqttTopic::try_from("topic").unwrap(),
                b"payload",
                Qos::ExactlyOnce,
                false
            )
            .assign_packet_identifier(|| expected_packet_identifier, false),
        );

        write_packet(&mut stream, |buf| {
            PubRec::<V>::new_ok(recv_msg.packet_identifier().unwrap()).write_to_buf(buf)
        });

        let (header, mut data) = util::read_packet(&mut stream);
        let pub_rel = PubRel::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(
            recv_msg.packet_identifier().unwrap(),
            pub_rel.packet_identifier()
        );
        assert_eq!(
            pub_rel,
            PubRel::<V>::new_ok(recv_msg.packet_identifier().unwrap())
        );

        let (header, mut data) = loop {
            let (header, mut data) = util::read_packet(&mut stream);
            match &header.control_packet_type {
                ControlPacketType::PingReq => {
                    assert_eq!(data.len(), 0);
                    let connect = PingReq::try_read_entire_buf(header, &mut data).unwrap();
                    assert_eq!(connect, PingReq);

                    write_packet(&mut stream, |buf| PingResp.write_to_buf(buf));
                }
                ControlPacketType::PubRel => {
                    break (header, data);
                }
                _ => panic!("Should not get here"),
            }
        };
        let pub_rel2 = PubRel::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(pub_rel2, PubRel::<V>::new_ok(pub_rel2.packet_identifier()));
        write_packet(&mut stream, |buf| {
            PubComp::<V>::new_ok(expected_packet_identifier).write_to_buf(buf)
        });
        rx_close.recv().unwrap();
    });

    let client: SyncClient<V> = SyncClient::connect_tcp(
        ClientOpts {
            client_id: "client-id".to_string(),
            keep_alive: 1,
            ..Default::default()
        },
        addr.to_string(),
    )
    .unwrap();
    let msg = client
        .publish(Publish::new(
            MqttTopic::try_from("topic").unwrap(),
            b"payload",
            Qos::ExactlyOnce,
            false,
        ))
        .unwrap()
        .unwrap();
    send.send(msg.packet_identifier()).unwrap();
    msg.wait_until_delivered();

    client.disconnect().unwrap();
    tx_close.send(()).unwrap();
    handle.join().unwrap();
}
test!(
    publish_resend_pubrel_qos2,
    test_publish_resend_pubrel_qos2,
    15000,
    (ConnectRcV3::Accepted),
    (ConnectRcV5::Accepted)
);

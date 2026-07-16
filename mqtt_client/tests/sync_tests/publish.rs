use crate::util::{self, write_packet};

use std::net::TcpListener;

use mqtt_client::{client::SyncClient, client_opts::ClientOpts};
use ntest::timeout;
use rust_mqtt_protocol::{
    ConnAck, Connect, ConnectRcV3, ControlPacketType, MqttTopic, Packet, PingReq, PingResp, PubAck,
    PubComp, PubRec, PubRel, Publish, Qos, VersionedConnect,
};

#[test]
#[timeout(5000)]
fn publish_qos0() {
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

        let (header, mut data) = util::read_packet(&mut stream);
        let recv_msg = Publish::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(
            recv_msg,
            Publish::new_v3(
                MqttTopic::try_from("topic").unwrap(),
                b"payload",
                Qos::AtMostOnce,
                false
            )
            .assign_packet_identifier(|| 1, false)
        );
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
    assert!(client
        .publish(Publish::new_v3(
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

        let (header, mut data) = util::read_packet(&mut stream);
        let recv_msg = Publish::try_read_entire_buf(header, &mut data).unwrap();
        assert!(recv_msg.packet_identifier().is_some());

        let expected_packet_identifier = recv.recv().unwrap();

        assert_eq!(
            recv_msg,
            Publish::new_v3(
                MqttTopic::try_from("topic").unwrap(),
                b"payload",
                Qos::AtLeastOnce,
                false
            )
            .assign_packet_identifier(|| expected_packet_identifier, false)
        );
        write_packet(&mut stream, |buf| {
            PubAck::new_v3(recv_msg.packet_identifier().unwrap()).write_to_buf(buf)
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
    let msg = client
        .publish(Publish::new_v3(
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

        let (header, mut data) = util::read_packet(&mut stream);
        let recv_pub = Publish::try_read_entire_buf(header, &mut data).unwrap();
        assert!(recv_pub.packet_identifier().is_some());
        let expected_packet_identifier = recv.recv().unwrap();
        assert_eq!(
            recv_pub,
            Publish::new_v3(
                MqttTopic::try_from("topic").unwrap(),
                b"payload",
                Qos::ExactlyOnce,
                false
            )
            .assign_packet_identifier(|| expected_packet_identifier, false)
        );
        write_packet(&mut stream, |buf| {
            PubRec::new_v3(recv_pub.packet_identifier().unwrap()).write_to_buf(buf)
        });

        let (header, mut data) = util::read_packet(&mut stream);
        let pub_rel = PubRel::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(pub_rel, PubRel::new_v3(expected_packet_identifier));
        write_packet(&mut stream, |buf| {
            PubComp::new_v3(recv_pub.packet_identifier().unwrap()).write_to_buf(buf)
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
    let msg = client
        .publish(Publish::new_v3(
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

        let (header, mut data) = util::read_packet(&mut stream);
        let recv_pub = Publish::try_read_entire_buf(header, &mut data).unwrap();
        assert!(recv_pub.packet_identifier().is_some());
        let expected_packet_identifier = recv.recv().unwrap();
        assert_eq!(
            recv_pub,
            Publish::new_v3(
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
                    assert_eq!(ping_req, PingReq::default());

                    write_packet(&mut stream, |buf| PingResp::default().write_to_buf(buf));
                }
                ControlPacketType::Publish { .. } => {
                    break (header, data);
                }
                _ => panic!("Should not get here"),
            }
        };
        let recv_pub2 = Publish::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(
            recv_pub2,
            Publish::new_v3(
                MqttTopic::try_from("topic").unwrap(),
                b"payload",
                Qos::AtLeastOnce,
                false
            )
            .assign_packet_identifier(|| recv_pub2.packet_identifier().unwrap(), false),
        );

        write_packet(&mut stream, |buf| {
            PubAck::new_v3(recv_pub2.packet_identifier().unwrap()).write_to_buf(buf)
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
    let msg = client
        .publish(Publish::new_v3(
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

        let (header, mut data) = util::read_packet(&mut stream);
        let recv_pub = Publish::try_read_entire_buf(header, &mut data).unwrap();
        assert!(recv_pub.packet_identifier().is_some());
        let expected_packet_identifier = recv.recv().unwrap();
        assert_eq!(
            recv_pub,
            Publish::new_v3(
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
                    assert_eq!(connect, PingReq::default());

                    write_packet(&mut stream, |buf| {
                        PingResp::default().write_to_buf(buf);
                    });
                }
                ControlPacketType::Publish { .. } => {
                    break (header, data);
                }
                _ => panic!("Should not get here"),
            }
        };
        let recv_pub2 = Publish::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(
            recv_pub2,
            Publish::new_v3(
                MqttTopic::try_from("topic").unwrap(),
                b"payload",
                Qos::ExactlyOnce,
                false
            )
            .assign_packet_identifier(|| expected_packet_identifier, false)
        );

        write_packet(&mut stream, |buf| {
            PubRec::new_v3(recv_pub2.packet_identifier().unwrap()).write_to_buf(buf)
        });

        let (header, mut data) = util::read_packet(&mut stream);
        let pub_rel = PubRel::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(pub_rel, PubRel::new_v3(expected_packet_identifier));
        write_packet(&mut stream, |buf| {
            PubComp::new_v3(recv_pub2.packet_identifier().unwrap()).write_to_buf(buf)
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
    let msg = client
        .publish(Publish::new_v3(
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

        let (header, mut data) = util::read_packet(&mut stream);
        let recv_msg = Publish::try_read_entire_buf(header, &mut data).unwrap();
        assert!(recv_msg.packet_identifier().is_some());
        let expected_packet_identifier = recv.recv().unwrap();
        assert_eq!(
            recv_msg,
            Publish::new_v3(
                MqttTopic::try_from("topic").unwrap(),
                b"payload",
                Qos::ExactlyOnce,
                false
            )
            .assign_packet_identifier(|| expected_packet_identifier, false),
        );

        write_packet(&mut stream, |buf| {
            PubRec::new_v3(recv_msg.packet_identifier().unwrap()).write_to_buf(buf)
        });

        let (header, mut data) = util::read_packet(&mut stream);
        let pub_rel = PubRel::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(
            recv_msg.packet_identifier().unwrap(),
            pub_rel.packet_identifier()
        );
        assert_eq!(
            pub_rel,
            PubRel::new_v3(recv_msg.packet_identifier().unwrap())
        );

        let (header, mut data) = loop {
            let (header, mut data) = util::read_packet(&mut stream);
            match &header.control_packet_type {
                ControlPacketType::PingReq => {
                    assert_eq!(data.len(), 0);
                    let connect = PingReq::try_read_entire_buf(header, &mut data).unwrap();
                    assert_eq!(connect, PingReq::default());

                    write_packet(&mut stream, |buf| PingResp::default().write_to_buf(buf));
                }
                ControlPacketType::PubRel => {
                    break (header, data);
                }
                _ => panic!("Should not get here"),
            }
        };
        let pub_rel2 = PubRel::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(pub_rel2, PubRel::new_v3(pub_rel2.packet_identifier()));
        write_packet(&mut stream, |buf| {
            PubComp::new_v3(expected_packet_identifier).write_to_buf(buf)
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
    let msg = client
        .publish(Publish::new_v3(
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

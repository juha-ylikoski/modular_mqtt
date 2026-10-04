use crate::util::{self, write_packet, GenericClientOpts, Harness};

use std::io::Read;
use std::net::{TcpListener, TcpStream};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use modular_mqtt::{ClientOpts, ClientOptsV5, SyncClient};
use modular_mqtt_protocol::{
    ConnAck, Connect, ConnectRcV3, ConnectRcV5, ControlPacketType, FixedHeader, MqttTopic,
    MqttVersion, Packet, PingReq, PingResp, PubAck, PubComp, PubRec, PubRel, Publish, Qos,
    QosPacketIdentifier, VersionedConnect, MAX_MQTT_PACKET_SIZE,
};

/// Read one packet from `stream`, giving up after a short read timeout.
///
/// Returns `None` on timeout, or when the peer closed the connection. `util::read_packet`
/// blocks forever, which cannot be used to observe the *absence* of a packet — here, the
/// absence of a retransmission the client should not have sent.
fn read_packet_timeout(stream: &mut TcpStream) -> Option<(FixedHeader, Bytes)> {
    stream
        .set_read_timeout(Some(Duration::from_millis(150)))
        .unwrap();
    let mut buf = BytesMut::zeroed(4096);
    let mut len = 0;
    loop {
        match stream.read(&mut buf[len..]) {
            Ok(0) | Err(_) => return None,
            Ok(n) => len += n,
        }
        buf.truncate(len);
        match FixedHeader::parse(&mut buf, MAX_MQTT_PACKET_SIZE) {
            Ok(Some(packet)) => return Some(packet),
            Ok(None) => buf.resize(len + 4096, 0),
            Err(_) => return None,
        }
    }
}

async fn test_publish_qos0<H, V, O>(connack_rc: V::ConnackRc)
where
    H: Harness,
    V: MqttVersion,
    O: ClientOpts<V> + From<GenericClientOpts>,
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
            Connect::new(true, 30, "client-id".to_string(), None, None, None).into()
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

    let (_, client) = H::connect::<V, O>(
        GenericClientOpts(ClientOptsV5 {
            client_id: "client-id".to_string(),
            keep_alive: 30,
            ..Default::default()
        }),
        addr.to_string(),
    )
    .await
    .unwrap();
    assert!(H::publish(
        &client,
        Publish::new(
            MqttTopic::try_from("topic").unwrap(),
            b"payload",
            Qos::AtMostOnce,
            false
        )
    )
    .await
    .unwrap()
    .is_none());
    H::disconnect(client).await.unwrap();
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

async fn test_publish_qos1<H, V, O>(connack_rc: V::ConnackRc)
where
    H: Harness,
    V: MqttVersion,
    O: ClientOpts<V> + From<GenericClientOpts>,
    VersionedConnect: From<Connect<V>>,
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
            Connect::new(true, 30, "client-id".to_string(), None, None, None).into()
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

    let (_, client) = H::connect::<V, O>(
        GenericClientOpts(ClientOptsV5 {
            client_id: "client-id".to_string(),
            keep_alive: 30,
            ..Default::default()
        }),
        addr.to_string(),
    )
    .await
    .unwrap();
    let msg = H::publish(
        &client,
        Publish::new(
            MqttTopic::try_from("topic").unwrap(),
            b"payload",
            Qos::AtLeastOnce,
            false,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    send.send(msg.packet_identifier()).unwrap();
    H::wait_until_delivered(msg).await;

    H::disconnect(client).await.unwrap();
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

async fn test_publish_qos2<H, V, O>(connack_rc: V::ConnackRc)
where
    H: Harness,
    V: MqttVersion,
    O: ClientOpts<V> + From<GenericClientOpts>,
    VersionedConnect: From<Connect<V>>,
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
            Connect::new(true, 30, "client-id".to_string(), None, None, None).into()
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

    let (_, client) = H::connect::<V, O>(
        GenericClientOpts(ClientOptsV5 {
            client_id: "client-id".to_string(),
            keep_alive: 30,
            ..Default::default()
        }),
        addr.to_string(),
    )
    .await
    .unwrap();
    let msg = H::publish(
        &client,
        Publish::new(
            MqttTopic::try_from("topic").unwrap(),
            b"payload",
            Qos::ExactlyOnce,
            false,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    send.send(msg.packet_identifier()).unwrap();
    tracing::info!("Wait for receive!");
    H::wait_until_delivered(msg).await;
    tracing::info!("Received");

    H::disconnect(client).await.unwrap();
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

async fn test_publish_resend_qos1<H, V, O>(connack_rc: V::ConnackRc)
where
    H: Harness,
    V: MqttVersion,
    O: ClientOpts<V> + From<GenericClientOpts>,
    VersionedConnect: From<Connect<V>>,
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

    let (_, client) = H::connect::<V, O>(
        GenericClientOpts(ClientOptsV5 {
            client_id: "client-id".to_string(),
            keep_alive: 1,
            resend_interval: Duration::from_millis(200),
            ..Default::default()
        }),
        addr.to_string(),
    )
    .await
    .unwrap();
    let msg = H::publish(
        &client,
        Publish::new(
            MqttTopic::try_from("topic").unwrap(),
            b"payload",
            Qos::AtLeastOnce,
            false,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    send.send(msg.packet_identifier()).unwrap();
    H::wait_until_delivered(msg).await;

    H::disconnect(client).await.unwrap();
    tx_close.send(()).unwrap();
    handle.join().unwrap();
}
test!(
    publish_resend_qos1,
    test_publish_resend_qos1,
    5000,
    (ConnectRcV3::Accepted),
    (ConnectRcV5::Accepted)
);

async fn test_publish_resend_qos2<H, V, O>(connack_rc: V::ConnackRc)
where
    H: Harness,
    V: MqttVersion,
    O: ClientOpts<V> + From<GenericClientOpts>,
    VersionedConnect: From<Connect<V>>,
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

    let (_, client) = H::connect::<V, O>(
        GenericClientOpts(ClientOptsV5 {
            client_id: "client-id".to_string(),
            keep_alive: 1,
            resend_interval: Duration::from_millis(200),
            ..Default::default()
        }),
        addr.to_string(),
    )
    .await
    .unwrap();
    let msg = H::publish(
        &client,
        Publish::new(
            MqttTopic::try_from("topic").unwrap(),
            b"payload",
            Qos::ExactlyOnce,
            false,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    send.send(msg.packet_identifier()).unwrap();
    H::wait_until_delivered(msg).await;

    H::disconnect(client).await.unwrap();
    tx_close.send(()).unwrap();
    handle.join().unwrap();
}
test!(
    publish_resend_qos2,
    test_publish_resend_qos2,
    5000,
    (ConnectRcV3::Accepted),
    (ConnectRcV5::Accepted)
);

async fn test_publish_resend_pubrel_qos2<H, V, O>(connack_rc: V::ConnackRc)
where
    H: Harness,
    V: MqttVersion,
    O: ClientOpts<V> + From<GenericClientOpts>,
    VersionedConnect: From<Connect<V>>,
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

        let (header, mut data) = loop {
            let (header, mut data) = util::read_packet(&mut stream);
            match &header.control_packet_type {
                ControlPacketType::PingReq => {
                    assert_eq!(data.len(), 0);
                    let ping_req = PingReq::try_read_entire_buf(header, &mut data).unwrap();
                    assert_eq!(ping_req, PingReq);

                    write_packet(&mut stream, |buf| PingResp.write_to_buf(buf));
                }
                ControlPacketType::PubRel => {
                    break (header, data);
                }
                _ => panic!("Received unexpected msg: {header:?}"),
            }
        };
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

    let (_, client) = H::connect::<V, O>(
        GenericClientOpts(ClientOptsV5 {
            client_id: "client-id".to_string(),
            keep_alive: 1,
            resend_interval: Duration::from_millis(200),
            ..Default::default()
        }),
        addr.to_string(),
    )
    .await
    .unwrap();
    let msg = H::publish(
        &client,
        Publish::new(
            MqttTopic::try_from("topic").unwrap(),
            b"payload",
            Qos::ExactlyOnce,
            false,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    send.send(msg.packet_identifier()).unwrap();
    H::wait_until_delivered(msg).await;

    H::disconnect(client).await.unwrap();
    tx_close.send(()).unwrap();
    handle.join().unwrap();
}
test!(
    publish_resend_pubrel_qos2,
    test_publish_resend_pubrel_qos2,
    5000,
    (ConnectRcV3::Accepted),
    (ConnectRcV5::Accepted)
);

/// A QoS 1 PUBLISH issued while the backend is parked in `read()` must be acknowledged
/// without being retransmitted.
///
/// Both backends drain `inflight_ch` into `inflight_msgs` before they block in `read()`, but
/// the client only publishes a message into `inflight_msgs` by sending on that channel. A
/// publish issued while the backend is idle has therefore not been drained yet when its
/// PUBACK arrives, so `handle_msg` finds no entry for it, logs "Received unexpected PubAck"
/// and drops it. Delivery is then reported only after the client retransmits and that copy
/// is acked — so every QoS 1 publish from an idle client reaches the broker twice.
///
/// The yield before publishing is what makes this deterministic: publishing straight after
/// `connect()` is the one ordering where the drain wins, which is why `publish_qos1` passes
/// and never sees this.
///
/// The publish count is sampled once delivery completes, because a dropped PUBACK cannot
/// delay delivery without the broker having already seen the duplicate.
async fn test_publish_qos1_while_backend_idle<H, V, O>(connack_rc: V::ConnackRc)
where
    H: Harness,
    V: MqttVersion,
    O: ClientOpts<V> + From<GenericClientOpts>,
    VersionedConnect: From<Connect<V>>,
{
    util::init_logging();
    let server = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = server.local_addr().unwrap();
    let (publish_count_tx, publish_count_rx) = std::sync::mpsc::channel();
    let (tx_close, rx_close) = std::sync::mpsc::channel();
    let test_finished = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

    let handle = {
        let test_finished = test_finished.clone();
        std::thread::spawn(move || {
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

            // Ack every PUBLISH we receive and keep listening afterwards, so a client that
            // failed to match our PUBACK has its retransmission counted rather than ignored.
            let mut publishes = 0;
            let deadline = Instant::now() + Duration::from_secs(20);
            loop {
                if test_finished.load(std::sync::atomic::Ordering::Relaxed)
                    || Instant::now() > deadline
                {
                    break;
                }
                let Some((header, mut data)) = read_packet_timeout(&mut stream) else {
                    continue;
                };
                if !matches!(
                    header.control_packet_type,
                    ControlPacketType::Publish { .. }
                ) {
                    // PINGREQ keepalives and the like are not interesting here.
                    continue;
                }
                let msg = Publish::<V, QosPacketIdentifier>::try_read_entire_buf(header, &mut data)
                    .unwrap();
                assert_eq!(
                    msg,
                    Publish::new(
                        MqttTopic::try_from("topic").unwrap(),
                        b"payload",
                        Qos::AtLeastOnce,
                        false
                    )
                    .assign_packet_identifier(|| msg.packet_identifier().unwrap(), false)
                );
                publishes += 1;
                write_packet(&mut stream, |buf| {
                    PubAck::<V>::new_ok(msg.packet_identifier().unwrap()).write_to_buf(buf)
                });
            }
            // Tolerate a failed assertion in the test body: it drops the receiver, and the
            // broker thread should not add a second, unrelated panic on top of it.
            let _ = publish_count_tx.send(publishes);
            rx_close.recv().unwrap();
        })
    };

    let (_, client) = H::connect::<V, O>(
        GenericClientOpts(ClientOptsV5 {
            client_id: "client-id".to_string(),
            // A short keep alive lets the backend wake up and run its resend check quickly,
            // and a short resend interval makes the unwanted retransmission appear soon.
            keep_alive: 1,
            resend_interval: Duration::from_millis(200),
            ..Default::default()
        }),
        addr.to_string(),
    )
    .await
    .unwrap();

    // Let the backend complete its first loop iteration and park in read(). Even a
    // zero-duration sleep is enough to lose the drain race on every harness.
    H::sleep(Duration::from_millis(300)).await;

    let inflight = H::publish(
        &client,
        Publish::new(
            MqttTopic::try_from("topic").unwrap(),
            b"payload",
            Qos::AtLeastOnce,
            false,
        ),
    )
    .await
    .unwrap()
    .unwrap();

    H::wait_until_delivered(inflight).await;
    test_finished.store(true, std::sync::atomic::Ordering::Relaxed);

    let publishes = publish_count_rx.recv().unwrap();
    assert_eq!(
        publishes, 1,
        "QoS 1 publish issued while the backend was idle was retransmitted: the broker saw \
         {publishes} PUBLISH packets, expected 1. Its PUBACK was dropped because the message \
         had not been drained from inflight_ch into inflight_msgs yet."
    );

    H::disconnect(client).await.unwrap();
    tx_close.send(()).unwrap();
    handle.join().unwrap();
}
test!(
    publish_qos1_while_backend_idle,
    test_publish_qos1_while_backend_idle,
    10000,
    (ConnectRcV3::Accepted),
    (ConnectRcV5::Accepted)
);

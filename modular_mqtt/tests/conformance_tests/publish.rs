use crate::util::{self, GenericClientOpts, Harness, START_MID};

use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use modular_mqtt::{ClientOpts, ClientOptsV5, OnDisconnectBehavior, SyncClient};
use modular_mqtt_protocol::{
    ConnAck, Connect, ConnectRcV3, ConnectRcV5, ControlPacketType, FixedHeader, MqttTopic,
    MqttVersion, Packet, PingReq, PingResp, PubAck, PubComp, PubRec, PubRel, Publish, Qos,
    QosPacketIdentifier, VersionedConnect,
};

async fn test_publish_qos0<H, V, O>(connack_rc: V::ConnackRc)
where
    H: Harness,
    V: MqttVersion,
    O: ClientOpts<V> + From<GenericClientOpts>,
    VersionedConnect: From<Connect<V>>,
{
    util::init_logging();
    let mut broker = H::broker().await;
    let addr = broker.addr();
    let (tx_close, mut rx_close) = H::channel();

    let handle = H::spawn(async move {
        let mut stream = broker.accept().await;
        stream
            .read_connect_and_compare(
                Connect::try_new(true, 30, "client-id".to_string(), None, None, None).unwrap(),
            )
            .await;
        stream
            .write_packet(ConnAck::<V>::new(false, connack_rc))
            .await;

        stream
            .read_and_compare_msg(
                Publish::<V, Qos>::new(
                    MqttTopic::try_from("topic").unwrap(),
                    b"payload",
                    Qos::AtMostOnce,
                    false,
                )
                .assign_packet_identifier(|| 1, false),
            )
            .await;
        rx_close.recv().await;
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
    H::disconnect(client).await;
    tx_close.send(()).await;
    handle.join().await;
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
    let mut broker = H::broker().await;
    let addr = broker.addr();
    let (send, mut recv) = H::channel();
    let (tx_close, mut rx_close) = H::channel();

    let handle = H::spawn(async move {
        let mut stream = broker.accept().await;
        stream
            .read_connect_and_compare(
                Connect::try_new(true, 30, "client-id".to_string(), None, None, None).unwrap(),
            )
            .await;
        stream
            .write_packet(ConnAck::<V>::new(false, connack_rc))
            .await;

        let expected_packet_identifier = recv.recv().await;
        stream
            .read_and_compare_msg(
                Publish::<V, Qos>::new(
                    MqttTopic::try_from("topic").unwrap(),
                    b"payload",
                    Qos::AtLeastOnce,
                    false,
                )
                .assign_packet_identifier(|| expected_packet_identifier, false),
            )
            .await;
        stream
            .write_packet(PubAck::<V>::new_ok(expected_packet_identifier))
            .await;
        rx_close.recv().await;
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
    send.send(msg.packet_identifier()).await;
    H::wait_until_delivered(msg).await;

    H::disconnect(client).await;
    tx_close.send(()).await;
    handle.join().await;
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
    let mut broker = H::broker().await;
    let addr = broker.addr();
    let (send, mut recv) = H::channel();
    let (tx_close, mut rx_close) = H::channel();

    let handle = H::spawn(async move {
        let mut stream = broker.accept().await;
        stream
            .read_connect_and_compare(
                Connect::try_new(true, 30, "client-id".to_string(), None, None, None).unwrap(),
            )
            .await;
        stream
            .write_packet(ConnAck::<V>::new(false, connack_rc))
            .await;

        let expected_packet_identifier = recv.recv().await;
        stream
            .read_and_compare_msg(
                Publish::<V, Qos>::new(
                    MqttTopic::try_from("topic").unwrap(),
                    b"payload",
                    Qos::ExactlyOnce,
                    false,
                )
                .assign_packet_identifier(|| expected_packet_identifier, false),
            )
            .await;
        stream
            .write_packet(PubRec::<V>::new_ok(expected_packet_identifier))
            .await;

        let (header, mut data) = stream.read_packet().await;
        let pub_rel = PubRel::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(pub_rel, PubRel::<V>::new_ok(expected_packet_identifier));
        stream
            .write_packet(PubComp::<V>::new_ok(expected_packet_identifier))
            .await;
        rx_close.recv().await;
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
    send.send(msg.packet_identifier()).await;
    tracing::info!("Wait for receive!");
    H::wait_until_delivered(msg).await;
    tracing::info!("Received");

    H::disconnect(client).await;
    tx_close.send(()).await;
    handle.join().await;
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

    let mut broker = H::broker().await;
    let addr = broker.addr();
    let (send, mut recv) = H::channel();
    let (tx_close, mut rx_close) = H::channel();

    let handle = H::spawn(async move {
        let mut stream = broker.accept().await;
        stream
            .read_connect_and_compare(
                Connect::try_new(true, 1, "client-id".to_string(), None, None, None).unwrap(),
            )
            .await;
        stream
            .write_packet(ConnAck::<V>::new(false, connack_rc))
            .await;

        let expected_packet_identifier = recv.recv().await;
        stream
            .read_and_compare_msg(
                Publish::<V, Qos>::new(
                    MqttTopic::try_from("topic").unwrap(),
                    b"payload",
                    Qos::AtLeastOnce,
                    false,
                )
                .assign_packet_identifier(|| expected_packet_identifier, false),
            )
            .await;
        let (header, mut data) = loop {
            let (header, mut data) = stream.read_packet().await;
            match &header.control_packet_type {
                ControlPacketType::PingReq => {
                    assert_eq!(data.len(), 0);
                    let ping_req = PingReq::try_read_entire_buf(header, &mut data).unwrap();
                    assert_eq!(ping_req, PingReq);

                    stream.write_packet(PingResp).await;
                }
                ControlPacketType::Publish { .. } => {
                    break (header, data);
                }
                _ => panic!("Should not get here"),
            }
        };
        let recv_pub2 =
            Publish::<V, QosPacketIdentifier>::try_read_entire_buf(header, &mut data).unwrap();
        // This is a re-delivery attempt, so DUP must be set [MQTT-3.3.1-1]. The first
        // PUBLISH above is asserted with dup: false, which pins both directions.
        assert_eq!(
            recv_pub2,
            Publish::new(
                MqttTopic::try_from("topic").unwrap(),
                b"payload",
                Qos::AtLeastOnce,
                false
            )
            .assign_packet_identifier(|| recv_pub2.packet_identifier().unwrap(), true),
        );

        stream
            .write_packet(PubAck::<V>::new_ok(recv_pub2.packet_identifier().unwrap()))
            .await;
        rx_close.recv().await;
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
    send.send(msg.packet_identifier()).await;
    H::wait_until_delivered(msg).await;

    H::disconnect(client).await;
    tx_close.send(()).await;
    handle.join().await;
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
    let mut broker = H::broker().await;
    let addr = broker.addr();
    let (send, mut recv) = H::channel();
    let (tx_close, mut rx_close) = H::channel();

    let handle = H::spawn(async move {
        let mut stream = broker.accept().await;
        stream
            .read_connect_and_compare(
                Connect::try_new(true, 1, "client-id".to_string(), None, None, None).unwrap(),
            )
            .await;
        stream
            .write_packet(ConnAck::<V>::new(false, connack_rc))
            .await;

        let expected_packet_identifier = recv.recv().await;
        stream
            .read_and_compare_msg(
                Publish::<V, Qos>::new(
                    MqttTopic::try_from("topic").unwrap(),
                    b"payload",
                    Qos::ExactlyOnce,
                    false,
                )
                .assign_packet_identifier(|| expected_packet_identifier, false),
            )
            .await;
        let (header, mut data) = loop {
            let (header, mut data) = stream.read_packet().await;
            match &header.control_packet_type {
                ControlPacketType::PingReq => {
                    assert_eq!(data.len(), 0);
                    let connect = PingReq::try_read_entire_buf(header, &mut data).unwrap();
                    assert_eq!(connect, PingReq);

                    stream.write_packet(PingResp).await;
                }
                ControlPacketType::Publish { .. } => {
                    break (header, data);
                }
                _ => panic!("Should not get here"),
            }
        };
        let recv_pub2 =
            Publish::<V, QosPacketIdentifier>::try_read_entire_buf(header, &mut data).unwrap();
        // Re-delivery attempt: DUP must be set [MQTT-3.3.1-1]. The first PUBLISH
        // above asserts dup: false, so both directions are covered.
        assert_eq!(
            recv_pub2,
            Publish::new(
                MqttTopic::try_from("topic").unwrap(),
                b"payload",
                Qos::ExactlyOnce,
                false
            )
            .assign_packet_identifier(|| expected_packet_identifier, true)
        );

        stream
            .write_packet(PubRec::<V>::new_ok(recv_pub2.packet_identifier().unwrap()))
            .await;

        stream
            .read_and_compare_msg(PubRel::<V>::new_ok(expected_packet_identifier))
            .await;
        stream
            .write_packet(PubComp::<V>::new_ok(recv_pub2.packet_identifier().unwrap()))
            .await;
        rx_close.recv().await;
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
    send.send(msg.packet_identifier()).await;
    H::wait_until_delivered(msg).await;

    H::disconnect(client).await;
    tx_close.send(()).await;
    handle.join().await;
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
    let mut broker = H::broker().await;
    let addr = broker.addr();
    let (send, mut recv) = H::channel();
    let (tx_close, mut rx_close) = H::channel();

    let handle = H::spawn(async move {
        let mut stream = broker.accept().await;
        stream
            .read_connect_and_compare(
                Connect::<V>::try_new(true, 1, "client-id".to_string(), None, None, None).unwrap(),
            )
            .await;
        stream
            .write_packet(ConnAck::<V>::new(false, connack_rc))
            .await;

        let expected_packet_identifier = recv.recv().await;
        stream
            .read_and_compare_msg(
                Publish::<V, Qos>::new(
                    MqttTopic::try_from("topic").unwrap(),
                    b"payload",
                    Qos::ExactlyOnce,
                    false,
                )
                .assign_packet_identifier(|| expected_packet_identifier, false),
            )
            .await;

        stream
            .write_packet(PubRec::<V>::new_ok(expected_packet_identifier))
            .await;

        let (header, mut data) = loop {
            let (header, mut data) = stream.read_packet().await;
            match &header.control_packet_type {
                ControlPacketType::PingReq => {
                    assert_eq!(data.len(), 0);
                    let ping_req = PingReq::try_read_entire_buf(header, &mut data).unwrap();
                    assert_eq!(ping_req, PingReq);

                    stream.write_packet(PingResp).await;
                }
                ControlPacketType::PubRel => {
                    break (header, data);
                }
                _ => panic!("Received unexpected msg: {header:?}"),
            }
        };
        let pub_rel = PubRel::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(expected_packet_identifier, pub_rel.packet_identifier());
        assert_eq!(pub_rel, PubRel::<V>::new_ok(expected_packet_identifier));

        let (header, mut data) = loop {
            let (header, mut data) = stream.read_packet().await;
            match &header.control_packet_type {
                ControlPacketType::PingReq => {
                    assert_eq!(data.len(), 0);
                    let connect = PingReq::try_read_entire_buf(header, &mut data).unwrap();
                    assert_eq!(connect, PingReq);

                    stream.write_packet(PingResp).await;
                }
                ControlPacketType::PubRel => {
                    break (header, data);
                }
                _ => panic!("Should not get here"),
            }
        };
        let pub_rel2 = PubRel::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(pub_rel2, PubRel::<V>::new_ok(pub_rel2.packet_identifier()));
        stream
            .write_packet(PubComp::<V>::new_ok(expected_packet_identifier))
            .await;
        rx_close.recv().await;
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
    send.send(msg.packet_identifier()).await;
    H::wait_until_delivered(msg).await;

    H::disconnect(client).await;
    tx_close.send(()).await;
    handle.join().await;
}
test!(
    publish_resend_pubrel_qos2,
    test_publish_resend_pubrel_qos2,
    5000,
    (ConnectRcV3::Accepted),
    (ConnectRcV5::Accepted)
);

/// Re-delivering a PUBLISH after a reconnect must also set DUP [MQTT-3.3.1-1].
///
/// `check_resend_msgs` covers the resend-interval path; this covers the other
/// re-delivery path, `re_write_qos1_and_qos2_to_write_buf`, which rewrites every
/// in-flight message onto the fresh socket. The first PUBLISH asserts `dup: false`,
/// so both directions are pinned.
///
/// The broker hangs up without acknowledging, the client reconnects, and the
/// retransmission must arrive with DUP set.
async fn test_publish_resend_dup_after_reconnect<H, V, O>(connack_rc: V::ConnackRc)
where
    H: Harness,
    V: MqttVersion,
    O: ClientOpts<V> + From<GenericClientOpts>,
    VersionedConnect: From<Connect<V>>,
{
    util::init_logging();

    let mut broker = H::broker().await;
    let addr = broker.addr();
    let (tx_close, mut rx_close) = H::channel();
    // `publish` returns as soon as the message is queued, so the client thread would
    // otherwise race ahead and disconnect before the broker has hung up and the
    // reconnect has happened. The broker signals once its assertions are done.
    let (tx_done, mut rx_done) = H::channel();

    let handle = H::spawn(async move {
        // First connection: handshake, take the PUBLISH, then hang up unacknowledged.
        let mut first = broker.accept().await;
        first
            .read_connect_and_compare(
                Connect::<V>::try_new(true, 30, "client-id".to_string(), None, None, None).unwrap(),
            )
            .await;
        first
            .write_packet(ConnAck::<V>::new(false, connack_rc))
            .await;

        first
            .read_and_compare_msg(
                Publish::<V, Qos>::new(
                    MqttTopic::try_from("topic").unwrap(),
                    b"payload",
                    Qos::AtLeastOnce,
                    false,
                )
                .assign_packet_identifier(|| START_MID, false),
            )
            .await;
        drop(first); // close the socket without sending a PUBACK

        // Second connection: the client must retransmit with DUP set.
        let mut second = broker.accept().await;
        second
            .read_connect_and_compare(
                Connect::<V>::try_new(true, 30, "client-id".to_string(), None, None, None).unwrap(),
            )
            .await;
        second
            .write_packet(ConnAck::<V>::new(false, connack_rc))
            .await;

        second
            .read_and_compare_msg(
                Publish::<V, Qos>::new(
                    MqttTopic::try_from("topic").unwrap(),
                    b"payload",
                    Qos::AtLeastOnce,
                    false,
                )
                .assign_packet_identifier(|| START_MID, true),
            )
            .await;

        second.write_packet(PubAck::<V>::new_ok(START_MID)).await;
        tx_done.send(()).await;
        rx_close.recv().await;
    });

    let (_, client) = H::connect::<V, O>(
        GenericClientOpts(ClientOptsV5 {
            client_id: "client-id".to_string(),
            keep_alive: 30,
            on_disconnect: OnDisconnectBehavior::ReconnectExponentialBackoff {
                min_retry_interval: Duration::from_millis(50),
                max_retry_interval: Duration::from_millis(200),
            },
            ..Default::default()
        }),
        addr.to_string(),
    )
    .await
    .unwrap();

    H::publish(
        &client,
        Publish::new(
            MqttTopic::try_from("topic").unwrap(),
            b"payload",
            Qos::AtLeastOnce,
            false,
        ),
    )
    .await
    .unwrap();

    rx_done.recv().await;
    H::disconnect(client).await;
    tx_close.send(()).await;
    handle.join().await;
}

test!(
    publish_resend_dup_after_reconnect,
    test_publish_resend_dup_after_reconnect,
    10000,
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
    let mut broker = H::broker().await;
    let addr = broker.addr();
    let (publish_count_tx, mut publish_count_rx) = H::channel();
    let (tx_close, mut rx_close) = H::channel();
    let test_finished = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

    let handle = {
        let test_finished = test_finished.clone();
        H::spawn(async move {
            let mut stream = broker.accept().await;
            stream
                .read_connect_and_compare(
                    Connect::try_new(true, 1, "client-id".to_string(), None, None, None).unwrap(),
                )
                .await;
            stream
                .write_packet(ConnAck::<V>::new(false, connack_rc))
                .await;

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
                let Some((header, mut data)) =
                    stream.read_packet_timeout(Duration::from_millis(150)).await
                else {
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
                stream
                    .write_packet({ PubAck::<V>::new_ok(msg.packet_identifier().unwrap()) })
                    .await;
            }
            // Tolerate a failed assertion in the test body: it drops the receiver, and the
            // broker thread should not add a second, unrelated panic on top of it.
            let _ = publish_count_tx.send(publishes).await;
            rx_close.recv().await;
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

    let publishes = publish_count_rx.recv().await;
    assert_eq!(
        publishes, 1,
        "QoS 1 publish issued while the backend was idle was retransmitted: the broker saw \
         {publishes} PUBLISH packets, expected 1. Its PUBACK was dropped because the message \
         had not been drained from inflight_ch into inflight_msgs yet."
    );

    H::disconnect(client).await;
    tx_close.send(()).await;
    handle.join().await;
}
test!(
    publish_qos1_while_backend_idle,
    test_publish_qos1_while_backend_idle,
    10000,
    (ConnectRcV3::Accepted),
    (ConnectRcV5::Accepted)
);

async fn test_large_publish_paced_across_keepalive<H, V, O>(connack_rc: V::ConnackRc)
where
    H: Harness,
    V: MqttVersion,
    O: ClientOpts<V> + From<GenericClientOpts>,
    VersionedConnect: From<Connect<V>>,
{
    util::init_logging();
    let mut broker = H::broker().await;
    let addr = broker.addr();
    let (tx_close, mut rx_close) = H::channel();
    let (tx_disconnect, mut rx_disconnect) = H::channel();

    let payload: Vec<u8> = (0..5000u32).map(|i| i as u8).collect();
    let _payload = payload.clone();

    let handle = H::spawn(async move {
        let payload = _payload;

        let mut stream = broker.accept().await;
        stream
            .read_connect_and_compare(
                Connect::try_new(true, 2, "client-id".to_string(), None, None, None).unwrap(),
            )
            .await;
        stream
            .write_packet(ConnAck::<V>::new(false, connack_rc))
            .await;

        let id = || 100;

        let mut packet = BytesMut::new();
        Publish::<V, Qos>::new(
            MqttTopic::try_from("t/large").unwrap(),
            payload,
            Qos::AtMostOnce,
            false,
        )
        .assign_packet_identifier(id, false)
        .write_to_buf(&mut packet);
        assert!(packet.len() > 4096, "must exceed STREAM_READ_CHUNK_SIZE");

        let mut got_ping = false;
        // Gaps (300ms) sit above MIN_READ_TIMEOUT (5ms) but below keep_alive (2s),
        // and the 3s total exceeds keep_alive — so the deadline must be refreshed
        // on every read or the client falsely declares a stall.
        for chunk in packet.chunks(500) {
            H::sleep(Duration::from_millis(300)).await;
            stream
                .write_packet_partial(|buf| buf.extend_from_slice(chunk))
                .await;

            if let Some((fixed_header, mut data)) =
                stream.read_packet_timeout(Duration::from_millis(50)).await
            {
                assert!(
                    matches!(fixed_header.control_packet_type, ControlPacketType::PingReq),
                    "client sent a packet mid-transfer"
                );
                PingReq::try_read(fixed_header, &mut data).unwrap();
                got_ping = true;
            }
        }
        if !got_ping {
            let (header, mut data) = stream.read_packet().await;
            assert_eq!(data.len(), 0);
            let ping = PingReq::try_read(header, &mut data).unwrap();
            assert_eq!(ping, PingReq);
        }

        stream.write_packet(PingResp).await;

        tx_disconnect.send(()).await;
        rx_close.recv().await;
    });
    let (mut rx, client) = H::connect::<V, O>(
        GenericClientOpts(ClientOptsV5 {
            client_id: "client-id".to_string(),
            keep_alive: 2,
            ..Default::default()
        }),
        addr.to_string(),
    )
    .await
    .unwrap();

    let msg = H::recv(&mut rx).await;
    assert_eq!(msg.payload(), &payload[..], "payload corrupted");
    rx_disconnect.recv().await;
    H::disconnect(client).await;
    tx_close.send(()).await;
    handle.join().await;
}

test!(
    large_publish_paced_across_keepalive,
    test_large_publish_paced_across_keepalive,
    20000,
    (ConnectRcV3::Accepted),
    (ConnectRcV5::Accepted)
);

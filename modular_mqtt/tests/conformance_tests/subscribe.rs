use crate::util::{self, write_packet, GenericClientOpts, Harness};

use std::{net::TcpListener, time::Duration};

use modular_mqtt::{util::IntoTopicSubscription, ClientOpts, ClientOptsV5, SyncClient};
use modular_mqtt_protocol::{
    ConnAck, Connect, ConnectRcV3, ConnectRcV5, MqttTopic, MqttVersion, Packet, PubAck, PubComp,
    PubRec, PubRel, Publish, Qos, SubAck, SubAckDataV5, SubRcV3, SubRcV5, Subscribe,
    TopicSubscription, VersionedConnect,
};

async fn test_sub_qos0<H, V, O>(
    connack_rc: V::ConnackRc,
    sub_rc: V::SubAckData,
    sub_rc2: V::SubAckData,
) where
    H: Harness,
    V: MqttVersion,
    O: ClientOpts<V> + From<GenericClientOpts>,
    VersionedConnect: From<Connect<V>>,
    for<'a> &'a str: IntoTopicSubscription<V>,
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
        let sub = Subscribe::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(
            sub,
            Subscribe::<V>::new(
                sub.packet_identifier(),
                vec![V::TopicSubscription::new(
                    "topic".to_string(),
                    Qos::AtMostOnce
                )]
            )
        );
        send.send(sub.packet_identifier()).unwrap();
        write_packet(&mut stream, |buf| {
            SubAck::<V>::new(sub.packet_identifier(), sub_rc).write_to_buf(buf)
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
    let suback = H::subscribe(
        &client,
        vec!["topic"],
        Qos::AtMostOnce,
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    let packet_identifier = recv.recv().unwrap();
    assert_eq!(suback, SubAck::<V>::new(packet_identifier, sub_rc2));
    H::disconnect(client).await.unwrap();
    tx_close.send(()).unwrap();
    handle.join().unwrap();
}
test!(
    sub_qos0,
    test_sub_qos0,
    5000,
    (
        ConnectRcV3::Accepted,
        vec![SubRcV3::SuccessQos0],
        vec![SubRcV3::SuccessQos0]
    ),
    (
        ConnectRcV5::Accepted,
        SubAckDataV5 {
            return_codes: vec![SubRcV5::SuccessQos0],
            reason: None,
            user_property: Vec::new()
        },
        SubAckDataV5 {
            return_codes: vec![SubRcV5::SuccessQos0],
            reason: None,
            user_property: Vec::new()
        }
    )
);

async fn test_sub_qos0_receive_packet<H, V, O>(
    connack_rc: V::ConnackRc,
    sub_rc: V::SubAckData,
    sub_rc2: V::SubAckData,
) where
    H: Harness,
    V: MqttVersion,
    O: ClientOpts<V> + From<GenericClientOpts>,
    VersionedConnect: From<Connect<V>>,
    for<'a> &'a str: IntoTopicSubscription<V>,
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
        let sub = Subscribe::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(
            sub,
            Subscribe::<V>::new(
                sub.packet_identifier(),
                vec![V::TopicSubscription::new(
                    "topic".to_string(),
                    Qos::AtMostOnce
                )]
            )
        );
        send.send(sub.packet_identifier()).unwrap();
        write_packet(&mut stream, |buf| {
            SubAck::<V>::new(sub.packet_identifier(), sub_rc).write_to_buf(buf)
        });
        let topic = MqttTopic::try_from("topic").unwrap();
        write_packet(&mut stream, |buf| {
            Publish::<V, Qos>::new(topic, b"test", Qos::AtMostOnce, false)
                .assign_packet_identifier(|| 1, false)
                .write_to_buf(buf)
        });
        rx_close.recv().unwrap();
    });

    let (mut stream, client) = H::connect::<V, O>(
        GenericClientOpts(ClientOptsV5 {
            client_id: "client-id".to_string(),
            keep_alive: 30,
            ..Default::default()
        }),
        addr.to_string(),
    )
    .await
    .unwrap();

    let suback = H::subscribe(
        &client,
        vec!["topic"],
        Qos::AtMostOnce,
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    let packet_identifier = recv.recv().unwrap();
    assert_eq!(suback, SubAck::new(packet_identifier, sub_rc2));
    let msg = H::recv(&mut stream).await;
    assert_eq!(
        msg,
        Publish::new(
            MqttTopic::try_from("topic").unwrap(),
            b"test",
            Qos::AtMostOnce,
            false
        )
        .assign_packet_identifier(|| 1, false)
    );
    H::disconnect(client).await.unwrap();
    tx_close.send(()).unwrap();
    handle.join().unwrap();
}
test!(
    sub_qos0_receive_packet,
    test_sub_qos0_receive_packet,
    5000,
    (
        ConnectRcV3::Accepted,
        vec![SubRcV3::SuccessQos0],
        vec![SubRcV3::SuccessQos0]
    ),
    (
        ConnectRcV5::Accepted,
        SubAckDataV5 {
            return_codes: vec![SubRcV5::SuccessQos0],
            reason: None,
            user_property: Vec::new()
        },
        SubAckDataV5 {
            return_codes: vec![SubRcV5::SuccessQos0],
            reason: None,
            user_property: Vec::new()
        }
    )
);

async fn test_sub_qos1_receive_packet<H, V, O>(
    connack_rc: V::ConnackRc,
    sub_rc: V::SubAckData,
    sub_rc2: V::SubAckData,
) where
    H: Harness,
    V: MqttVersion,
    O: ClientOpts<V> + From<GenericClientOpts>,
    VersionedConnect: From<Connect<V>>,
    for<'a> &'a str: IntoTopicSubscription<V>,
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
        let sub = Subscribe::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(
            sub,
            Subscribe::<V>::new(
                sub.packet_identifier(),
                vec![V::TopicSubscription::new(
                    "topic".to_string(),
                    Qos::AtLeastOnce
                )]
            )
        );
        send.send(sub.packet_identifier()).unwrap();
        write_packet(&mut stream, |buf| {
            SubAck::<V>::new(sub.packet_identifier(), sub_rc).write_to_buf(buf)
        });
        let topic = MqttTopic::try_from("topic").unwrap();
        write_packet(&mut stream, |buf| {
            Publish::<V, Qos>::new(topic, b"test", Qos::AtLeastOnce, false)
                .assign_packet_identifier(|| 42, false)
                .write_to_buf(buf)
        });
        let (header, mut data) = util::read_packet(&mut stream);
        let ack = PubAck::<V>::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(ack, PubAck::new_ok(42));
        rx_close.recv().unwrap();
    });

    let (mut stream, client) = H::connect::<V, O>(
        GenericClientOpts(ClientOptsV5 {
            client_id: "client-id".to_string(),
            keep_alive: 30,
            ..Default::default()
        }),
        addr.to_string(),
    )
    .await
    .unwrap();

    let suback = H::subscribe(
        &client,
        vec!["topic"],
        Qos::AtLeastOnce,
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    let packet_identifier = recv.recv().unwrap();
    assert_eq!(suback, SubAck::new(packet_identifier, sub_rc2));
    let msg = H::recv(&mut stream).await;
    assert_eq!(
        msg,
        Publish::new(
            MqttTopic::try_from("topic").unwrap(),
            b"test",
            Qos::AtLeastOnce,
            false
        )
        .assign_packet_identifier(|| 42, false)
    );
    H::disconnect(client).await.unwrap();
    tx_close.send(()).unwrap();
    handle.join().unwrap();
}
test!(
    sub_qos1_receive_packet,
    test_sub_qos1_receive_packet,
    5000,
    (
        ConnectRcV3::Accepted,
        vec![SubRcV3::SuccessQos0],
        vec![SubRcV3::SuccessQos0]
    ),
    (
        ConnectRcV5::Accepted,
        SubAckDataV5 {
            return_codes: vec![SubRcV5::SuccessQos1],
            reason: None,
            user_property: Vec::new()
        },
        SubAckDataV5 {
            return_codes: vec![SubRcV5::SuccessQos1],
            reason: None,
            user_property: Vec::new()
        }
    )
);

async fn test_sub_qos2_receive_packet<H, V, O>(
    connack_rc: V::ConnackRc,
    sub_rc: V::SubAckData,
    sub_rc2: V::SubAckData,
) where
    H: Harness,
    V: MqttVersion,
    O: ClientOpts<V> + From<GenericClientOpts>,
    VersionedConnect: From<modular_mqtt_protocol::Connect<V>>,
    for<'a> &'a str: IntoTopicSubscription<V>,
{
    util::init_logging();
    let server = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = server.local_addr().unwrap();
    let (send, mut recv) = H::channel();
    let (pub_done_tx, mut pub_done_rx) = H::channel();
    let (tx_close, mut rx_close) = H::channel();

    let handle = H::spawn(async move {
        let (mut stream, _addr) = server.accept().unwrap();
        let (header, mut data) = util::read_packet(&mut stream);
        let connect = VersionedConnect::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(
            connect,
            Connect::<V>::new(true, 30, "client-id".to_string(), None, None, None).into()
        );
        write_packet(&mut stream, |buf| {
            ConnAck::<V>::new(false, connack_rc).write_to_buf(buf)
        });

        let (header, mut data) = util::read_packet(&mut stream);
        let sub = Subscribe::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(
            sub,
            Subscribe::<V>::new(
                sub.packet_identifier(),
                vec![V::TopicSubscription::new(
                    "topic".to_string(),
                    Qos::ExactlyOnce
                )]
            )
        );
        send.send(sub.packet_identifier()).await;
        write_packet(&mut stream, |buf| {
            SubAck::<V>::new(sub.packet_identifier(), sub_rc2).write_to_buf(buf)
        });
        let topic = MqttTopic::try_from("topic").unwrap();
        write_packet(&mut stream, |buf| {
            Publish::<V, Qos>::new(topic, b"test", Qos::ExactlyOnce, false)
                .assign_packet_identifier(|| 42, false)
                .write_to_buf(buf)
        });
        let (header, mut data) = util::read_packet(&mut stream);
        let rec = PubRec::<V>::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(rec, PubRec::new_ok(42));

        write_packet(&mut stream, |buf| PubRel::<V>::new_ok(42).write_to_buf(buf));
        let (header, mut data) = util::read_packet(&mut stream);
        let rec = PubComp::<V>::try_read_entire_buf(header, &mut data).unwrap();
        assert_eq!(rec, PubComp::new_ok(42));
        pub_done_tx.send(()).await;
        rx_close.recv().await;
    });

    let (mut stream, client) = H::connect::<V, O>(
        GenericClientOpts(ClientOptsV5 {
            client_id: "client-id".to_string(),
            keep_alive: 30,
            ..Default::default()
        }),
        addr.to_string(),
    )
    .await
    .unwrap();

    let suback = H::subscribe(
        &client,
        vec!["topic"],
        Qos::ExactlyOnce,
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    let packet_identifier = recv.recv().await;
    assert_eq!(suback, SubAck::<V>::new(packet_identifier, sub_rc));
    let msg = H::recv(&mut stream).await;
    assert_eq!(
        msg,
        Publish::new(
            MqttTopic::try_from("topic").unwrap(),
            b"test",
            Qos::ExactlyOnce,
            false
        )
        .assign_packet_identifier(|| 42, false)
    );
    pub_done_rx.recv().await;
    H::disconnect(client).await.unwrap();
    tx_close.send(()).await;
    handle.join().await;
}
test!(
    sub_qos2_receive_packet,
    test_sub_qos2_receive_packet,
    5000,
    (
        ConnectRcV3::Accepted,
        vec![SubRcV3::SuccessQos0],
        vec![SubRcV3::SuccessQos0]
    ),
    (
        ConnectRcV5::Accepted,
        SubAckDataV5 {
            return_codes: vec![SubRcV5::SuccessQos2],
            reason: None,
            user_property: Vec::new()
        },
        SubAckDataV5 {
            return_codes: vec![SubRcV5::SuccessQos2],
            reason: None,
            user_property: Vec::new()
        }
    )
);

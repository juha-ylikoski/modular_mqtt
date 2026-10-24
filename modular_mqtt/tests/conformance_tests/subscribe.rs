use crate::util::{self, GenericClientOpts, Harness, START_MID};

use std::{str, time::Duration};

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

        stream
            .read_and_compare_msg(Subscribe::<V>::new(
                START_MID,
                vec![V::TopicSubscription::new(
                    "topic".to_string(),
                    Qos::AtMostOnce,
                )],
            ))
            .await;
        send.send(()).await;
        stream
            .write_packet(SubAck::<V>::new(START_MID, sub_rc))
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
    let suback = H::subscribe(
        &client,
        vec!["topic"],
        Qos::AtMostOnce,
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    recv.recv().await;
    assert_eq!(suback, SubAck::<V>::new(START_MID, sub_rc2));
    H::disconnect(client).await;
    tx_close.send(()).await;
    handle.join().await;
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
    let mut broker = H::broker().await;
    let addr = broker.addr();
    let (send, mut recv) = H::channel();
    let (tx_close, mut rx_close) = H::channel();

    let handle = H::spawn(async move {
        let mut stream = broker.accept().await;
        stream
            .read_connect_and_compare(
                Connect::try_new(true, 30, "client-id".to_string(), None, None, None)
                    .unwrap()
                    .into(),
            )
            .await;
        stream
            .write_packet(ConnAck::<V>::new(false, connack_rc))
            .await;

        stream
            .read_and_compare_msg(Subscribe::<V>::new(
                START_MID,
                vec![V::TopicSubscription::new(
                    "topic".to_string(),
                    Qos::AtMostOnce,
                )],
            ))
            .await;
        send.send(()).await;
        stream
            .write_packet(SubAck::<V>::new(START_MID, sub_rc))
            .await;
        let topic = MqttTopic::try_from("topic").unwrap();
        stream
            .write_packet(
                Publish::<V, Qos>::new(topic, b"test", Qos::AtMostOnce, false)
                    .assign_packet_identifier(|| 2, false),
            )
            .await;
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
        Qos::AtMostOnce,
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    recv.recv().await;
    assert_eq!(suback, SubAck::new(START_MID, sub_rc2));
    let msg = H::recv(&mut stream).await;
    assert_eq!(
        msg,
        Publish::new(
            MqttTopic::try_from("topic").unwrap(),
            b"test",
            Qos::AtMostOnce,
            false
        )
        .assign_packet_identifier(|| 2, false)
    );
    H::disconnect(client).await;
    tx_close.send(()).await;
    handle.join().await;
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
    let mut broker = H::broker().await;
    let addr = broker.addr();
    let (send, mut recv) = H::channel();
    let (tx_close, mut rx_close) = H::channel();

    let handle = H::spawn(async move {
        let mut stream = broker.accept().await;
        stream
            .read_connect_and_compare(
                Connect::try_new(true, 30, "client-id".to_string(), None, None, None)
                    .unwrap()
                    .into(),
            )
            .await;
        stream
            .write_packet(ConnAck::<V>::new(false, connack_rc))
            .await;

        stream
            .read_and_compare_msg(Subscribe::<V>::new(
                START_MID,
                vec![V::TopicSubscription::new(
                    "topic".to_string(),
                    Qos::AtLeastOnce,
                )],
            ))
            .await;
        send.send(()).await;
        stream
            .write_packet(SubAck::<V>::new(START_MID, sub_rc))
            .await;
        let topic = MqttTopic::try_from("topic").unwrap();
        stream
            .write_packet(
                Publish::<V, Qos>::new(topic, b"test", Qos::AtLeastOnce, false)
                    .assign_packet_identifier(|| 42, false),
            )
            .await;
        stream.read_and_compare_msg(PubAck::<V>::new_ok(42)).await;
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
        Qos::AtLeastOnce,
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    let packet_identifier = recv.recv().await;
    assert_eq!(suback, SubAck::new(START_MID, sub_rc2));
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
    H::disconnect(client).await;
    tx_close.send(()).await;
    handle.join().await;
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
    let mut broker = H::broker().await;
    let addr = broker.addr();
    let (send, mut recv) = H::channel();
    let (pub_done_tx, mut pub_done_rx) = H::channel();
    let (tx_close, mut rx_close) = H::channel();

    let handle = H::spawn(async move {
        let mut stream = broker.accept().await;
        stream
            .read_connect_and_compare(
                Connect::<V>::try_new(true, 30, "client-id".to_string(), None, None, None)
                    .unwrap()
                    .into(),
            )
            .await;
        stream
            .write_packet(ConnAck::<V>::new(false, connack_rc))
            .await;

        stream
            .read_and_compare_msg(Subscribe::<V>::new(
                START_MID,
                vec![V::TopicSubscription::new(
                    "topic".to_string(),
                    Qos::ExactlyOnce,
                )],
            ))
            .await;
        send.send(()).await;
        stream
            .write_packet(SubAck::<V>::new(START_MID, sub_rc2))
            .await;
        let topic = MqttTopic::try_from("topic").unwrap();
        stream
            .write_packet(
                Publish::<V, Qos>::new(topic, b"test", Qos::ExactlyOnce, false)
                    .assign_packet_identifier(|| 42, false),
            )
            .await;
        stream.read_and_compare_msg(PubRec::<V>::new_ok(42)).await;
        stream.write_packet(PubRel::<V>::new_ok(42)).await;
        stream.read_and_compare_msg(PubComp::<V>::new_ok(42)).await;
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
    recv.recv().await;
    assert_eq!(suback, SubAck::<V>::new(START_MID, sub_rc));
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
    H::disconnect(client).await;
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

async fn test_sub_qos2_receive_packet_re_transmit<H, V, O>(
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
    let mut broker = H::broker().await;
    let addr = broker.addr();
    let (send, mut recv) = H::channel();
    let (pub_done_tx, mut pub_done_rx) = H::channel();
    let (tx_close, mut rx_close) = H::channel();

    let handle = H::spawn(async move {
        let mut stream = broker.accept().await;
        stream
            .read_connect_and_compare(
                Connect::<V>::try_new(true, 30, "client-id".to_string(), None, None, None)
                    .unwrap()
                    .into(),
            )
            .await;
        stream
            .write_packet(ConnAck::<V>::new(false, connack_rc))
            .await;

        stream
            .read_and_compare_msg(Subscribe::<V>::new(
                START_MID,
                vec![V::TopicSubscription::new(
                    "topic".to_string(),
                    Qos::ExactlyOnce,
                )],
            ))
            .await;
        send.send(()).await;
        stream
            .write_packet(SubAck::<V>::new(START_MID, sub_rc2))
            .await;
        let topic = MqttTopic::try_from("topic").unwrap();

        stream
            .write_packet(
                Publish::<V, Qos>::new(topic.clone(), b"test", Qos::ExactlyOnce, false)
                    .assign_packet_identifier(|| 42, false),
            )
            .await;
        stream
            .write_packet(
                Publish::<V, Qos>::new(topic.clone(), b"test", Qos::ExactlyOnce, false)
                    .assign_packet_identifier(|| 42, true),
            )
            .await;
        stream.read_and_compare_msg(PubRec::<V>::new_ok(42)).await;
        stream.read_and_compare_msg(PubRec::<V>::new_ok(42)).await;
        stream.write_packet(PubRel::<V>::new_ok(42)).await;
        stream.read_and_compare_msg(PubComp::<V>::new_ok(42)).await;
        stream
            .write_packet(
                Publish::<V, Qos>::new(topic, b"different", Qos::ExactlyOnce, false)
                    .assign_packet_identifier(|| 43, false),
            )
            .await;
        stream.read_and_compare_msg(PubRec::<V>::new_ok(43)).await;
        stream.write_packet(PubRel::<V>::new_ok(43)).await;
        stream.read_and_compare_msg(PubComp::<V>::new_ok(43)).await;
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
    recv.recv().await;
    assert_eq!(suback, SubAck::<V>::new(START_MID, sub_rc));
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
    let msg = H::recv(&mut stream).await;
    assert_eq!(
        msg,
        Publish::new(
            MqttTopic::try_from("topic").unwrap(),
            b"different",
            Qos::ExactlyOnce,
            false
        )
        .assign_packet_identifier(|| 43, false)
    );
    pub_done_rx.recv().await;
    H::disconnect(client).await;
    tx_close.send(()).await;
    handle.join().await;
}
test!(
    sub_qos2_receive_packet_re_transmit,
    test_sub_qos2_receive_packet_re_transmit,
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

async fn test_sub_qos2_retransmit_pubrel<H, V, O>(
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
    let mut broker = H::broker().await;
    let addr = broker.addr();
    let (send, mut recv) = H::channel();
    let (pub_done_tx, mut pub_done_rx) = H::channel();
    let (tx_close, mut rx_close) = H::channel();

    let handle = H::spawn(async move {
        let mut stream = broker.accept().await;
        stream
            .read_connect_and_compare(
                Connect::<V>::try_new(true, 30, "client-id".to_string(), None, None, None)
                    .unwrap()
                    .into(),
            )
            .await;
        stream
            .write_packet(ConnAck::<V>::new(false, connack_rc))
            .await;

        stream
            .read_and_compare_msg(Subscribe::<V>::new(
                START_MID,
                vec![V::TopicSubscription::new(
                    "topic".to_string(),
                    Qos::ExactlyOnce,
                )],
            ))
            .await;
        send.send(()).await;
        stream
            .write_packet(SubAck::<V>::new(START_MID, sub_rc2))
            .await;
        let topic = MqttTopic::try_from("topic").unwrap();

        stream
            .write_packet(
                Publish::<V, Qos>::new(topic.clone(), b"test", Qos::ExactlyOnce, false)
                    .assign_packet_identifier(|| 42, false),
            )
            .await;
        stream.read_and_compare_msg(PubRec::<V>::new_ok(42)).await;
        stream.write_packet(PubRel::<V>::new_ok(42)).await;
        stream.write_packet(PubRel::<V>::new_ok(42)).await;
        stream.read_and_compare_msg(PubComp::<V>::new_ok(42)).await;
        stream.read_and_compare_msg(PubComp::<V>::new_ok(42)).await;
        tracing::info!("ALL DONE");

        pub_done_tx.send(()).await;
        tracing::info!("Close");
        rx_close.recv().await;
        tracing::info!("Exit");
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
    recv.recv().await;
    assert_eq!(suback, SubAck::<V>::new(START_MID, sub_rc));
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
    H::disconnect(client).await;
    tx_close.send(()).await;
    handle.join().await;
}
test!(
    sub_qos2_retransmit_pubrel,
    test_sub_qos2_retransmit_pubrel,
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

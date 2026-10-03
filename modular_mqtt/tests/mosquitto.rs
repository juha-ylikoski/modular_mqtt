mod util;

use std::time::Duration;
use testcontainers::runners::SyncRunner;
use testcontainers_modules::mosquitto;

use modular_mqtt::{ClientOptsV3, ClientOptsV5, SyncClient};
use modular_mqtt_protocol::{
    MqttTopic, MqttV3_1_1, MqttV5_0_0, Publish, Qos, SubAck, SubAckDataV5, SubRcV3, SubRcV5,
    UnSubAckDataV5, UnsubAck,
};

struct MosquittoContainer {
    #[allow(unused)]
    instance: testcontainers::Container<mosquitto::Mosquitto>,
    url: String,
}

#[cfg(feature = "async")]
struct MosquittoContainerAsync {
    #[allow(unused)]
    instance: testcontainers::ContainerAsync<mosquitto::Mosquitto>,
    url: String,
}

fn init_mosquitto() -> MosquittoContainer {
    let mosquitto_instance = SyncRunner::start(mosquitto::Mosquitto::default()).unwrap();

    let broker_url = format!(
        "{}:{}",
        mosquitto_instance.get_host().unwrap(),
        mosquitto_instance.get_host_port_ipv4(1883).unwrap()
    );
    MosquittoContainer {
        instance: mosquitto_instance,
        url: broker_url,
    }
}

#[cfg(feature = "async")]
async fn init_mosquitto_async() -> MosquittoContainerAsync {
    use testcontainers::runners::AsyncRunner;

    let mosquitto_instance = AsyncRunner::start(mosquitto::Mosquitto::default())
        .await
        .unwrap();

    let broker_url = format!(
        "{}:{}",
        mosquitto_instance.get_host().await.unwrap(),
        mosquitto_instance.get_host_port_ipv4(1883).await.unwrap()
    );
    MosquittoContainerAsync {
        instance: mosquitto_instance,
        url: broker_url,
    }
}

fn mosquitto_publish_v3(
    opts1: ClientOptsV3,
    opts2: ClientOptsV3,
    qos: Qos,
    suback_data: Vec<SubRcV3>,
) {
    util::init_logging();
    let mosquitto = init_mosquitto();
    let addr = mosquitto.url;
    let (tx, rx) = std::sync::mpsc::channel();

    let t = std::thread::spawn(move || {
        let (_, client_w) = SyncClient::connect(opts1, addr.to_string()).unwrap();

        let (stream_r, client_r) = SyncClient::connect(opts2, addr.to_string()).unwrap();

        assert_eq!(
            client_r
                .subscribe(vec!["topic"], qos, Duration::from_secs(5))
                .unwrap(),
            SubAck::new(1, suback_data)
        );

        let inflight = client_w
            .publish(Publish::new(
                MqttTopic::try_from("topic").unwrap(),
                b"payload",
                qos,
                false,
            ))
            .unwrap();

        if qos == Qos::AtMostOnce {
            assert!(inflight.is_none());
        } else {
            assert!(inflight.is_some());
            let inflight = inflight.unwrap();
            inflight.wait_until_delivered();
        }

        assert_eq!(
            stream_r.recv().unwrap(),
            Publish::new(
                MqttTopic::try_from("topic").unwrap(),
                b"payload",
                qos,
                false
            )
            .assign_packet_identifier(|| 1, false)
        );
        client_w.disconnect().unwrap();
        client_r.disconnect().unwrap();
        tx.send(()).unwrap()
    });

    if rx.recv_timeout(Duration::from_secs(30)).is_err() {
        if t.is_finished() {
            t.join().unwrap();
        }
        panic!("Test timed out!");
    }
}

fn mosquitto_publish_v5(
    opts1: ClientOptsV5,
    opts2: ClientOptsV5,
    qos: Qos,
    suback_data: SubAckDataV5,
) {
    util::init_logging();
    let mosquitto = init_mosquitto();
    let addr = mosquitto.url;
    let (tx, rx) = std::sync::mpsc::channel();

    let t = std::thread::spawn(move || {
        let (_, client_w) = SyncClient::connect(opts1, addr.to_string()).unwrap();

        let (stream_r, client_r) = SyncClient::connect(opts2, addr.to_string()).unwrap();

        assert_eq!(
            client_r
                .subscribe(vec!["topic"], qos, Duration::from_secs(5))
                .unwrap(),
            SubAck::new(1, suback_data)
        );

        let inflight = client_w
            .publish(Publish::new(
                MqttTopic::try_from("topic").unwrap(),
                b"payload",
                qos,
                false,
            ))
            .unwrap();

        if qos == Qos::AtMostOnce {
            assert!(inflight.is_none());
        } else {
            assert!(inflight.is_some());
            let inflight = inflight.unwrap();
            inflight.wait_until_delivered();
        }

        assert_eq!(
            stream_r.recv().unwrap(),
            Publish::new(
                MqttTopic::try_from("topic").unwrap(),
                b"payload",
                qos,
                false
            )
            .assign_packet_identifier(|| 1, false)
        );
        client_w.disconnect().unwrap();
        client_r.disconnect().unwrap();
        tx.send(()).unwrap()
    });

    if rx.recv_timeout(Duration::from_secs(30)).is_err() {
        if t.is_finished() {
            t.join().unwrap();
        }
        panic!("Test timed out!");
    }
}

fn mosquitto_unsub_v3(opts1: ClientOptsV3, opts2: ClientOptsV3, suback_data: Vec<SubRcV3>) {
    util::init_logging();
    let qos = Qos::AtMostOnce;
    let mosquitto = init_mosquitto();
    let addr = mosquitto.url;
    let (tx, rx) = std::sync::mpsc::channel();

    let t = std::thread::spawn(move || {
        let (_, client_w) = SyncClient::connect(opts1, addr.to_string()).unwrap();

        let (stream_r, client_r) = SyncClient::connect(opts2, addr.to_string()).unwrap();

        assert_eq!(
            client_r
                .subscribe(vec!["topic"], qos, Duration::from_secs(5))
                .unwrap(),
            SubAck::new(1, suback_data)
        );

        assert_eq!(
            client_r
                .unsubscribe(vec!["topic".to_string()], Duration::from_secs(5))
                .unwrap(),
            UnsubAck::<MqttV3_1_1>::new(2, ())
        );

        assert!(client_w
            .publish(Publish::new(
                MqttTopic::try_from("topic").unwrap(),
                b"payload",
                qos,
                false
            ))
            .unwrap()
            .is_none());

        assert!(stream_r.recv_timeout(Duration::from_secs(1)).is_err());
        client_w.disconnect().unwrap();
        client_r.disconnect().unwrap();
        tx.send(()).unwrap()
    });

    if rx.recv_timeout(Duration::from_secs(10)).is_err() {
        if t.is_finished() {
            t.join().unwrap();
        }
        panic!("Test timed out!");
    }
}

fn mosquitto_unsub_v5(
    opts1: ClientOptsV5,
    opts2: ClientOptsV5,
    suback_data: SubAckDataV5,
    unsuback_data: UnSubAckDataV5,
) {
    util::init_logging();
    let qos = Qos::AtMostOnce;
    let mosquitto = init_mosquitto();
    let addr = mosquitto.url;
    let (tx, rx) = std::sync::mpsc::channel();

    let t = std::thread::spawn(move || {
        let (_, client_w) = SyncClient::connect(opts1, addr.to_string()).unwrap();

        let (stream_r, client_r) = SyncClient::connect(opts2, addr.to_string()).unwrap();

        assert_eq!(
            client_r
                .subscribe(vec!["topic"], qos, Duration::from_secs(5))
                .unwrap(),
            SubAck::new(1, suback_data)
        );

        assert_eq!(
            client_r
                .unsubscribe(vec!["topic".to_string()], Duration::from_secs(5))
                .unwrap(),
            UnsubAck::<MqttV5_0_0>::new(2, unsuback_data)
        );

        assert!(client_w
            .publish(Publish::new(
                MqttTopic::try_from("topic").unwrap(),
                b"payload",
                qos,
                false
            ))
            .unwrap()
            .is_none());

        assert!(stream_r.recv_timeout(Duration::from_secs(1)).is_err());
        client_w.disconnect().unwrap();
        client_r.disconnect().unwrap();
        tx.send(()).unwrap()
    });

    if rx.recv_timeout(Duration::from_secs(10)).is_err() {
        if t.is_finished() {
            t.join().unwrap();
        }
        panic!("Test timed out!");
    }
}

mod sync_client {
    use super::*;

    mod v3 {

        use modular_mqtt::ClientOptsV3;

        use super::*;

        #[test]
        fn mosquitto_publish_qos0() {
            mosquitto_publish_v3(
                ClientOptsV3 {
                    client_id: "client-id-1".to_string(),
                    keep_alive: 1,
                    ..Default::default()
                },
                ClientOptsV3 {
                    client_id: "client-id-2".to_string(),
                    keep_alive: 1,
                    ..Default::default()
                },
                Qos::AtMostOnce,
                vec![SubRcV3::SuccessQos0],
            );
        }
        #[test]
        fn mosquitto_publish_qos1() {
            mosquitto_publish_v3(
                ClientOptsV3 {
                    client_id: "client-id-1".to_string(),
                    keep_alive: 1,
                    ..Default::default()
                },
                ClientOptsV3 {
                    client_id: "client-id-2".to_string(),
                    keep_alive: 1,
                    ..Default::default()
                },
                Qos::AtLeastOnce,
                vec![SubRcV3::SuccessQos1],
            );
        }
        #[test]
        fn mosquitto_publish_qos2() {
            mosquitto_publish_v3(
                ClientOptsV3 {
                    client_id: "client-id-1".to_string(),
                    keep_alive: 1,
                    ..Default::default()
                },
                ClientOptsV3 {
                    client_id: "client-id-2".to_string(),
                    keep_alive: 1,
                    ..Default::default()
                },
                Qos::ExactlyOnce,
                vec![SubRcV3::SuccessQos2],
            );
        }

        #[test]
        fn mosquitto_unsub() {
            super::mosquitto_unsub_v3(
                ClientOptsV3 {
                    client_id: "client-id-1".to_string(),
                    keep_alive: 1,
                    ..Default::default()
                },
                ClientOptsV3 {
                    client_id: "client-id-2".to_string(),
                    keep_alive: 1,
                    ..Default::default()
                },
                vec![SubRcV3::SuccessQos0],
            );
        }
    }

    mod v5 {

        use modular_mqtt::ClientOptsV5;
        use modular_mqtt_protocol::{SubAckDataV5, UnSubAckDataV5, UnsubAckReasonCode};

        use super::*;

        #[test]
        fn mosquitto_publish_qos0() {
            mosquitto_publish_v5(
                ClientOptsV5 {
                    client_id: "client-id-1".to_string(),
                    keep_alive: 1,
                    ..Default::default()
                },
                ClientOptsV5 {
                    client_id: "client-id-2".to_string(),
                    keep_alive: 1,
                    ..Default::default()
                },
                Qos::AtMostOnce,
                SubAckDataV5 {
                    return_codes: vec![SubRcV5::SuccessQos0],
                    reason: None,
                    user_property: Vec::new(),
                },
            );
        }
        #[test]
        fn mosquitto_publish_qos1() {
            mosquitto_publish_v5(
                ClientOptsV5 {
                    client_id: "client-id-1".to_string(),
                    keep_alive: 1,
                    ..Default::default()
                },
                ClientOptsV5 {
                    client_id: "client-id-2".to_string(),
                    keep_alive: 1,
                    ..Default::default()
                },
                Qos::AtLeastOnce,
                SubAckDataV5 {
                    return_codes: vec![SubRcV5::SuccessQos1],
                    reason: None,
                    user_property: Vec::new(),
                },
            );
        }
        #[test]
        fn mosquitto_publish_qos2() {
            mosquitto_publish_v5(
                ClientOptsV5 {
                    client_id: "client-id-1".to_string(),
                    keep_alive: 1,
                    ..Default::default()
                },
                ClientOptsV5 {
                    client_id: "client-id-2".to_string(),
                    keep_alive: 1,
                    ..Default::default()
                },
                Qos::ExactlyOnce,
                SubAckDataV5 {
                    return_codes: vec![SubRcV5::SuccessQos2],
                    reason: None,
                    user_property: Vec::new(),
                },
            );
        }

        #[test]
        fn mosquitto_unsub() {
            super::mosquitto_unsub_v5(
                ClientOptsV5 {
                    client_id: "client-id-1".to_string(),
                    keep_alive: 1,
                    ..Default::default()
                },
                ClientOptsV5 {
                    client_id: "client-id-2".to_string(),
                    keep_alive: 1,
                    ..Default::default()
                },
                SubAckDataV5 {
                    return_codes: vec![SubRcV5::SuccessQos0],
                    reason: None,
                    user_property: Vec::new(),
                },
                UnSubAckDataV5 {
                    reason_code: vec![UnsubAckReasonCode::Success],
                    reason: None,
                    user_property: Vec::new(),
                },
            );
        }
    }
}

#[cfg(feature = "async")]
mod async_client {

    use std::time::Duration;

    use modular_mqtt::{AsyncClient, ClientOptsV3, ClientOptsV5};
    use modular_mqtt_protocol::{
        MqttTopic, MqttV3_1_1, MqttV5_0_0, Publish, Qos, SubAck, SubAckDataV5, SubRcV3, SubRcV5,
        UnSubAckDataV5, UnsubAck,
    };

    use crate::{init_mosquitto_async, util};

    async fn mosquitto_publish_v3(
        opts1: ClientOptsV3,
        opts2: ClientOptsV3,
        qos: Qos,
        suback_data: Vec<SubRcV3>,
    ) {
        util::init_logging();
        let mosquitto = init_mosquitto_async().await;
        let addr = mosquitto.url;
        let (tx, mut rx) = tokio::sync::mpsc::channel(100);

        let t = tokio::task::spawn(async move {
            let (_, client_w) = AsyncClient::connect(opts1, addr.to_string()).await.unwrap();

            let (mut stream_r, client_r) =
                AsyncClient::connect(opts2, addr.to_string()).await.unwrap();

            assert_eq!(
                client_r.subscribe(vec!["topic"], qos).await.unwrap(),
                SubAck::new(1, suback_data)
            );

            let inflight = client_w
                .publish(Publish::new(
                    MqttTopic::try_from("topic").unwrap(),
                    b"payload",
                    qos,
                    false,
                ))
                .await
                .unwrap();

            if qos == Qos::AtMostOnce {
                assert!(inflight.is_none());
            } else {
                assert!(inflight.is_some());
                let inflight = inflight.unwrap();
                inflight.wait_until_delivered().await;
            }

            assert_eq!(
                stream_r.recv().await.unwrap(),
                Publish::new(
                    MqttTopic::try_from("topic").unwrap(),
                    b"payload",
                    qos,
                    false
                )
                .assign_packet_identifier(|| 1, false)
            );
            client_w.disconnect().await.unwrap();
            client_r.disconnect().await.unwrap();
            tx.send(()).await.unwrap()
        });

        match tokio::time::timeout(Duration::from_secs(30), rx.recv()).await {
            Ok(Some(_)) => t.await.unwrap(),
            Ok(None) => panic!("Channel died"),
            Err(_) => panic!("Timed out!"),
        };
    }

    async fn mosquitto_publish_v5(
        opts1: ClientOptsV5,
        opts2: ClientOptsV5,
        qos: Qos,
        suback_data: SubAckDataV5,
    ) {
        util::init_logging();
        let mosquitto = init_mosquitto_async().await;
        let addr = mosquitto.url;
        let (tx, mut rx) = tokio::sync::mpsc::channel(10);

        let t = tokio::task::spawn(async move {
            let (_, client_w) = AsyncClient::connect(opts1, addr.to_string()).await.unwrap();

            let (mut stream_r, client_r) =
                AsyncClient::connect(opts2, addr.to_string()).await.unwrap();

            assert_eq!(
                client_r.subscribe(vec!["topic"], qos).await.unwrap(),
                SubAck::new(1, suback_data)
            );

            let inflight = client_w
                .publish(Publish::new(
                    MqttTopic::try_from("topic").unwrap(),
                    b"payload",
                    qos,
                    false,
                ))
                .await
                .unwrap();

            if qos == Qos::AtMostOnce {
                assert!(inflight.is_none());
            } else {
                assert!(inflight.is_some());
                let inflight = inflight.unwrap();
                inflight.wait_until_delivered().await;
            }

            assert_eq!(
                stream_r.recv().await.unwrap(),
                Publish::new(
                    MqttTopic::try_from("topic").unwrap(),
                    b"payload",
                    qos,
                    false
                )
                .assign_packet_identifier(|| 1, false)
            );
            client_w.disconnect().await.unwrap();
            client_r.disconnect().await.unwrap();
            tx.send(()).await.unwrap()
        });

        match tokio::time::timeout(Duration::from_secs(30), rx.recv()).await {
            Ok(Some(_)) => t.await.unwrap(),
            Ok(None) => panic!("Channel died"),
            Err(_) => panic!("Timed out!"),
        };
    }

    async fn mosquitto_unsub_v3(
        opts1: ClientOptsV3,
        opts2: ClientOptsV3,
        suback_data: Vec<SubRcV3>,
    ) {
        util::init_logging();
        let qos = Qos::AtMostOnce;
        let mosquitto = init_mosquitto_async().await;
        let addr = mosquitto.url;
        let (tx, mut rx) = tokio::sync::mpsc::channel(10);

        let t = tokio::task::spawn(async move {
            let (_, client_w) = AsyncClient::connect(opts1, addr.to_string()).await.unwrap();

            let (mut stream_r, client_r) =
                AsyncClient::connect(opts2, addr.to_string()).await.unwrap();

            assert_eq!(
                client_r.subscribe(vec!["topic"], qos,).await.unwrap(),
                SubAck::new(1, suback_data)
            );

            assert_eq!(
                client_r
                    .unsubscribe(vec!["topic".to_string()],)
                    .await
                    .unwrap(),
                UnsubAck::<MqttV3_1_1>::new(2, ())
            );

            assert!(client_w
                .publish(Publish::new(
                    MqttTopic::try_from("topic").unwrap(),
                    b"payload",
                    qos,
                    false
                ))
                .await
                .unwrap()
                .is_none());

            assert!(
                tokio::time::timeout(Duration::from_secs(1), stream_r.recv())
                    .await
                    .is_err()
            );
            client_w.disconnect().await.unwrap();
            client_r.disconnect().await.unwrap();
            tx.send(()).await.unwrap()
        });

        match tokio::time::timeout(Duration::from_secs(30), rx.recv()).await {
            Ok(Some(_)) => t.await.unwrap(),
            Ok(None) => panic!("Channel died"),
            Err(_) => panic!("Timed out!"),
        };
    }

    async fn mosquitto_unsub_v5(
        opts1: ClientOptsV5,
        opts2: ClientOptsV5,
        suback_data: SubAckDataV5,
        unsuback_data: UnSubAckDataV5,
    ) {
        util::init_logging();
        let qos = Qos::AtMostOnce;
        let mosquitto = init_mosquitto_async().await;
        let addr = mosquitto.url;
        let (tx, mut rx) = tokio::sync::mpsc::channel(10);

        let t = tokio::task::spawn(async move {
            let (_, client_w) = AsyncClient::connect(opts1, addr.to_string()).await.unwrap();

            let (mut stream_r, client_r) =
                AsyncClient::connect(opts2, addr.to_string()).await.unwrap();

            assert_eq!(
                client_r.subscribe(vec!["topic"], qos,).await.unwrap(),
                SubAck::new(1, suback_data)
            );

            assert_eq!(
                client_r
                    .unsubscribe(vec!["topic".to_string()],)
                    .await
                    .unwrap(),
                UnsubAck::<MqttV5_0_0>::new(2, unsuback_data)
            );

            assert!(client_w
                .publish(Publish::new(
                    MqttTopic::try_from("topic").unwrap(),
                    b"payload",
                    qos,
                    false
                ))
                .await
                .unwrap()
                .is_none());

            assert!(
                tokio::time::timeout(Duration::from_secs(1), stream_r.recv())
                    .await
                    .is_err()
            );
            client_w.disconnect().await.unwrap();
            client_r.disconnect().await.unwrap();
            tx.send(()).await.unwrap()
        });

        match tokio::time::timeout(Duration::from_secs(30), rx.recv()).await {
            Ok(Some(_)) => t.await.unwrap(),
            Ok(None) => panic!("Channel died"),
            Err(_) => panic!("Timed out!"),
        };
    }

    mod v3 {

        use modular_mqtt::ClientOptsV3;

        use super::*;

        #[tokio::test]
        async fn mosquitto_publish_qos0() {
            mosquitto_publish_v3(
                ClientOptsV3 {
                    client_id: "client-id-1".to_string(),
                    keep_alive: 1,
                    ..Default::default()
                },
                ClientOptsV3 {
                    client_id: "client-id-2".to_string(),
                    keep_alive: 1,
                    ..Default::default()
                },
                Qos::AtMostOnce,
                vec![SubRcV3::SuccessQos0],
            )
            .await;
        }
        #[tokio::test]
        async fn mosquitto_publish_qos1() {
            mosquitto_publish_v3(
                ClientOptsV3 {
                    client_id: "client-id-1".to_string(),
                    keep_alive: 1,
                    ..Default::default()
                },
                ClientOptsV3 {
                    client_id: "client-id-2".to_string(),
                    keep_alive: 1,
                    ..Default::default()
                },
                Qos::AtLeastOnce,
                vec![SubRcV3::SuccessQos1],
            )
            .await;
        }
        #[tokio::test]
        async fn mosquitto_publish_qos2() {
            mosquitto_publish_v3(
                ClientOptsV3 {
                    client_id: "client-id-1".to_string(),
                    keep_alive: 1,
                    ..Default::default()
                },
                ClientOptsV3 {
                    client_id: "client-id-2".to_string(),
                    keep_alive: 1,
                    ..Default::default()
                },
                Qos::ExactlyOnce,
                vec![SubRcV3::SuccessQos2],
            )
            .await;
        }

        #[tokio::test]
        async fn mosquitto_unsub() {
            super::mosquitto_unsub_v3(
                ClientOptsV3 {
                    client_id: "client-id-1".to_string(),
                    keep_alive: 1,
                    ..Default::default()
                },
                ClientOptsV3 {
                    client_id: "client-id-2".to_string(),
                    keep_alive: 1,
                    ..Default::default()
                },
                vec![SubRcV3::SuccessQos0],
            )
            .await;
        }
    }

    mod v5 {

        use modular_mqtt::ClientOptsV5;
        use modular_mqtt_protocol::{SubAckDataV5, UnSubAckDataV5, UnsubAckReasonCode};

        use super::*;

        #[tokio::test]
        async fn mosquitto_publish_qos0() {
            mosquitto_publish_v5(
                ClientOptsV5 {
                    client_id: "client-id-1".to_string(),
                    keep_alive: 1,
                    ..Default::default()
                },
                ClientOptsV5 {
                    client_id: "client-id-2".to_string(),
                    keep_alive: 1,
                    ..Default::default()
                },
                Qos::AtMostOnce,
                SubAckDataV5 {
                    return_codes: vec![SubRcV5::SuccessQos0],
                    reason: None,
                    user_property: Vec::new(),
                },
            )
            .await;
        }
        #[tokio::test]
        async fn mosquitto_publish_qos1() {
            mosquitto_publish_v5(
                ClientOptsV5 {
                    client_id: "client-id-1".to_string(),
                    keep_alive: 1,
                    ..Default::default()
                },
                ClientOptsV5 {
                    client_id: "client-id-2".to_string(),
                    keep_alive: 1,
                    ..Default::default()
                },
                Qos::AtLeastOnce,
                SubAckDataV5 {
                    return_codes: vec![SubRcV5::SuccessQos1],
                    reason: None,
                    user_property: Vec::new(),
                },
            )
            .await;
        }
        #[tokio::test]
        async fn mosquitto_publish_qos2() {
            mosquitto_publish_v5(
                ClientOptsV5 {
                    client_id: "client-id-1".to_string(),
                    keep_alive: 1,
                    ..Default::default()
                },
                ClientOptsV5 {
                    client_id: "client-id-2".to_string(),
                    keep_alive: 1,
                    ..Default::default()
                },
                Qos::ExactlyOnce,
                SubAckDataV5 {
                    return_codes: vec![SubRcV5::SuccessQos2],
                    reason: None,
                    user_property: Vec::new(),
                },
            )
            .await;
        }

        #[tokio::test]
        async fn mosquitto_unsub() {
            super::mosquitto_unsub_v5(
                ClientOptsV5 {
                    client_id: "client-id-1".to_string(),
                    keep_alive: 1,
                    ..Default::default()
                },
                ClientOptsV5 {
                    client_id: "client-id-2".to_string(),
                    keep_alive: 1,
                    ..Default::default()
                },
                SubAckDataV5 {
                    return_codes: vec![SubRcV5::SuccessQos0],
                    reason: None,
                    user_property: Vec::new(),
                },
                UnSubAckDataV5 {
                    reason_code: vec![UnsubAckReasonCode::Success],
                    reason: None,
                    user_property: Vec::new(),
                },
            )
            .await;
        }
    }
}

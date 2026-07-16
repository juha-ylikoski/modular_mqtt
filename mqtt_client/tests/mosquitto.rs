mod util;

use std::time::Duration;
use testcontainers::runners::SyncRunner;
use testcontainers_modules::mosquitto;

use mqtt_client::{
    client::{MqttClient, SyncClient},
    client_opts::ClientOpts,
    util::IntoTopicSubscription,
};
use rust_mqtt_protocol::{
    MqttTopic, MqttVersion, Publish, Qos, SubAck, SubRcV3, SubRcV5, UnsubAck,
};

struct MosquittoContainer {
    #[allow(unused)]
    instance: testcontainers::Container<mosquitto::Mosquitto>,
    url: String,
}

fn init_mosquitto() -> MosquittoContainer {
    let mosquitto_instance = mosquitto::Mosquitto::default().start().unwrap();

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

fn mosquitto_publish<V>(
    opts: ClientOpts<V>,
    opts2: ClientOpts<V>,
    qos: Qos,
    suback_data: V::SubAckData,
) where
    V: MqttVersion,
    SyncClient<V>: MqttClient<V>,
    rust_mqtt_protocol::MqttTopic: IntoTopicSubscription<V>,
{
    util::init_logging();
    let mosquitto = init_mosquitto();
    let addr = mosquitto.url;
    let (tx, rx) = std::sync::mpsc::channel();

    let t = std::thread::spawn(move || {
        let client_w = SyncClient::connect_tcp(opts, addr.to_string()).unwrap();

        let client_r = SyncClient::connect_tcp(opts2, addr.to_string()).unwrap();

        let r_stream = client_r.stream();
        assert_eq!(
            client_r
                .subscribe(vec![MqttTopic::try_from("topic").unwrap()], qos)
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
            r_stream.recv().unwrap(),
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

    if rx.recv_timeout(Duration::from_secs(10)).is_err() {
        if t.is_finished() {
            t.join().unwrap();
        }
        panic!("Test timed out!");
    }
}

fn mosquitto_unsub<V>(
    opts: ClientOpts<V>,
    opts2: ClientOpts<V>,
    suback_data: V::SubAckData,
    unsuback_data: V::UnSubAckProperties,
) where
    V: MqttVersion,
    SyncClient<V>: MqttClient<V>,
    rust_mqtt_protocol::MqttTopic: IntoTopicSubscription<V>,
{
    util::init_logging();
    let qos = Qos::AtMostOnce;
    let mosquitto = init_mosquitto();
    let addr = mosquitto.url;
    let (tx, rx) = std::sync::mpsc::channel();

    let t = std::thread::spawn(move || {
        let client_w = SyncClient::connect_tcp(opts, addr.to_string()).unwrap();

        let client_r = SyncClient::connect_tcp(opts2, addr.to_string()).unwrap();

        let r_stream = client_r.stream();
        assert_eq!(
            client_r
                .subscribe(vec![MqttTopic::try_from("topic").unwrap()], qos)
                .unwrap(),
            SubAck::new(1, suback_data)
        );

        assert_eq!(
            client_r
                .unsubscribe(vec!["topic".try_into().unwrap()])
                .unwrap(),
            UnsubAck::<V>::new(2, unsuback_data)
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

        assert!(r_stream.recv_timeout(Duration::from_secs(1)).is_err());
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

mod v3 {

    use rust_mqtt_protocol::MqttV3_1_1;

    use super::*;

    #[test]
    fn mosquitto_publish_qos0() {
        mosquitto_publish::<MqttV3_1_1>(
            ClientOpts {
                client_id: "client-id-1".to_string(),
                keep_alive: 1,
                ..Default::default()
            },
            ClientOpts {
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
        mosquitto_publish::<MqttV3_1_1>(
            ClientOpts {
                client_id: "client-id-1".to_string(),
                keep_alive: 1,
                ..Default::default()
            },
            ClientOpts {
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
        mosquitto_publish::<MqttV3_1_1>(
            ClientOpts {
                client_id: "client-id-1".to_string(),
                keep_alive: 1,
                ..Default::default()
            },
            ClientOpts {
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
        super::mosquitto_unsub::<MqttV3_1_1>(
            ClientOpts {
                client_id: "client-id-1".to_string(),
                keep_alive: 1,
                ..Default::default()
            },
            ClientOpts {
                client_id: "client-id-2".to_string(),
                keep_alive: 1,
                ..Default::default()
            },
            vec![SubRcV3::SuccessQos0],
            (),
        );
    }
}

mod v5 {

    use rust_mqtt_protocol::{MqttV5_0_0, SubAckDataV5, UnSubAckDataV5, UnsubAckReasonCode};

    use super::*;

    #[test]
    fn mosquitto_publish_qos0() {
        mosquitto_publish::<MqttV5_0_0>(
            ClientOpts {
                client_id: "client-id-1".to_string(),
                keep_alive: 1,
                ..Default::default()
            },
            ClientOpts {
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
        mosquitto_publish::<MqttV5_0_0>(
            ClientOpts {
                client_id: "client-id-1".to_string(),
                keep_alive: 1,
                ..Default::default()
            },
            ClientOpts {
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
        mosquitto_publish::<MqttV5_0_0>(
            ClientOpts {
                client_id: "client-id-1".to_string(),
                keep_alive: 1,
                ..Default::default()
            },
            ClientOpts {
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
        super::mosquitto_unsub::<MqttV5_0_0>(
            ClientOpts {
                client_id: "client-id-1".to_string(),
                keep_alive: 1,
                ..Default::default()
            },
            ClientOpts {
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

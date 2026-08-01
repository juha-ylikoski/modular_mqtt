mod util;

use std::time::Duration;
use testcontainers::runners::SyncRunner;
use testcontainers_modules::mosquitto;

use modular_mqtt::{
    client::{Client, MqttClient},
    client_opts::{ClientOpts, MqttOptions},
    connection::SyncWriter,
    util::IntoTopicSubscription,
};
use modular_mqtt_protocol::{
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

fn mosquitto_publish<V>(qos: Qos, suback_data: V::SubAckData)
where
    V: MqttVersion + MqttOptions,
    Client<V, SyncWriter>: MqttClient<V>,
    modular_mqtt_protocol::MqttTopic: IntoTopicSubscription<V>,
    ClientOpts<V>: Default,
{
    util::init_logging();
    let mosquitto = init_mosquitto();
    let addr = mosquitto.url;
    let (tx, rx) = std::sync::mpsc::channel();

    let t = std::thread::spawn(move || {
        let (_, client_w) = Client::<V, SyncWriter>::connect_tcp(
            ClientOpts {
                client_id: "client-id-1".to_string(),
                keep_alive: 1,
                ..Default::default()
            },
            addr.to_string(),
        )
        .unwrap();

        let (stream_r, client_r) = Client::<V, SyncWriter>::connect_tcp(
            ClientOpts {
                client_id: "client-id-2".to_string(),
                keep_alive: 1,
                ..Default::default()
            },
            addr.to_string(),
        )
        .unwrap();

        assert_eq!(
            client_r
                .subscribe(
                    vec![MqttTopic::try_from("topic").unwrap()],
                    qos,
                    Duration::from_secs(5)
                )
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

fn mosquitto_unsub<V>(suback_data: V::SubAckData, unsuback_data: V::UnSubAckProperties)
where
    V: MqttVersion + MqttOptions,
    Client<V, SyncWriter>: MqttClient<V>,
    modular_mqtt_protocol::MqttTopic: IntoTopicSubscription<V>,
    ClientOpts<V>: Default,
{
    util::init_logging();
    let qos = Qos::AtMostOnce;
    let mosquitto = init_mosquitto();
    let addr = mosquitto.url;
    let (tx, rx) = std::sync::mpsc::channel();

    let t = std::thread::spawn(move || {
        let (_, client_w) = Client::<V, SyncWriter>::connect_tcp(
            ClientOpts {
                client_id: "client-id-1".to_string(),
                keep_alive: 1,
                ..Default::default()
            },
            addr.to_string(),
        )
        .unwrap();

        let (stream_r, client_r) = Client::<V, SyncWriter>::connect_tcp(
            ClientOpts {
                client_id: "client-id-2".to_string(),
                keep_alive: 1,
                ..Default::default()
            },
            addr.to_string(),
        )
        .unwrap();

        assert_eq!(
            client_r
                .subscribe(
                    vec![MqttTopic::try_from("topic").unwrap()],
                    qos,
                    Duration::from_secs(5)
                )
                .unwrap(),
            SubAck::new(1, suback_data)
        );

        assert_eq!(
            client_r
                .unsubscribe(vec!["topic".try_into().unwrap()], Duration::from_secs(5))
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

mod v3 {

    use modular_mqtt_protocol::MqttV3_1_1;

    use super::*;

    #[test]
    fn mosquitto_publish_qos0() {
        mosquitto_publish::<MqttV3_1_1>(Qos::AtMostOnce, vec![SubRcV3::SuccessQos0]);
    }
    #[test]
    fn mosquitto_publish_qos1() {
        mosquitto_publish::<MqttV3_1_1>(Qos::AtLeastOnce, vec![SubRcV3::SuccessQos1]);
    }
    #[test]
    fn mosquitto_publish_qos2() {
        mosquitto_publish::<MqttV3_1_1>(Qos::ExactlyOnce, vec![SubRcV3::SuccessQos2]);
    }

    #[test]
    fn mosquitto_unsub() {
        super::mosquitto_unsub::<MqttV3_1_1>(vec![SubRcV3::SuccessQos0], ());
    }
}

mod v5 {

    use modular_mqtt_protocol::{MqttV5_0_0, SubAckDataV5, UnSubAckDataV5, UnsubAckReasonCode};

    use super::*;

    #[test]
    fn mosquitto_publish_qos0() {
        mosquitto_publish::<MqttV5_0_0>(
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

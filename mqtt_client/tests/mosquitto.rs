mod util;

use std::time::Duration;
use testcontainers::runners::SyncRunner;
use testcontainers_modules::mosquitto;

use mqtt_client::{
    client::SyncClient,
    client_opts::{ClientOpts, OnDisconnectBehavior},
    util::Message,
};
use rust_mqtt_protocol::{
    ControlPacketType, MqttTopic, Qos, ReceivedMessage, SubAck, SubRc, UnsubscribeAck,
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

fn mosquitto_publish(qos: Qos) {
    util::init_logging();
    let mosquitto = init_mosquitto();
    let addr = mosquitto.url;
    let (tx, rx) = std::sync::mpsc::channel();

    let t = std::thread::spawn(move || {
        let client_w = SyncClient::connect_tcp(
            ClientOpts {
                client_id: "client-id-1".to_string(),
                keep_alive: 1,
                clean_session: true,
                will: None,
                username: None,
                password: None,
                on_disconnect: OnDisconnectBehavior::Panic,
            },
            addr.to_string(),
        )
        .unwrap();

        let client_r = SyncClient::connect_tcp(
            ClientOpts {
                client_id: "client-id-2".to_string(),
                keep_alive: 1,
                clean_session: true,
                will: None,
                username: None,
                password: None,
                on_disconnect: OnDisconnectBehavior::Panic,
            },
            addr.to_string(),
        )
        .unwrap();

        let r_stream = client_r.stream();
        assert_eq!(
            client_r
                .subscribe(vec!["topic".try_into().unwrap()], qos)
                .unwrap(),
            SubAck::new(
                1,
                vec![match qos {
                    Qos::AtMostOnce => SubRc::SuccessQos0,
                    Qos::AtLeastOnce => SubRc::SuccessQos1,
                    Qos::ExactlyOnce => SubRc::SuccessQos2,
                }]
            )
        );

        let inflight = client_w
            .publish(Message::new(
                MqttTopic::try_from("topic").unwrap(),
                b"payload",
                qos,
            ))
            .unwrap();

        if qos == Qos::AtMostOnce {
            assert!(inflight.is_none());
        } else {
            assert!(inflight.is_some());
            let inflight = inflight.unwrap();
            inflight.wait_until_delivered();
        }

        let packet_identifier = if qos == Qos::AtMostOnce {
            None
        } else {
            // Mosquitto always returns 1 as first packet identifier
            Some(1)
        };

        assert_eq!(
            r_stream.recv().unwrap(),
            ReceivedMessage {
                flags: ControlPacketType::Publish {
                    dup: false,
                    qos,
                    retain: false
                }
                .flags(),
                topic: "topic".to_string(),
                packet_identifier,
                payload: b"payload".to_vec()
            }
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

#[test]
fn mosquitto_publish_qos0() {
    mosquitto_publish(Qos::AtMostOnce);
}
#[test]
fn mosquitto_publish_qos1() {
    mosquitto_publish(Qos::AtLeastOnce);
}
#[test]
fn mosquitto_publish_qos2() {
    mosquitto_publish(Qos::ExactlyOnce);
}

#[test]
fn mosquitto_unsub() {
    util::init_logging();
    let qos = Qos::AtMostOnce;
    let mosquitto = init_mosquitto();
    let addr = mosquitto.url;
    let (tx, rx) = std::sync::mpsc::channel();

    let t = std::thread::spawn(move || {
        let client_w = SyncClient::connect_tcp(
            ClientOpts {
                client_id: "client-id-1".to_string(),
                keep_alive: 1,
                clean_session: true,
                will: None,
                username: None,
                password: None,
                on_disconnect: OnDisconnectBehavior::Panic,
            },
            addr.to_string(),
        )
        .unwrap();

        let client_r = SyncClient::connect_tcp(
            ClientOpts {
                client_id: "client-id-2".to_string(),
                keep_alive: 1,
                clean_session: true,
                will: None,
                username: None,
                password: None,
                on_disconnect: OnDisconnectBehavior::Panic,
            },
            addr.to_string(),
        )
        .unwrap();

        let r_stream = client_r.stream();
        assert_eq!(
            client_r
                .subscribe(vec!["topic".try_into().unwrap()], qos)
                .unwrap(),
            SubAck::new(1, vec![SubRc::SuccessQos0,])
        );

        assert_eq!(
            client_r
                .unsubscribe(vec!["topic".try_into().unwrap()])
                .unwrap(),
            UnsubscribeAck::new(2)
        );

        assert!(client_w
            .publish(Message::new(
                MqttTopic::try_from("topic").unwrap(),
                b"payload",
                qos,
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

use mqtt_client::{client::Client, client_opts::ClientOpts, connection::SyncWriter};
use rust_mqtt_protocol::{MqttV3_1_1, Publish, Qos};
use tracing::{dispatcher::set_global_default, Level};

fn main() {
    let collector = tracing_subscriber::fmt::fmt()
        .with_max_level(Level::TRACE)
        .finish();
    set_global_default(collector.into()).unwrap();

    let (_recv_stream, client) = Client::<MqttV3_1_1, SyncWriter>::connect_tcp(
        ClientOpts {
            client_id: "client-id-pub".to_string(),
            ..Default::default()
        },
        "127.0.0.1:1883".to_string(),
    )
    .expect("Connection failed");

    tracing::info!("Publish with qos=0");
    assert!(client
        .publish(Publish::new(
            "qos0".try_into().unwrap(),
            b"maybe this will be received",
            Qos::AtMostOnce,
            false
        ))
        .unwrap()
        .is_none());
    tracing::info!("Published!");

    tracing::info!("Publish with qos=1");
    let inflight = client
        .publish(Publish::new(
            "qos1".try_into().unwrap(),
            b"This will be received",
            Qos::AtLeastOnce,
            false,
        ))
        .unwrap()
        .unwrap();
    tracing::info!("Published!");
    inflight.wait_until_delivered();
    tracing::info!("Broker received!");

    tracing::info!("Publish with qos=2");
    let inflight = client
        .publish(Publish::new(
            "qos2".try_into().unwrap(),
            b"This will be received exactly once",
            Qos::ExactlyOnce,
            false,
        ))
        .unwrap()
        .unwrap();
    tracing::info!("Published!");
    inflight.wait_until_delivered();
    tracing::info!("Broker received!");
    client.disconnect().unwrap();
}

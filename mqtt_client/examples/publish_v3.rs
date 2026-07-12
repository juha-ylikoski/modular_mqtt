use mqtt_client::{client::SyncClient, client_opts::ClientOpts};
use rust_mqtt_protocol::{Publish, Qos};
use tracing::{dispatcher::set_global_default, Level};

#[tokio::main()]
async fn main() {
    let collector = tracing_subscriber::fmt::fmt()
        .with_max_level(Level::TRACE)
        .finish();
    set_global_default(collector.into()).unwrap();

    let client = SyncClient::connect_tcp(
        ClientOpts {
            client_id: "client-id-pub".to_string(),
            keep_alive: 15,
            clean_session: true,
            will: None,
            username: None,
            password: None,
            on_disconnect: mqtt_client::client_opts::OnDisconnectBehavior::Panic,
            max_packet_size: rust_mqtt_protocol::MAX_MQTT_PACKET_SIZE,
        },
        "127.0.0.1:1883".to_string(),
    )
    .expect("Connection failed");

    tracing::info!("Publish with qos=0");
    assert!(client
        .publish(Publish::new_v3(
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
        .publish(Publish::new_v3(
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
        .publish(Publish::new_v3(
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

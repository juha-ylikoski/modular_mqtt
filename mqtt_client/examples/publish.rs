use mqtt_client::{
    client::{MqttClient, SyncClient},
    client_opts::ClientOpts,
    util::Message,
};
use rust_mqtt_protocol::Qos;
use tracing::{dispatcher::set_global_default, Level};

#[tokio::main()]
async fn main() {
    let collector = tracing_subscriber::fmt::fmt()
        .with_max_level(Level::TRACE)
        .finish();
    set_global_default(collector.into()).unwrap();

    let mut client = SyncClient::connect(ClientOpts {
        broker: "127.0.0.1:1883".to_string(),
        client_id: "client-id".to_string(),
        keep_alive: 15,
        clean_session: true,
        will: None,
        username: None,
        password: None,
    })
    .expect("Connection failed");

    tracing::info!("Publish with qos=0");
    assert!(client
        .publish(Message::new(
            "qos0".try_into().unwrap(),
            b"maybe this will be received",
            Qos::AtMostOnce,
        ))
        .unwrap()
        .is_none());
    tracing::info!("Published!");

    tracing::info!("Publish with qos=1");
    let inflight = client
        .publish(Message::new(
            "qos1".try_into().unwrap(),
            b"This will be received",
            Qos::AtLeastOnce,
        ))
        .unwrap()
        .unwrap();
    tracing::info!("Published!");
    inflight.wait_until_delivered();
    tracing::info!("Broker received!");

    tracing::info!("Publish with qos=2");
    let inflight = client
        .publish(Message::new(
            "qos2".try_into().unwrap(),
            b"This will be received exactly once",
            Qos::ExactlyOnce,
        ))
        .unwrap()
        .unwrap();
    tracing::info!("Published!");
    inflight.wait_until_delivered();
    tracing::info!("Broker received!");
}

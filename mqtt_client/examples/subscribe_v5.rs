use mqtt_client::{client::SyncClient, client_opts::ClientOpts};
use rust_mqtt_protocol::{MqttTopic, MqttV5_0_0, Publish, Qos};
use tracing::{dispatcher::set_global_default, Level};

#[tokio::main()]
async fn main() {
    let collector = tracing_subscriber::fmt::fmt()
        .with_max_level(Level::TRACE)
        .finish();
    set_global_default(collector.into()).unwrap();

    let client: SyncClient<MqttV5_0_0> = SyncClient::connect_tcp(
        ClientOpts {
            client_id: "client-id-sub".to_string(),
            ..Default::default()
        },
        "127.0.0.1:1883".to_string(),
    )
    .expect("Connection failed");

    tracing::info!("Subscribing!");
    let suback = client
        .subscribe(vec![MqttTopic::try_from("qos0").unwrap()], Qos::AtMostOnce)
        .unwrap();
    tracing::info!("Got SubAck {suback:?}");

    tracing::info!("Publish with qos=0");
    assert!(client
        .publish(Publish::new(
            "qos0".try_into().unwrap(),
            b"Published message content as utf8 string",
            Qos::AtMostOnce,
            false,
        ))
        .unwrap()
        .is_none());
    tracing::info!("Published!");

    let stream = client.stream();
    let msg = stream.recv().unwrap();
    tracing::info!("Received message: {msg:?}");
    tracing::info!(
        "Decoded message: {}",
        String::from_utf8(msg.payload().to_vec()).unwrap()
    );
}

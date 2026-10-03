use std::time::Duration;

use modular_mqtt::{ClientOptsV5, SyncClient};
use modular_mqtt_protocol::{Publish, Qos};
use tracing::{dispatcher::set_global_default, Level};

fn main() {
    let collector = tracing_subscriber::fmt::fmt()
        .with_max_level(Level::TRACE)
        .finish();
    set_global_default(collector.into()).unwrap();

    let (recv_stream, client) = SyncClient::connect(
        ClientOptsV5 {
            client_id: "client-id-sub".to_string(),
            ..Default::default()
        },
        "127.0.0.1:1883".to_string(),
    )
    .expect("Connection failed");

    tracing::info!("Subscribing!");
    let suback = client
        .subscribe(vec!["qos0"], Qos::AtMostOnce, Duration::from_secs(5))
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

    let msg = recv_stream.recv().unwrap();
    tracing::info!("Received message: {msg:?}");
    tracing::info!(
        "Decoded message: {}",
        String::from_utf8(msg.payload().to_vec()).unwrap()
    );
}

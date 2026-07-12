use mqtt_client::{client::SyncClient, client_opts::ClientOpts};
use tracing::{dispatcher::set_global_default, Level};

#[tokio::main()]
async fn main() {
    let collector = tracing_subscriber::fmt::fmt()
        .with_max_level(Level::TRACE)
        .finish();
    set_global_default(collector.into()).unwrap();

    SyncClient::connect_tcp(
        ClientOpts {
            client_id: "client-id".to_string(),
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
}

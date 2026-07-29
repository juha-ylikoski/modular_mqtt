use mqtt_client::{client::Client, client_opts::ClientOpts, connection::SyncWriter};
use rust_mqtt_protocol::MqttV5_0_0;
use tracing::{dispatcher::set_global_default, Level};

fn main() {
    let collector = tracing_subscriber::fmt::fmt()
        .with_max_level(Level::TRACE)
        .finish();
    set_global_default(collector.into()).unwrap();

    Client::<MqttV5_0_0, SyncWriter>::connect_tcp(
        ClientOpts {
            client_id: "client-id".to_string(),
            ..Default::default()
        },
        "127.0.0.1:1883".to_string(),
    )
    .expect("Connection failed");
}

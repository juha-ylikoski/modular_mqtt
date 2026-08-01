use modular_mqtt::{client::Client, client_opts::ClientOpts, connection::SyncWriter};
use modular_mqtt_protocol::MqttV3_1_1;
use tracing::{dispatcher::set_global_default, Level};

fn main() {
    let collector = tracing_subscriber::fmt::fmt()
        .with_max_level(Level::TRACE)
        .finish();
    set_global_default(collector.into()).unwrap();

    Client::<MqttV3_1_1, SyncWriter>::connect_tcp(
        ClientOpts {
            client_id: "client-id".to_string(),
            ..Default::default()
        },
        "127.0.0.1:1883".to_string(),
    )
    .expect("Connection failed");
}

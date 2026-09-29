use modular_mqtt::{ClientOptsV3, SyncClient};
use tracing::{dispatcher::set_global_default, Level};

fn main() {
    let collector = tracing_subscriber::fmt::fmt()
        .with_max_level(Level::TRACE)
        .finish();
    set_global_default(collector.into()).unwrap();

    SyncClient::connect_tcp(
        ClientOptsV3 {
            client_id: "client-id".to_string(),
            ..Default::default()
        },
        "127.0.0.1:1883".to_string(),
    )
    .expect("Connection failed");
}

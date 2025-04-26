use mqtt_client::{client::SyncClient, client_opts::ClientOpts};
use tracing::{dispatcher::set_global_default, Level};

#[tokio::main()]
async fn main() {
    let collector = tracing_subscriber::fmt::fmt()
        .with_max_level(Level::TRACE)
        .finish();
    set_global_default(collector.into()).unwrap();

    SyncClient::connect(ClientOpts {
        broker: "127.0.0.1:1883".to_string(),
        client_id: "client-id".to_string(),
        keep_alive: 15,
        clean_session: true,
        will: None,
        username: None,
        password: None,
    })
    .expect("Connection failed");
}

use std::time::Duration;

pub(crate) mod backend;
pub(crate) mod client;
pub(crate) mod client_communication;
pub(crate) mod client_opts;
pub(crate) mod connection;
pub mod error;
pub(crate) mod util;

pub const RESENT_INTERVAL: Duration = Duration::from_secs(10);

pub use client::sync_client::Client as SyncClient;
pub use client_opts::{ClientOpts, ClientOptsV3, ClientOptsV5};
use modular_mqtt_protocol::{MqttV3_1_1, MqttV5_0_0};
pub use util::IntoTopicSubscription;

pub type SyncClientV3 = SyncClient<MqttV3_1_1, ClientOptsV3>;
pub type SyncClientV5 = SyncClient<MqttV5_0_0, ClientOptsV5>;

// #[cfg(feature = "async")]
// pub type AsyncClient<V> = client::Client<V, AsyncWriter>;

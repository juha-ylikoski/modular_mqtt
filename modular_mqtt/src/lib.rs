use std::time::Duration;

pub(crate) mod backend;
pub(crate) mod client;
pub(crate) mod client_communication;
pub(crate) mod client_opts;
pub(crate) mod connection;
pub mod error;
pub mod util;

pub const RESENT_INTERVAL: Duration = Duration::from_secs(10);

pub use client::sync_client::Client as SyncClient;
pub use client_opts::{ClientOpts, ClientOptsV3, ClientOptsV5};
use modular_mqtt_protocol::{MqttV3_1_1, MqttV5_0_0};

pub type SyncClientV3 = SyncClient<MqttV3_1_1, ClientOptsV3>;
pub type SyncClientV5 = SyncClient<MqttV5_0_0, ClientOptsV5>;

#[cfg(feature = "async")]
pub use client::async_client::Client as AsyncClient;

#[cfg(feature = "async")]
pub type AsyncClientV3 = AsyncClient<MqttV3_1_1, ClientOptsV3>;
#[cfg(feature = "async")]
pub type AsyncClientV5 = AsyncClient<MqttV5_0_0, ClientOptsV5>;

#[cfg(not(feature = "async"))]
pub(crate) use std::time::Instant;
#[cfg(feature = "async")]
pub(crate) use tokio::time::Instant;

const CONNECTION_TIMEOUT: Duration = Duration::from_secs(30);
const PING_TIMEOUT: Duration = Duration::from_secs(30);
/// How long timeout can there be between fragmented packet
/// fragment reads
const PACKET_TIMEOUT: Duration = Duration::from_secs(30);

/// Frequency of how often pings are send
///
/// this value is multiplied with keep alive to get
/// absolute value (in seconds)
const PING_FREQUENCY: u64 = 70;

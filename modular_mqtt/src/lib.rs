use std::time::Duration;

#[cfg(feature = "async")]
use crate::connection::async_stream::AsyncWriter;
use crate::connection::SyncWriter;

pub mod client;
pub mod client_communication;
pub mod client_opts;
pub mod connection;
pub mod error;
pub mod util;

pub const RESENT_INTERVAL: Duration = Duration::from_secs(10);

pub type SyncClient<V> = client::Client<V, SyncWriter>;
#[cfg(feature = "async")]
pub type AsyncClient<V> = client::Client<V, AsyncWriter>;

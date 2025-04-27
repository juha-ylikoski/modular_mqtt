use std::time::Duration;

pub mod client;
pub mod client_opts;
pub mod error;
pub mod sync_connection;
pub mod util;

pub const RESENT_INTERVAL: Duration = Duration::from_secs(10);

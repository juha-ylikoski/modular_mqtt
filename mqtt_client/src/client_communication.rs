use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::error::ClientError;

/// Mqtt message id (mid)
type Mid = u16;

#[derive(Clone)]
pub struct ClientCommunicator<I, W>
where
    I: Clone,
    W: Clone,
{
    retention: Duration,
    data: I,
    wakeup: W,
}

pub type SyncData<V> = Arc<std::sync::RwLock<HashMap<Mid, (Instant, V)>>>;
pub type SyncWakeup = Arc<(std::sync::Mutex<Mid>, std::sync::Condvar)>;

impl<V> ClientCommunicator<SyncData<V>, SyncWakeup>
where
    V: Clone,
{
    pub fn new(retention: Duration) -> Self {
        Self {
            retention,
            data: Arc::new(std::sync::RwLock::new(HashMap::new())),
            wakeup: Arc::new((std::sync::Mutex::new(0), std::sync::Condvar::new())),
        }
    }

    pub fn insert(&self, k: Mid, v: V) {
        self.data.write().unwrap().insert(k, (Instant::now(), v));
        let (lock, cvar) = &*self.wakeup;
        let mut mid = lock.lock().unwrap();
        *mid = k;
        cvar.notify_all();
    }

    pub fn get(&self, k: Mid, timeout: Duration) -> Result<V, ClientError> {
        // Check if already there (maybe due to race condition)
        if let Some(v) = self.data.read().unwrap().get(&k) {
            return Ok(v.1.clone());
        }

        let now = Instant::now();
        let timeout_at = now + timeout;
        let (lock, cvar) = &*self.wakeup;
        let mut mid = lock.lock().unwrap();
        while *mid != k {
            let timeout = timeout_at.duration_since(now);
            let res = cvar.wait_timeout(mid, timeout).unwrap();
            mid = res.0;
            if res.1.timed_out() {
                return Err(ClientError::Timeout);
            }
        }

        Ok(self.data.read().unwrap().get(&k).unwrap().1.clone())
    }

    pub fn prune(&mut self) {
        let now = Instant::now();
        self.data
            .write()
            .unwrap()
            .retain(|_, (creation_time, _)| now.duration_since(*creation_time) < self.retention);
    }
}

#[cfg(feature = "async")]
pub mod async_communicator {
    use super::*;

    use tokio::time::Instant;

    pub type AsyncData<V> = Arc<tokio::sync::RwLock<HashMap<Mid, (tokio::time::Instant, V)>>>;
    pub type AsyncWakeup = Arc<tokio::sync::Notify>;

    impl<V> ClientCommunicator<AsyncData<V>, AsyncWakeup>
    where
        V: Clone,
    {
        pub fn new(retention: Duration) -> Self {
            Self {
                retention,
                data: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
                wakeup: Arc::new(tokio::sync::Notify::new()),
            }
        }

        pub async fn insert(&self, k: Mid, v: V) {
            self.data.write().await.insert(k, (Instant::now(), v));
            self.wakeup.notify_waiters();
        }

        pub async fn get(&self, k: Mid) -> Result<V, ClientError> {
            // Check first if already there (maybe due to race condition)
            if let Some(v) = self.data.read().await.get(&k) {
                return Ok(v.1.clone());
            }

            self.wakeup.notified().await;
            loop {
                if let Some(v) = self.data.read().await.get(&k) {
                    return Ok(v.1.clone());
                }
            }
        }

        pub async fn prune(&mut self) {
            let now = Instant::now();
            self.data.write().await.retain(|_, (creation_time, _)| {
                now.duration_since(*creation_time) < self.retention
            });
        }
    }
}

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

pub type SyncClientCommunicator<V> = ClientCommunicator<SyncData<V>, SyncWakeup>;

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

#[cfg(test)]
mod test {
    use std::time::Duration;

    use crate::{client_communication::SyncClientCommunicator, error::ClientError};

    #[test]
    fn test_notify_after_recv() {
        let comm: SyncClientCommunicator<u8> =
            SyncClientCommunicator::new(Duration::from_secs(2000));
        let sender = comm.clone();

        sender.insert(1, 42);
        assert_eq!(comm.get(1, Duration::from_secs(1)).unwrap(), 42);
    }

    #[test]
    fn test_notify_before_recv() {
        let comm: SyncClientCommunicator<u8> =
            SyncClientCommunicator::new(Duration::from_secs(2000));

        let sender = comm.clone();

        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            sender.insert(1, 42);
        });
        assert_eq!(comm.get(1, Duration::from_secs(1)).unwrap(), 42);
    }

    #[test]
    fn unrelated_ack_does_not_strand_other_waiter() {
        let comm: SyncClientCommunicator<u8> = SyncClientCommunicator::new(Duration::from_secs(60));
        let s = comm.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            s.insert(2, 99); // ack for a different mid
            s.insert(1, 42); // ack for the waiter, immediately after
        });
        assert_eq!(comm.get(1, Duration::from_secs(5)).unwrap(), 42);
    }

    #[test]
    fn test_timeout() {
        let comm: SyncClientCommunicator<u8> =
            SyncClientCommunicator::new(Duration::from_secs(2000));
        let sender = comm.clone();

        sender.insert(1, 42);
        let res = comm.get(2, Duration::from_secs(1));
        assert!(res.is_err());
        match res.unwrap_err() {
            ClientError::Timeout => (),
            _ => panic!("Received wrong error type"),
        }
    }
}

#[cfg(feature = "async")]
pub mod async_communicator {
    use super::*;

    use tokio::time::Instant;

    pub type AsyncData<V> = Arc<tokio::sync::RwLock<HashMap<Mid, (tokio::time::Instant, V)>>>;
    pub type AsyncWakeup = Arc<tokio::sync::Notify>;

    pub type AsyncClientCommunicator<V> = ClientCommunicator<AsyncData<V>, AsyncWakeup>;

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
            loop {
                // register interest *before* checking
                let notified = self.wakeup.notified();
                tokio::pin!(notified);
                notified.as_mut().enable(); // registers with the Notify now

                if let Some(v) = self.data.read().await.get(&k) {
                    return Ok(v.1.clone());
                }

                notified.await;
            }
        }

        pub async fn prune(&mut self) {
            let now = Instant::now();
            self.data.write().await.retain(|_, (creation_time, _)| {
                now.duration_since(*creation_time) < self.retention
            });
        }
    }

    #[cfg(test)]
    mod test {
        use std::time::Duration;

        use crate::client_communication::async_communicator::AsyncClientCommunicator;

        #[tokio::test]
        async fn test_notify_after_recv() {
            let comm: AsyncClientCommunicator<u8> =
                AsyncClientCommunicator::new(Duration::from_secs(2000));
            let sender = comm.clone();

            sender.insert(1, 42).await;
            assert_eq!(comm.get(1).await.unwrap(), 42);
        }

        #[tokio::test]
        async fn test_notify_before_recv() {
            let comm: AsyncClientCommunicator<u8> =
                AsyncClientCommunicator::new(Duration::from_secs(2000));

            let sender = comm.clone();

            tokio::task::spawn(async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                sender.insert(1, 42).await;
            });
            assert_eq!(comm.get(1).await.unwrap(), 42);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn unrelated_ack_does_not_strand_other_waiter() {
        let comm: AsyncClientCommunicator<u8> =
            AsyncClientCommunicator::new(Duration::from_secs(60));
        let s = comm.clone();
        tokio::task::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            s.insert(2, 99).await; // ack for a different mid
            s.insert(1, 42).await; // ack for the waiter, immediately after
        });
        assert_eq!(comm.get(1).await.unwrap(), 42);
    }
}

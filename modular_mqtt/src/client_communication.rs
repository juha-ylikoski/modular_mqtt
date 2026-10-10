use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::error::ClientError;

/// Mqtt message id (mid)
type Mid = u16;

#[derive(Clone)]
pub struct SyncClientCommunicator<V>
where
    V: Clone,
{
    retention: Duration,
    #[allow(clippy::type_complexity)]
    wakeup: Arc<(
        std::sync::Mutex<HashMap<Mid, (Instant, V)>>,
        std::sync::Condvar,
    )>,
}

impl<V> SyncClientCommunicator<V>
where
    V: Clone,
{
    pub fn new(retention: Duration) -> Self {
        Self {
            retention,
            wakeup: Arc::new((
                std::sync::Mutex::new(HashMap::new()),
                std::sync::Condvar::new(),
            )),
        }
    }

    pub fn insert(&self, k: Mid, v: V) {
        self.wakeup.0.lock().unwrap().insert(k, (Instant::now(), v));
        self.wakeup.1.notify_all();
    }

    pub fn get(&self, k: Mid, timeout: Duration) -> Result<V, ClientError> {
        /// Upper bound on a single park, so a missed notification costs latency rather
        /// than the full timeout. The lock is held continuously across "check map" and
        /// "wait", so `notify_all` cannot actually be missed — this is insurance against
        /// a future refactor breaking that, not a correctness requirement.
        const MAX_PARK: Duration = Duration::from_millis(500);

        let mut now = Instant::now();
        let timeout_at = now + timeout;
        let mut lock = self.wakeup.0.lock().unwrap();
        let cvar = &self.wakeup.1;
        let mut res = lock.get(&k).cloned();
        while res.is_none() {
            let timeout = timeout_at.saturating_duration_since(now);
            let wait_res = cvar.wait_timeout(lock, timeout.min(MAX_PARK)).unwrap();
            res = wait_res.0.get(&k).cloned();
            lock = wait_res.0;
            now = Instant::now();
            if now > timeout_at && res.is_none() {
                return Err(ClientError::Timeout);
            }
        }

        Ok(res.unwrap().1)
    }

    pub fn prune(&self) {
        let now = Instant::now();
        self.wakeup
            .0
            .lock()
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

    #[derive(Clone)]
    pub struct AsyncClientCommunicator<V>
    where
        V: Clone,
    {
        retention: Duration,
        data: Arc<tokio::sync::RwLock<HashMap<Mid, (tokio::time::Instant, V)>>>,
        wakeup: Arc<tokio::sync::Notify>,
    }

    impl<V> AsyncClientCommunicator<V>
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

        pub async fn prune(&self) {
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

use std::{
    io::{Read, Write},
    sync::{Arc, Mutex},
    time::Duration,
};

use mqtt_client::sync_connection::SyncStream;

#[derive(Clone)]
pub struct Stream {
    from_server: Arc<Mutex<Vec<u8>>>,
    read_buf: Arc<Mutex<Vec<u8>>>,
    to_server: Arc<Mutex<Vec<u8>>>,
    timeout: Arc<Mutex<Option<Duration>>>,
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        std::io::Write::write(&mut *self.to_server.lock().unwrap(), buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        std::io::Write::flush(&mut *self.to_server.lock().unwrap())
    }
}
impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if !self.read_buf.lock().unwrap().is_empty() {
            let mut lock = self.read_buf.lock().unwrap();
            let mut read_buf = &lock[..];
            let len = std::io::Read::read(&mut read_buf, buf)?;
            lock.drain(0..len);
            return Ok(len);
        }

        if let Some(timeout) = *self.timeout.lock().unwrap() {
            let mut dur = Duration::from_millis(0);
            loop {
                let mut lock = self.from_server.lock().unwrap();
                if lock.is_empty() {
                    if dur > timeout {
                        return Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout"));
                    }
                    dur += Duration::from_millis(10);
                    std::thread::sleep(Duration::from_millis(10));
                } else {
                    *self.read_buf.lock().unwrap() = lock.drain(..).collect();
                    break;
                }
            }
        } else {
            *self.read_buf.lock().unwrap() = self.from_server.lock().unwrap().drain(..).collect();
        };
        let mut lock = self.read_buf.lock().unwrap();
        let mut read_buf = &lock[..];
        let len = std::io::Read::read(&mut read_buf, buf)?;
        lock.drain(0..len);
        Ok(len)
    }
}
impl SyncStream for Stream {
    fn set_read_timeout(&self, dur: Option<std::time::Duration>) -> Result<(), std::io::Error> {
        *self.timeout.lock().unwrap() = dur;
        Ok(())
    }

    fn try_clone(&self) -> Result<Self, std::io::Error> {
        Ok(self.clone())
    }
}

impl Stream {
    pub fn new() -> Self {
        Self {
            timeout: Arc::new(Mutex::new(None)),
            from_server: Arc::new(Mutex::new(Vec::new())),
            read_buf: Arc::new(Mutex::new(Vec::new())),
            to_server: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

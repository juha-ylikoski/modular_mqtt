use std::{
    sync::{Arc, Condvar, Mutex, RwLock},
    time::{Duration, SystemTime},
};

use rust_mqtt_protocol::{
    MqttTopic, Publish, Qos, QosPacketIdentifier,
};

pub fn buf_with_size(n: usize) -> Vec<u8> {
    let mut vec: Vec<u8> = Vec::with_capacity(n);
    unsafe { vec.set_len(n) };
    vec
}

#[derive(Clone)]
pub struct Event(Arc<(Mutex<bool>, Condvar)>);

impl Event {
    pub fn new() -> Self {
        Self(Arc::new((Mutex::new(false), Condvar::new())))
    }
    pub fn notify(self) {
        let (lock, cvar) = &*self.0;
        let mut done = lock.lock().unwrap();
        *done = true;
        cvar.notify_one();
    }
    pub fn wait_timeout(self, duration: Duration) {
        // Ref https://doc.rust-lang.org/nightly/std/sync/struct.Condvar.html#method.wait_timeout
        let (lock, cvar) = &*self.0;
        let mut done = lock.lock().unwrap();
        loop {
            let result = cvar.wait_timeout(done, duration).unwrap();
            done = result.0;
            if *done == true {
                break;
            }
        }
    }
}

#[derive(Debug)]
pub struct Message {
    topic: MqttTopic,
    payload: Vec<u8>,
    retain: bool,
    pub qos: Qos,
}

impl Message {
    pub fn new<T: Into<Vec<u8>>>(topic: MqttTopic, payload: T, qos: Qos) -> Self {
        Self {
            topic,
            payload: payload.into(),
            retain: false,
            qos,
        }
    }
    pub fn packet(&self, dup: bool, id: Option<u16>) -> Publish {
        let qos = match self.qos {
            Qos::AtMostOnce => QosPacketIdentifier::AtMostOnce,
            Qos::AtLeastOnce => {
                let id = id.expect("Did not receive packet identifier with qos>0. This is a bug!");
                QosPacketIdentifier::AtLeastOnce(id)
            }
            Qos::ExactlyOnce => {
                let id = id.expect("Did not receive packet identifier with qos>0. This is a bug!");
                QosPacketIdentifier::ExactlyOnce(id)
            }
        };
        Publish::new(dup, qos, self.retain, &self.topic, &self.payload)
    }
}

#[derive(Debug)]
pub enum InflightMessageState {
    PubAck(SystemTime),
    PubRec(SystemTime),
    PubComp(SystemTime),
    Sent,
}

#[derive(Debug)]
pub struct InflightMessage {
    pub state: RwLock<InflightMessageState>,
    pub msg: Message,
}

impl InflightMessage {
    pub fn wait_until_delivered(&self) {
        loop {
            if matches!(*self.state.read().unwrap(), InflightMessageState::Sent) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }
}

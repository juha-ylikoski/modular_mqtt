use std::{sync::RwLock, time::SystemTime};

use rust_mqtt_protocol::{MqttTopic, Publish, Qos, QosPacketIdentifier};

#[allow(clippy::uninit_vec)]
pub fn buf_with_size(n: usize) -> Vec<u8> {
    let mut vec: Vec<u8> = Vec::with_capacity(n);
    unsafe { vec.set_len(n) };
    vec
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

#[derive(Debug, Clone)]
pub enum InflightMessageState {
    PubAck(SystemTime),
    PubRec(SystemTime),
    PubComp(SystemTime),
    Sent,
}

#[derive(Debug)]
pub struct InflightMessage {
    pub state: RwLock<InflightMessageState>,
    pub packet_identifier: u16,
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
    pub fn packet_identifier(&self) -> u16 {
        self.packet_identifier
    }
}

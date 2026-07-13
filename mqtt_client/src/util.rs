use std::{sync::RwLock, time::SystemTime};

use rust_mqtt_protocol::{
    MqttTopic, Publish, Qos, QosPacketIdentifier, RetainHandling, TopicSubscription,
};

#[derive(Debug, Clone)]
pub enum InflightMessageState {
    PubAck(SystemTime),
    PubRec(SystemTime),
    PubComp(SystemTime),
    Sent,
}

#[derive(Debug)]
pub struct InflightMessage<V> {
    pub state: RwLock<InflightMessageState>,
    pub packet_identifier: u16,
    pub msg: Publish<V, QosPacketIdentifier>,
}

impl<V> InflightMessage<V> {
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

pub trait IntoTopicSubscription {
    fn into_topic_subscription(
        self,
        v3: bool,
        qos: Qos,
        no_local: bool,
        keep_retain: bool,
        retain_handling: RetainHandling,
    ) -> TopicSubscription;
}

impl IntoTopicSubscription for MqttTopic {
    fn into_topic_subscription(
        self,
        v3: bool,
        qos: Qos,
        no_local: bool,
        keep_retain: bool,
        retain_handling: RetainHandling,
    ) -> TopicSubscription {
        if v3 {
            TopicSubscription::V3 { topic: self, qos }
        } else {
            TopicSubscription::V5 {
                topic: self,
                qos,
                no_local,
                keep_retain,
                retain_handling,
            }
        }
    }
}

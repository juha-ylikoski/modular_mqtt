use std::{sync::RwLock, time::SystemTime};

use rust_mqtt_protocol::{
    MqttTopic, MqttV3_1_1, MqttV5_0_0, MqttVersion, Publish, Qos, QosPacketIdentifier,
    RetainHandling, TopicSubscription, TopicSubscriptionV3, TopicSubscriptionV5,
};

#[derive(Debug, Clone)]
pub enum InflightMessageState {
    PubAck(SystemTime),
    PubRec(SystemTime),
    PubComp(SystemTime),
    Sent,
}

#[derive(Debug)]
pub struct InflightMessage<V: MqttVersion> {
    pub state: RwLock<InflightMessageState>,
    pub packet_identifier: u16,
    pub msg: Publish<V, QosPacketIdentifier>,
}

impl<V: MqttVersion> InflightMessage<V> {
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

pub trait IntoTopicSubscription<V: MqttVersion> {
    fn into_topic_subscription(
        self,
        qos: Qos,
        no_local: bool,
        keep_retain: bool,
        retain_handling: RetainHandling,
    ) -> V::TopicSubscription;
}

impl IntoTopicSubscription<MqttV3_1_1> for MqttTopic {
    fn into_topic_subscription(
        self,
        qos: Qos,
        _no_local: bool,
        _keep_retain: bool,
        _retain_handling: RetainHandling,
    ) -> TopicSubscriptionV3 {
        TopicSubscriptionV3::new(self, qos)
    }
}
impl IntoTopicSubscription<MqttV5_0_0> for MqttTopic {
    fn into_topic_subscription(
        self,
        qos: Qos,
        no_local: bool,
        keep_retain: bool,
        retain_handling: RetainHandling,
    ) -> TopicSubscriptionV5 {
        TopicSubscriptionV5::new(self, qos, no_local, keep_retain, retain_handling)
    }
}

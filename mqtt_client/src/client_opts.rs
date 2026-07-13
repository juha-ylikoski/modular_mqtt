use std::marker::PhantomData;

use rust_mqtt_protocol::{
    MqttLastWill3_1_1, MqttLastWill5_0_0, MqttV3_1_1, MqttV5_0_0, RetainHandling,
};

use bytes::Bytes;

pub enum OnDisconnectBehavior {
    Panic,
}

#[derive(Clone)]
pub enum MqttLastWill<V> {
    V3 {
        protocol_level: PhantomData<V>,
        will: MqttLastWill3_1_1,
    },
    V5 {
        protocol_level: PhantomData<V>,
        will: MqttLastWill5_0_0,
    },
}

impl From<MqttLastWill3_1_1> for MqttLastWill<MqttV3_1_1> {
    fn from(value: MqttLastWill3_1_1) -> Self {
        Self::V3 {
            protocol_level: PhantomData,
            will: value,
        }
    }
}

impl From<MqttLastWill5_0_0> for MqttLastWill<MqttV5_0_0> {
    fn from(value: MqttLastWill5_0_0) -> Self {
        Self::V5 {
            protocol_level: PhantomData,
            will: value,
        }
    }
}

pub enum ExtraOptions<V> {
    V3 {
        protocol_level: PhantomData<V>,
    },
    V5 {
        protocol_level: PhantomData<V>,
        subscription_no_local: bool,
        subscription_keep_retain: bool,
        subscription_retain_handling: RetainHandling,
    },
}

impl ExtraOptions<MqttV3_1_1> {
    pub fn new_v3() -> Self {
        Self::V3 {
            protocol_level: PhantomData,
        }
    }
}

impl ExtraOptions<MqttV5_0_0> {
    pub fn new_v5(
        subscription_no_local: bool,
        subscription_keep_retain: bool,
        subscription_retain_handling: RetainHandling,
    ) -> Self {
        Self::V5 {
            protocol_level: PhantomData,
            subscription_no_local,
            subscription_keep_retain,
            subscription_retain_handling,
        }
    }
}

pub struct ClientOpts<V> {
    pub client_id: String,
    pub keep_alive: u16,
    pub clean_session: bool,
    pub will: Option<MqttLastWill<V>>,
    pub username: Option<String>,
    pub password: Option<Bytes>,
    pub on_disconnect: OnDisconnectBehavior,
    pub max_packet_size: usize,
    pub extra_opts: ExtraOptions<V>,
}

impl Default for ClientOpts<MqttV3_1_1> {
    fn default() -> Self {
        Self {
            client_id: "".to_string(),
            keep_alive: 30,
            clean_session: true,
            will: None,
            username: None,
            password: None,
            on_disconnect: OnDisconnectBehavior::Panic,
            max_packet_size: rust_mqtt_protocol::MAX_MQTT_PACKET_SIZE,
            extra_opts: ExtraOptions::new_v3(),
        }
    }
}

impl Default for ClientOpts<MqttV5_0_0> {
    fn default() -> Self {
        Self {
            client_id: "".to_string(),
            keep_alive: 30,
            clean_session: true,
            will: None,
            username: None,
            password: None,
            on_disconnect: OnDisconnectBehavior::Panic,
            max_packet_size: rust_mqtt_protocol::MAX_MQTT_PACKET_SIZE,
            extra_opts: ExtraOptions::new_v5(false, false, RetainHandling::SendAtSubscribe),
        }
    }
}

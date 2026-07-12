use std::marker::PhantomData;

use rust_mqtt_protocol::{MqttLastWill3_1_1, MqttLastWill5_0_0, MqttV3_1_1, MqttV5_0_0};

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

pub struct ClientOpts<V> {
    pub client_id: String,
    pub keep_alive: u16,
    pub clean_session: bool,
    pub will: Option<MqttLastWill<V>>,
    pub username: Option<String>,
    pub password: Option<Bytes>,
    pub on_disconnect: OnDisconnectBehavior,
    pub max_packet_size: usize,
}

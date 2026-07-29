use rust_mqtt_protocol::{MqttV3_1_1, MqttV5_0_0, MqttVersion, RetainHandling};

use bytes::Bytes;
use std::time::Duration;

const DEFAULT_ACK_RETENTION: Duration = Duration::from_mins(1);

pub enum OnDisconnectBehavior {
    Panic,
}

pub trait MqttOptions: MqttVersion {
    type ExtraOptions: Send + Sync;
}

impl MqttOptions for MqttV3_1_1 {
    type ExtraOptions = ();
}

impl MqttOptions for MqttV5_0_0 {
    type ExtraOptions = MqttV5Options;
}

pub struct MqttV5Options {
    pub subscription_no_local: bool,
    pub subscription_keep_retain: bool,
    pub subscription_retain_handling: RetainHandling,
}

pub struct ClientOpts<V: MqttOptions> {
    pub client_id: String,
    pub keep_alive: u16,
    pub clean_session: bool,
    pub will: Option<V::LastWill>,
    pub username: Option<String>,
    pub password: Option<Bytes>,
    pub on_disconnect: OnDisconnectBehavior,
    pub max_packet_size: usize,
    pub resend_interval: Duration,
    pub extra_opts: V::ExtraOptions,
    pub ack_retention: Duration,
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
            resend_interval: crate::RESENT_INTERVAL,
            extra_opts: (),
            ack_retention: DEFAULT_ACK_RETENTION,
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
            resend_interval: crate::RESENT_INTERVAL,
            extra_opts: MqttV5Options {
                subscription_no_local: true,
                subscription_keep_retain: true,
                subscription_retain_handling: RetainHandling::SendAtSubscribe,
            },
            ack_retention: DEFAULT_ACK_RETENTION,
        }
    }
}

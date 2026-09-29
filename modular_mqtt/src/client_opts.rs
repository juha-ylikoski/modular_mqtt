use modular_mqtt_protocol::{
    ConnAck, Connect, MqttLastWill3_1_1, MqttLastWill5_0_0, MqttV3_1_1, MqttV5_0_0, MqttVersion,
    RetainHandling, UserProperty, MAX_MQTT_PACKET_SIZE,
};

use bytes::Bytes;
use std::time::Duration;

use crate::error::ConnectError;

const DEFAULT_ACK_RETENTION: Duration = Duration::from_mins(1);
const DEFAULT_KEEP_ALIVE: u16 = 30;
const DEFAULT_CLIENT_ID: &str = "";

pub(crate) fn exponential_backoff(
    min_retry_interval: Duration,
    max_retry_interval: Duration,
    retry_count: u32,
) -> Duration {
    // 1 ms floor: a zero or sub-second `min_retry_interval` must never degenerate into a
    // zero-delay reconnect loop.
    let min_ms = min_retry_interval
        .as_millis()
        .clamp(1, u128::from(u64::MAX)) as u64;
    let max_ms = max_retry_interval
        .as_millis()
        .clamp(1, u128::from(u64::MAX)) as u64;

    // `checked_shl` stops a large `retry_count` from overflowing the shift, and
    // `saturating_mul` stops the product from wrapping back *down* to a small delay.
    let factor = 1u64.checked_shl(retry_count).unwrap_or(u64::MAX);

    Duration::from_millis(min_ms.saturating_mul(factor).min(max_ms))
}

#[derive(Debug, Clone, Copy)]
pub enum OnDisconnectBehavior {
    Panic,
    ReconnectExponentialBackoff {
        min_retry_interval: Duration,
        max_retry_interval: Duration,
    },
}

pub trait ClientOpts<V>: std::marker::Send + std::marker::Sync + 'static
where
    V: MqttVersion,
{
    fn connect_error(connack: &ConnAck<V>) -> ConnectError;
    fn connect_msg(&self) -> Connect<V>;
    fn max_packet_size(&self) -> usize;
    fn keep_alive(&self) -> u16;
    fn ack_retention(&self) -> Duration;
    fn client_id(&self) -> &str;
    fn on_disconnect(&self) -> OnDisconnectBehavior;
    fn resend_interval(&self) -> Duration;
    fn clean_session(&self) -> bool;
    fn should_resubscribe(&self, connack: &ConnAck<V>) -> bool {
        self.clean_session() || !connack.session_present()
    }
}

pub struct ClientOptsV3 {
    pub client_id: String,
    pub keep_alive: u16,
    pub clean_session: bool,
    pub will: Option<MqttLastWill3_1_1>,
    pub username: Option<String>,
    pub password: Option<Bytes>,
    pub on_disconnect: OnDisconnectBehavior,
    pub max_packet_size: usize,
    pub resend_interval: Duration,
    pub ack_retention: Duration,
}

impl Default for ClientOptsV3 {
    fn default() -> Self {
        Self {
            client_id: DEFAULT_CLIENT_ID.to_string(),
            keep_alive: DEFAULT_KEEP_ALIVE,
            clean_session: true,
            will: None,
            username: None,
            password: None,
            on_disconnect: OnDisconnectBehavior::Panic,
            max_packet_size: MAX_MQTT_PACKET_SIZE,
            resend_interval: crate::RESENT_INTERVAL,
            ack_retention: DEFAULT_ACK_RETENTION,
        }
    }
}

impl ClientOpts<MqttV3_1_1> for ClientOptsV3 {
    fn connect_error(connack: &ConnAck<MqttV3_1_1>) -> ConnectError {
        ConnectError::ConnectFailedV3(connack.connect_rc())
    }

    fn connect_msg(&self) -> Connect<MqttV3_1_1> {
        Connect::new(
            self.clean_session,
            self.keep_alive,
            self.client_id.clone(),
            self.will.clone(),
            self.username.clone(),
            self.password.clone(),
        )
    }
    fn max_packet_size(&self) -> usize {
        self.max_packet_size
    }

    fn keep_alive(&self) -> u16 {
        self.keep_alive
    }

    fn ack_retention(&self) -> Duration {
        self.ack_retention
    }

    fn client_id(&self) -> &str {
        &self.client_id
    }

    fn on_disconnect(&self) -> OnDisconnectBehavior {
        self.on_disconnect
    }

    fn resend_interval(&self) -> Duration {
        self.resend_interval
    }

    fn clean_session(&self) -> bool {
        self.clean_session
    }
}

pub struct ClientOptsV5 {
    pub client_id: String,
    pub keep_alive: u16,
    pub clean_session: bool,
    pub will: Option<MqttLastWill5_0_0>,
    pub username: Option<String>,
    pub password: Option<Bytes>,
    pub on_disconnect: OnDisconnectBehavior,
    pub max_packet_size: usize,
    pub resend_interval: Duration,
    pub ack_retention: Duration,

    pub subscription_no_local: bool,
    pub subscription_keep_retain: bool,
    pub subscription_retain_handling: RetainHandling,
    /// If the Session Expiry Interval is absent the value 0 is used. If it is set to 0, or is absent,
    /// the Session ends when the Network Connection is closed.
    /// If the Session Expiry Interval is 0xFFFFFFFF (UINT_MAX), the Session does not expire.
    pub session_expiry_interval: Option<u32>,
    /// The Client uses this value to limit the number of QoS 1 and QoS 2 publications that it is willing
    /// to process concurrently. There is no mechanism to limit the QoS 0 publications that the Server might try to send.
    /// The value of Receive Maximum applies only to the current Network Connection. If the Receive Maximum
    /// value is absent then its value defaults to 65,535.
    pub receive_maximum: Option<u16>,
    /// If the Maximum Packet Size is not present, no limit on the packet size is imposed beyond the limitations
    /// in the protocol as a result of the remaining length encoding and the protocol header sizes.
    /// The packet size is the total number of bytes in an MQTT Control Packet
    /// The Client uses the Maximum Packet Size to inform the Server that it will not process packets exceeding this limit.
    pub maximum_packet_size: Option<u32>,
    /// If the Topic Alias Maximum property is absent, the default value is 0.
    /// This value indicates the highest value that the Client will accept as a Topic Alias sent by the Server.
    /// The Client uses this value to limit the number of Topic Aliases that it is willing to hold on this Connection
    /// A value of 0 indicates that the Client does not accept any Topic Aliases on this connection. If Topic
    /// Alias Maximum is absent or zero, the Server MUST NOT send any Topic Aliases to the Client
    pub topic_alias_maximum: Option<u16>,
    /// If the Request Response Information is absent, the value of false is used.
    /// The Client uses this value to request the Server to return Response Information in the CONNACK. A value of 0
    /// indicates that the Server MUST NOT return Response Information
    /// If the value is 1 the Server MAY return Response Information in the CONNACK packet.
    pub request_response_information: Option<bool>,
    /// The Client uses this value to indicate whether the Reason String or User Properties are sent in the case of failures.
    /// If the value of Request Problem Information is false, the Server MAY return a Reason String or User Properties on
    /// a CONNACK or DISCONNECT packet, but MUST NOT send a Reason String or User Properties on any packet other than PUBLISH, CONNACK, or DISCONNECT
    pub request_problem_information: Option<bool>,
    /// The User Property is allowed to appear multiple times to represent multiple name, value pairs. The same name is allowed to appear more than once.
    pub user_property: Vec<UserProperty>,
}

impl Default for ClientOptsV5 {
    fn default() -> Self {
        Self {
            client_id: DEFAULT_CLIENT_ID.to_string(),
            keep_alive: DEFAULT_KEEP_ALIVE,
            clean_session: true,
            will: None,
            username: None,
            password: None,
            on_disconnect: OnDisconnectBehavior::Panic,
            max_packet_size: MAX_MQTT_PACKET_SIZE,
            resend_interval: crate::RESENT_INTERVAL,
            ack_retention: DEFAULT_ACK_RETENTION,
            subscription_no_local: true,
            subscription_keep_retain: true,
            subscription_retain_handling: RetainHandling::SendAtSubscribe,
            session_expiry_interval: None,
            receive_maximum: None,
            maximum_packet_size: None,
            topic_alias_maximum: None,
            request_response_information: None,
            request_problem_information: None,
            user_property: Vec::new(),
        }
    }
}

impl ClientOpts<MqttV5_0_0> for ClientOptsV5 {
    fn connect_error(connack: &ConnAck<MqttV5_0_0>) -> ConnectError {
        ConnectError::ConnectFailedV5(connack.connect_rc())
    }

    fn connect_msg(&self) -> Connect<MqttV5_0_0> {
        let mut connect = Connect::new(
            self.clean_session,
            self.keep_alive,
            self.client_id.clone(),
            self.will.clone(),
            self.username.clone(),
            self.password.clone(),
        );
        if let Some(opt) = self.session_expiry_interval {
            connect = connect.set_session_expiry_interval(opt);
        }
        if let Some(opt) = self.receive_maximum {
            connect = connect.set_receive_maximum(opt);
        }
        if let Some(opt) = self.maximum_packet_size {
            connect = connect.set_maximum_packet_size(opt);
        }
        if let Some(opt) = self.topic_alias_maximum {
            connect = connect.set_topic_alias_maximum(opt);
        }
        if let Some(opt) = self.request_response_information {
            connect = connect.set_request_response_information(opt);
        }
        if let Some(opt) = self.request_problem_information {
            connect = connect.set_request_problem_information(opt);
        }
        connect.set_user_property(self.user_property.clone())
    }

    fn max_packet_size(&self) -> usize {
        self.max_packet_size
    }

    fn keep_alive(&self) -> u16 {
        self.keep_alive
    }

    fn ack_retention(&self) -> Duration {
        self.ack_retention
    }

    fn client_id(&self) -> &str {
        &self.client_id
    }

    fn on_disconnect(&self) -> OnDisconnectBehavior {
        self.on_disconnect
    }

    fn resend_interval(&self) -> Duration {
        self.resend_interval
    }

    fn clean_session(&self) -> bool {
        self.clean_session
    }
}

#[cfg(test)]
mod test {
    use super::exponential_backoff;
    use std::time::Duration;

    const MIN: Duration = Duration::from_secs(1);
    const MAX: Duration = Duration::from_secs(60);

    /// `min_retry_interval` is a floor, not the base of the exponent: the delay starts at
    /// `min` and doubles from there.
    #[test]
    fn first_retry_waits_min_interval() {
        assert_eq!(exponential_backoff(MIN, MAX, 0), MIN);
    }

    #[test]
    fn delay_doubles_on_each_retry() {
        let expected = [1, 2, 4, 8, 16, 32, 60, 60];
        for (retry_count, want) in expected.iter().enumerate() {
            assert_eq!(
                exponential_backoff(MIN, MAX, retry_count as u32),
                Duration::from_secs(*want),
                "retry_count={retry_count}"
            );
        }
    }

    /// A sub-second `min_retry_interval` must not be truncated to whole seconds, which would
    /// collapse the first few retries to a zero-delay loop.
    #[test]
    fn sub_second_min_interval_is_preserved() {
        let min = Duration::from_millis(500);
        let max = Duration::from_secs(8);
        let expected = [500, 1000, 2000, 4000, 8000, 8000];
        for (retry_count, want) in expected.iter().enumerate() {
            assert_eq!(
                exponential_backoff(min, max, retry_count as u32),
                Duration::from_millis(*want),
                "retry_count={retry_count}"
            );
        }
    }

    /// Guards the hot-loop regression: a zero `min_retry_interval` used to produce
    /// `Duration::ZERO` and spin the reconnect loop at full CPU.
    #[test]
    fn never_returns_zero_delay() {
        for retry_count in 0..32u32 {
            assert!(
                exponential_backoff(Duration::ZERO, MAX, retry_count) > Duration::ZERO,
                "retry_count={retry_count} produced a zero delay"
            );
        }
    }

    /// A long-lived client can accumulate an arbitrarily large `retry_count`; the shift and
    /// the multiply must saturate rather than overflow or wrap back down to a small delay.
    #[test]
    fn large_retry_count_saturates_to_max() {
        for retry_count in [63, 64, 65, 1_000, u32::MAX] {
            assert_eq!(
                exponential_backoff(MIN, MAX, retry_count),
                MAX,
                "retry_count={retry_count}"
            );
        }
    }

    /// `max_retry_interval` is the bound that protects the broker, so it wins even when the
    /// caller supplies a `min` above it.
    #[test]
    fn max_interval_wins_when_min_exceeds_max() {
        assert_eq!(
            exponential_backoff(Duration::from_secs(10), Duration::from_secs(5), 0),
            Duration::from_secs(5)
        );
    }
}

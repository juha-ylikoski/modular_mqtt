use modular_mqtt_protocol::{
    Connect, MqttV3_1_1, MqttV5_0_0, MqttVersion, RetainHandling, UserProperty,
};

use bytes::Bytes;
use std::time::Duration;

const DEFAULT_ACK_RETENTION: Duration = Duration::from_mins(1);

pub enum OnDisconnectBehavior {
    Panic,
}

pub(crate) trait MqttConnect<V>
where
    V: MqttVersion,
{
    fn finalize_connect_msg(&self, connect: Connect<V>) -> Connect<V>;
}

pub trait MqttOptions: MqttVersion {
    #[allow(private_bounds)]
    type ExtraOptions: Send + Sync + MqttConnect<Self>;
}

impl MqttOptions for MqttV3_1_1 {
    type ExtraOptions = ();
}

impl MqttConnect<MqttV3_1_1> for () {
    fn finalize_connect_msg(&self, connect: Connect<MqttV3_1_1>) -> Connect<MqttV3_1_1> {
        connect
    }
}

impl MqttOptions for MqttV5_0_0 {
    type ExtraOptions = MqttV5Options;
}

pub struct MqttV5Options {
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
    receive_maximum: Option<u16>,
    /// If the Maximum Packet Size is not present, no limit on the packet size is imposed beyond the limitations
    /// in the protocol as a result of the remaining length encoding and the protocol header sizes.
    /// The packet size is the total number of bytes in an MQTT Control Packet
    /// The Client uses the Maximum Packet Size to inform the Server that it will not process packets exceeding this limit.
    maximum_packet_size: Option<u32>,
    /// If the Topic Alias Maximum property is absent, the default value is 0.
    /// This value indicates the highest value that the Client will accept as a Topic Alias sent by the Server.
    /// The Client uses this value to limit the number of Topic Aliases that it is willing to hold on this Connection
    /// A value of 0 indicates that the Client does not accept any Topic Aliases on this connection. If Topic
    /// Alias Maximum is absent or zero, the Server MUST NOT send any Topic Aliases to the Client
    topic_alias_maximum: Option<u16>,
    /// If the Request Response Information is absent, the value of false is used.
    /// The Client uses this value to request the Server to return Response Information in the CONNACK. A value of 0
    /// indicates that the Server MUST NOT return Response Information
    /// If the value is 1 the Server MAY return Response Information in the CONNACK packet.
    request_response_information: Option<bool>,
    /// The Client uses this value to indicate whether the Reason String or User Properties are sent in the case of failures.
    /// If the value of Request Problem Information is false, the Server MAY return a Reason String or User Properties on
    /// a CONNACK or DISCONNECT packet, but MUST NOT send a Reason String or User Properties on any packet other than PUBLISH, CONNACK, or DISCONNECT
    request_problem_information: Option<bool>,
    /// The User Property is allowed to appear multiple times to represent multiple name, value pairs. The same name is allowed to appear more than once.
    user_property: Vec<UserProperty>,
}

impl MqttConnect<MqttV5_0_0> for MqttV5Options {
    fn finalize_connect_msg(&self, mut connect: Connect<MqttV5_0_0>) -> Connect<MqttV5_0_0> {
        if let Some(v) = self.session_expiry_interval {
            connect = connect.set_session_expiry_interval(v);
        }
        if let Some(v) = self.receive_maximum {
            connect = connect.set_receive_maximum(v);
        }
        if let Some(v) = self.maximum_packet_size {
            connect = connect.set_maximum_packet_size(v);
        }
        if let Some(v) = self.topic_alias_maximum {
            connect = connect.set_topic_alias_maximum(v);
        }
        if let Some(v) = self.request_response_information {
            connect = connect.set_request_response_information(v);
        }
        if let Some(v) = self.request_problem_information {
            connect = connect.set_request_problem_information(v);
        }
        if !self.user_property.is_empty() {
            connect = connect.set_user_property(self.user_property.clone());
        }
        connect
    }
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

impl<V> ClientOpts<V>
where
    V: MqttOptions,
{
    pub(crate) fn connect_msg(&self) -> Connect<V> {
        self.extra_opts.finalize_connect_msg(Connect::new(
            self.clean_session,
            self.keep_alive,
            self.client_id.clone(),
            self.will.clone(),
            self.username.clone(),
            self.password.clone(),
        ))
    }
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
            max_packet_size: modular_mqtt_protocol::MAX_MQTT_PACKET_SIZE,
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
            max_packet_size: modular_mqtt_protocol::MAX_MQTT_PACKET_SIZE,
            resend_interval: crate::RESENT_INTERVAL,
            extra_opts: MqttV5Options {
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
            },
            ack_retention: DEFAULT_ACK_RETENTION,
        }
    }
}

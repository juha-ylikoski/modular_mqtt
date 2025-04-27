pub use connack::{ConnAck, ConnectRc};
pub use connect::{Connect, MqttLastWill, MqttVersion3_1_1};
pub use fixed_header::{ControlPacketType, FixedHeader, FixedHeaderError};
pub use only_fixed::{Disconnect, PingReq, PingResp};
pub use packet_identifier_msgs::{PubAck, PubComp, PubRec, PubRel, UnsubscribeAck};
pub use publish::{Publish, ReceivedMessage};
pub use suback::SubAck;
pub use subscribe::{Subscribe, TopicSubscription};
pub use unsubscribe::Unsubscribe;
pub use util::{MqttTopic, PacketError, Qos, QosPacketIdentifier};

mod connack;
mod connect;
mod fixed_header;
mod only_fixed;
mod packet_identifier_msgs;
mod publish;
mod suback;
mod subscribe;
mod unsubscribe;
mod util;

pub enum MqttPackage<'a, V> {
    Connect(Connect<'a, V>),
    ConnAck(ConnAck),
    Publish(Publish<'a>),
    PubAck(PubAck),
    PubRec(PubRec),
    PubRel(PubRel),
    PubComp(PubComp),
    Subscribe(Subscribe),
    SubAck(SubAck),
    Unsubscribe(Unsubscribe),
    UnsubscribeAck(UnsubscribeAck),
    PingReq(PingReq),
    PingResp(PingResp),
    Disconnect(Disconnect),
}

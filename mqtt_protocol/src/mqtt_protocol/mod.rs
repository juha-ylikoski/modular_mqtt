use connack::ConnAck;
use connect::Connect;
use only_fixed::{Disconnect, PingReq, PingResp};
use packet_identifier_msgs::{PubAck, PubComp, PubRec, PubRel, UnsubscribeAck};
use publish::Publish;
use suback::SubAck;
use subscribe::Subscribe;
use unsubscribe::Unsubscribe;

pub mod connack;
pub mod connect;
pub mod fixed_header;
pub mod only_fixed;
pub mod packet_identifier_msgs;
pub mod publish;
pub mod suback;
pub mod subscribe;
pub mod unsubscribe;
pub mod util;

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

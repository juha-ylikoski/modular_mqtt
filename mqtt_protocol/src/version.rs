use std::fmt::Debug;

use bytes::{BufMut, Bytes};

use crate::{util::variable_len_int_size, Error};

mod sealed {
    use crate::{MqttV3_1_1, MqttV5_0_0};

    pub trait Sealed {}

    impl Sealed for MqttV3_1_1 {}
    impl Sealed for MqttV5_0_0 {}
}

pub trait PacketProperties: Sized + PartialEq + Debug {
    fn try_read(data: &mut Bytes) -> Result<Self, Error>;
    fn write_properties(&self, buf: &mut impl BufMut);
    fn properties_len(&self) -> usize;
    fn properties_block_len(&self) -> usize {
        let l = self.properties_len();
        variable_len_int_size(l) + l
    }
}

pub trait MqttVersion: sealed::Sealed {
    const VERSION: u8;
    type ConnackRc: Copy + Into<u8> + TryFrom<u8, Error = Error> + PartialEq + Debug;
    type ConnackProperties: PacketProperties;
    type AckTypeProperties<R: crate::ack_messages::ReasonCode>: PacketProperties;
    type DisconnectData: PacketProperties;
    type SubscribeData: PacketProperties;
    type SubAckData: PacketProperties;
    type TopicSubscription: crate::subscribe::TopicSubscription;
    type UnsubscribeProperties: PacketProperties;
    type UnSubAckProperties: PacketProperties;
    type PublishProperties: PacketProperties;
    type ConnectProperties: PacketProperties;
    type LastWill: crate::connect::MqttLastWill;
}

impl MqttVersion for crate::MqttV3_1_1 {
    const VERSION: u8 = 4;
    type ConnackRc = crate::connack::ConnectRcV3;
    type ConnackProperties = ();
    type AckTypeProperties<R: crate::ack_messages::ReasonCode> = ();
    type DisconnectData = ();
    type SubscribeData = ();
    type SubAckData = Vec<crate::suback::SubRcV3>;
    type TopicSubscription = crate::subscribe::TopicSubscriptionV3;
    type UnsubscribeProperties = ();
    type UnSubAckProperties = ();
    type PublishProperties = ();
    type ConnectProperties = ();
    type LastWill = crate::connect::MqttLastWill3_1_1;
}

impl MqttVersion for crate::MqttV5_0_0 {
    const VERSION: u8 = 5;
    type ConnackRc = crate::connack::ConnectRcV5;
    type ConnackProperties = crate::connack::ConnackPropertiesV5;
    type AckTypeProperties<R: crate::ack_messages::ReasonCode> = crate::ack_messages::PubAckData<R>;
    type DisconnectData = crate::disconnect::DisconnectData;
    type SubscribeData = crate::subscribe::SubscribeOptions;
    type SubAckData = crate::suback::SubAckDataV5;
    type TopicSubscription = crate::subscribe::TopicSubscriptionV5;
    type UnsubscribeProperties = crate::unsubscribe::UnsubscribeProperties;
    type UnSubAckProperties = crate::unsuback::UnSubAckDataV5;
    type PublishProperties = crate::publish::PublishProperties;
    type ConnectProperties = crate::connect::ConnectProperties;
    type LastWill = crate::connect::MqttLastWill5_0_0;
}

impl PacketProperties for () {
    fn try_read(_data: &mut Bytes) -> Result<Self, Error> {
        Ok(())
    }
    fn write_properties(&self, _buf: &mut impl BufMut) {}
    fn properties_len(&self) -> usize {
        0
    }
    fn properties_block_len(&self) -> usize {
        0
    }
}

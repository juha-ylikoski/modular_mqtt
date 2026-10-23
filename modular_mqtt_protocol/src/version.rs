use std::fmt::Debug;

use bytes::{BufMut, Bytes};

use crate::{
    disconnect::MaybeReasonCode, publish::PublishPropertiesTrait, suback::SubAckRcs,
    unsuback::ValidUnsubAckData, util::variable_len_int_size, Error, MqttV5_0_0,
};

mod sealed {
    use crate::{MqttV3_1_1, MqttV5_0_0};

    pub trait Sealed {}

    impl Sealed for MqttV3_1_1 {}
    impl Sealed for MqttV5_0_0 {}
}

pub trait PacketProperties: Sized + PartialEq + Debug + Send + Sync + Clone {
    fn try_read(data: &mut Bytes) -> Result<Self, Error>;
    fn write_properties(&self, buf: &mut impl BufMut);
    fn properties_len(&self) -> usize;
    fn properties_block_len(&self) -> usize {
        let l = self.properties_len();
        variable_len_int_size(l) + l
    }
}

pub trait IsSuccess {
    fn is_success(&self) -> bool;
}

pub trait MqttVersion:
    sealed::Sealed + std::fmt::Debug + Send + Sync + 'static + PartialEq + Clone
{
    const VERSION: u8;
    type ConnackRc: Copy
        + Into<u8>
        + TryFrom<u8, Error = Error>
        + PartialEq
        + Debug
        + IsSuccess
        + Send
        + Sync;
    type ConnackProperties: PacketProperties + Default;
    type AckTypeProperties<R: crate::ack_messages::ReasonCode>: PacketProperties + Default;
    type DisconnectData: PacketProperties + MaybeReasonCode;
    type SubscribeData: PacketProperties + Default;
    type SubAckData: PacketProperties + SubAckRcs;
    type TopicSubscription: crate::subscribe::TopicSubscription;
    type UnsubscribeProperties: PacketProperties;
    type UnSubAckProperties: ValidUnsubAckData + PacketProperties;
    type PublishProperties: PacketProperties + Default + PublishPropertiesTrait;
    type ConnectProperties: PacketProperties + Default;
    type LastWill: crate::connect::MqttLastWill;

    fn create_connect(
        clean_session: bool,
        keep_alive: u16,
        client_identifier: String,
        will: Option<Self::LastWill>,
        username: Option<String>,
        password: Option<Bytes>,
    ) -> Result<crate::Connect<Self>, Error>;
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

    fn create_connect(
        clean_session: bool,
        keep_alive: u16,
        client_identifier: String,
        will: Option<crate::MqttLastWill3_1_1>,
        username: Option<String>,
        password: Option<Bytes>,
    ) -> Result<crate::Connect<Self>, Error> {
        if username.is_none() && password.is_some() {
            Err(Error::ProtocolError(
                "MQTT 3.1.1 requires a User Name when a Password is present",
            ))
        } else {
            Ok(crate::Connect::new(
                clean_session,
                keep_alive,
                client_identifier,
                will,
                username,
                password,
            ))
        }
    }
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

    fn create_connect(
        clean_session: bool,
        keep_alive: u16,
        client_identifier: String,
        will: Option<crate::MqttLastWill5_0_0>,
        username: Option<String>,
        password: Option<Bytes>,
    ) -> Result<crate::Connect<MqttV5_0_0>, Error> {
        Ok(crate::Connect::new(
            clean_session,
            keep_alive,
            client_identifier,
            will,
            username,
            password,
        ))
    }
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

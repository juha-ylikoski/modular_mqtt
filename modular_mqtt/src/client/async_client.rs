use bytes::{Bytes, BytesMut};
use modular_mqtt_protocol::{
    ConnAck, ControlPacketType, Disconnect, FixedHeader, MqttV3_1_1, MqttV5_0_0, MqttVersion,
    Packet, PingReq, PingResp, PubAck, PubComp, PubRec, PubRel, Publish, Qos, QosPacketIdentifier,
    SubAck, Subscribe, TopicSubscription, TopicSubscriptionV3, TopicSubscriptionV5, UnsubAck,
    Unsubscribe,
};
use std::{
    collections::HashMap,
    io::{Read, Write},
    net::TcpStream,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};
use tracing::instrument;

use crate::{
    client_communication::{
        async_communicator::{AsyncData, AsyncWakeup},
        ClientCommunicator, SyncData, SyncWakeup,
    },
    client_opts::{exponential_backoff, ClientOpts, MqttOptions, OnDisconnectBehavior},
    connection::{async_stream::AsyncWriter, SyncReader, SyncWriter, Writer},
    error::{BackendError, ClientError, ConnectError},
    util::{InflightMessage, InflightMessageState, IntoTopicSubscription},
};

use tokio::{
    io::AsyncWriteExt,
    sync::{mpsc, Mutex},
};

#[derive(Clone)]
pub struct Client<V>
where
    V: MqttVersion + MqttOptions,
{
    write_buf: Arc<Mutex<BytesMut>>,
    opts: Arc<ClientOpts<V>>,
    next_packet_identifier: Arc<std::sync::atomic::AtomicU16>,
    writer: Arc<Mutex<AsyncWriter>>,
    backend: Arc<Mutex<Option<std::thread::JoinHandle<Result<(), crate::error::BackendError>>>>>,
    suback_comm: ClientCommunicator<AsyncData<SubAck<V>>, AsyncWakeup>,
    unsuback_comm: ClientCommunicator<AsyncData<UnsubAck<V>>, AsyncWakeup>,
    inflight_ch: mpsc::Sender<(u16, Arc<InflightMessage<V>>)>,
    online: Arc<RwLock<bool>>,
    kill_bg_thread: Arc<Mutex<bool>>,
    subscriptions: Arc<Mutex<Vec<V::TopicSubscription>>>,
}

impl<V> Client<V>
where
    V: MqttVersion + MqttOptions,
{
    fn next_packet_identifier(&self) -> u16 {
        self.next_packet_identifier
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }
    fn assert_online(&self) {
        if !*self.online.read().unwrap() {
            panic!("Connection to mqtt broker was disconnect. Cannot proceed.");
        }
    }
    async fn flush(&self, write_buf: &mut BytesMut) -> std::io::Result<()> {
        let mut writer = self.writer.lock().await;
        let out = write_buf.split();
        tracing::trace!("Send data: {out:?}");
        writer.write_all(&out[..]).await
    }
    async fn handle_recv_error(&self) -> ClientError {
        let be = self.backend.lock().await.take().unwrap();
        if be.is_finished() {
            return match be.join() {
                Ok(Ok(_)) => {
                    panic!("Backend thread has freed sender but did not error out");
                }
                Ok(Err(e)) => ClientError::BackendError(e),
                Err(e) => ClientError::BackendCrashed(e),
            };
        } else {
            panic!("Backend thread has freed sender but is not dead");
        }
    }

    pub fn online(&self) -> bool {
        *self.online.read().unwrap()
    }

    async fn kill_bg_thread(&self) {
        let mut killer = self.kill_bg_thread.lock().await;
        *killer = true;
    }

    async fn subscribe_with(&self, msg: Subscribe<V>) -> Result<SubAck<V>, ClientError> {
        self.assert_online();
        tracing::debug!("Sending subscribe: {msg:?}");
        {
            let mut write_buf = self.write_buf.lock().await;
            msg.write_to_buf(&mut *write_buf);
            self.flush(&mut write_buf).await?;
            drop(write_buf);
        }

        let suback = self.suback_comm.get(msg.packet_identifier()).await?;
        let mut subs = self.subscriptions.lock().await;
        for sub in msg
            .subscriptions()
            .iter()
            .zip(suback.subs_succeeded())
            .filter_map(|(sub, succeeded)| if succeeded { Some(sub) } else { None })
        {
            subs.push(sub.clone());
        }
        Ok(suback)
    }

    async fn unsubscribe_with(&self, msg: Unsubscribe<V>) -> Result<UnsubAck<V>, ClientError> {
        self.assert_online();
        tracing::debug!("Sending unsubscribe: {msg:?}");
        {
            let mut write_buf = self.write_buf.lock().await;
            msg.write_to_buf(&mut *write_buf);
            self.flush(&mut write_buf).await?;
        }
        let suback = self.unsuback_comm.get(msg.packet_identifier()).await?;
        let mut subs = self.subscriptions.lock().await;
        for topic in msg.topics() {
            if let Some(index) = subs.iter().position(|v| v.topic() == topic) {
                subs.remove(index);
            }
        }
        Ok(suback)
    }

    async fn disconnect_with(self, packet: Disconnect<V>) -> Result<(), ClientError> {
        let mut write_buf = self.write_buf.lock().await;
        self.assert_online();
        tracing::debug!("Sending Disconnect");
        self.kill_bg_thread().await;

        packet.write_to_buf(&mut *write_buf);
        self.flush(&mut write_buf).await?;
        let mut stream = self.writer.lock().await;
        let old_stream = std::mem::replace(&mut *stream, AsyncWriter::Disconnected);
        drop(stream);
        if let AsyncWriter::Tcp(mut tcp) = old_stream {
            // Unblocks the background thread's in-progress socket read (which can otherwise
            // block for up to `keep_alive` seconds) so it notices `kill_bg_thread` promptly.
            let _ = tcp.shutdown().await;
        }

        let mut backend = self.backend.lock().await;
        let backend = backend.take();
        if let Some(backend) = backend {
            backend.join().unwrap().unwrap();
        }
        Ok(())
    }
}

impl<V> Client<V>
where
    V: MqttVersion + MqttOptions,
{
    pub async fn publish(
        &self,
        msg: Publish<V, Qos>,
    ) -> Result<Option<Arc<InflightMessage<V>>>, ClientError> {
        let mut write_buf = self.write_buf.lock().await;
        self.assert_online();

        let msg = msg.assign_packet_identifier(
            || {
                self.next_packet_identifier
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            },
            false,
        );
        tracing::debug!("Sending message: {msg:?}");

        let mut inflight = None;
        if let Some(packet_identifier) = msg.packet_identifier() {
            let _inflight = Arc::new(match msg.qos() {
                Qos::AtMostOnce => unreachable!(),
                Qos::AtLeastOnce => InflightMessage {
                    state: RwLock::new(InflightMessageState::PubAck(Instant::now())),
                    packet_identifier,
                    msg,
                },
                Qos::ExactlyOnce => InflightMessage {
                    state: RwLock::new(InflightMessageState::PubRec(Instant::now())),
                    packet_identifier,
                    msg,
                },
            });

            _inflight.msg.write_to_buf(&mut *write_buf);

            if self
                .inflight_ch
                .send((packet_identifier, _inflight.clone()))
                .await
                .is_err()
            {
                return Err(self.handle_recv_error().await);
            }
            inflight = Some(_inflight);
        } else {
            msg.write_to_buf(&mut *write_buf);
        }
        self.flush(&mut write_buf).await?;
        Ok(inflight)
    }
}

impl Client<MqttV5_0_0> {
    pub async fn connect(
        opts: ClientOpts<MqttV5_0_0>,
        broker: String,
    ) -> Result<
        (
            std::sync::mpsc::Receiver<Publish<MqttV5_0_0, QosPacketIdentifier>>,
            Self,
        ),
        ConnectError,
    > {
        todo!()
    }
    pub async fn subscribe(
        &self,
        topics: Vec<impl IntoTopicSubscription<MqttV5_0_0>>,
        qos: Qos,
    ) -> Result<SubAck<MqttV5_0_0>, ClientError> {
        let subs = topics
            .into_iter()
            .map(|topic| {
                topic.into_topic_subscription(
                    qos,
                    self.opts.extra_opts.subscription_no_local,
                    self.opts.extra_opts.subscription_keep_retain,
                    self.opts.extra_opts.subscription_retain_handling,
                )
            })
            .collect();

        let packet_identifier = self.next_packet_identifier();
        let msg = Subscribe::new_with_options(packet_identifier, subs, None, Vec::new());
        self.subscribe_with(msg).await
    }
    pub async fn unsubscribe(
        &self,
        topics: Vec<String>,
    ) -> Result<UnsubAck<MqttV5_0_0>, ClientError> {
        let packet_identifier = self.next_packet_identifier();
        let msg = Unsubscribe::new_v5(packet_identifier, topics.clone(), Vec::new());
        self.unsubscribe_with(msg).await
    }

    pub async fn disconnect(self) -> Result<(), ClientError> {
        self.disconnect_with(Disconnect::new_v5(
            modular_mqtt_protocol::DisconnectReasonCode::Normal,
            None,
            None,
            Vec::new(),
            None,
        ))
        .await
    }
}

impl Client<MqttV3_1_1> {
    pub async fn subscribe(
        &self,
        topics: Vec<impl IntoTopicSubscription<MqttV3_1_1>>,
        qos: Qos,
    ) -> Result<SubAck<MqttV3_1_1>, ClientError> {
        let subs = topics
            .into_iter()
            .map(|topic| {
                topic.into_topic_subscription(
                    qos,
                    false,
                    false,
                    modular_mqtt_protocol::RetainHandling::SendAtSubscribe,
                )
            })
            .collect();

        let packet_identifier = self.next_packet_identifier();
        let msg = Subscribe::<MqttV3_1_1>::new(packet_identifier, subs);
        self.subscribe_with(msg).await
    }
    pub async fn unsubscribe(
        &self,
        topics: Vec<String>,
    ) -> Result<UnsubAck<MqttV3_1_1>, ClientError> {
        let packet_identifier = self.next_packet_identifier();
        let msg = Unsubscribe::new_v3(packet_identifier, topics.clone());
        self.unsubscribe_with(msg).await
    }

    pub async fn disconnect(self) -> Result<(), ClientError> {
        self.disconnect_with(Disconnect::new_v3()).await
    }
}

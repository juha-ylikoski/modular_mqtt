use bytes::BytesMut;
use modular_mqtt_protocol::{
    Disconnect, MqttV3_1_1, MqttV5_0_0, MqttVersion, Packet, Publish, Qos, QosPacketIdentifier,
    SubAck, Subscribe, TopicSubscription, UnsubAck, Unsubscribe,
};
use std::{
    collections::HashMap,
    io::Write,
    net::TcpStream,
    sync::{mpsc, Arc, Mutex, RwLock},
    time::{Duration, Instant},
};

use crate::{
    client_communication::{ClientCommunicator, SyncData, SyncWakeup},
    client_opts::{ClientOpts, ClientOptsV3, ClientOptsV5},
    connection::{SyncReader, SyncWriter},
    error::{ClientError, ConnectError},
    util::{
        connect_sync, InflightMessage, InflightMessageState, IntoTopicSubscription,
        STREAM_READ_CHUNK_SIZE,
    },
};

#[derive(Clone)]
pub struct Client<V, O>
where
    V: MqttVersion,
    O: ClientOpts<V>,
{
    write_buf: Arc<Mutex<BytesMut>>,
    opts: Arc<O>,
    next_packet_identifier: Arc<std::sync::atomic::AtomicU16>,
    writer: Arc<Mutex<SyncWriter>>,
    backend: Arc<Mutex<Option<std::thread::JoinHandle<Result<(), crate::error::BackendError>>>>>,
    suback_comm: ClientCommunicator<SyncData<SubAck<V>>, SyncWakeup>,
    unsuback_comm: ClientCommunicator<SyncData<UnsubAck<V>>, SyncWakeup>,
    inflight_ch: std::sync::mpsc::Sender<(u16, Arc<InflightMessage<V>>)>,
    online: Arc<RwLock<bool>>,
    kill_bg_thread: Arc<Mutex<bool>>,
    subscriptions: Arc<Mutex<Vec<V::TopicSubscription>>>,
}

impl<V, O> Client<V, O>
where
    V: MqttVersion,
    O: ClientOpts<V>,
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
    fn flush(&self, write_buf: &mut BytesMut) -> std::io::Result<()> {
        let mut writer = self.writer.lock().unwrap();
        let out = write_buf.split();
        tracing::trace!("Send data: {out:?}");
        writer.write_all(&out[..])
    }
    fn handle_recv_error(&self) -> ClientError {
        let be = self.backend.lock().unwrap().take().unwrap();
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

    fn kill_bg_thread(&self) {
        let mut killer = self.kill_bg_thread.lock().unwrap();
        *killer = true;
    }

    fn subscribe_with(
        &self,
        msg: Subscribe<V>,
        timeout: Duration,
    ) -> Result<SubAck<V>, ClientError> {
        self.assert_online();
        tracing::debug!("Sending subscribe: {msg:?}");
        {
            let mut write_buf = self.write_buf.lock().unwrap();
            msg.write_to_buf(&mut *write_buf);
            self.flush(&mut write_buf)?;
            drop(write_buf);
        }

        let suback = self.suback_comm.get(msg.packet_identifier(), timeout)?;
        let mut subs = self.subscriptions.lock().unwrap();
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

    fn unsubscribe_with(
        &self,
        msg: Unsubscribe<V>,
        timeout: Duration,
    ) -> Result<UnsubAck<V>, ClientError> {
        self.assert_online();
        tracing::debug!("Sending unsubscribe: {msg:?}");
        {
            let mut write_buf = self.write_buf.lock().unwrap();
            msg.write_to_buf(&mut *write_buf);
            self.flush(&mut write_buf)?;
        }
        let suback = self.unsuback_comm.get(msg.packet_identifier(), timeout)?;
        let mut subs = self.subscriptions.lock().unwrap();
        for topic in msg.topics() {
            if let Some(index) = subs.iter().position(|v| v.topic() == topic) {
                subs.remove(index);
            }
        }
        Ok(suback)
    }

    fn disconnect_with(self, packet: Disconnect<V>) -> Result<(), ClientError> {
        let mut write_buf = self.write_buf.lock().unwrap();
        self.assert_online();
        tracing::debug!("Sending Disconnect");
        self.kill_bg_thread();

        packet.write_to_buf(&mut *write_buf);
        self.flush(&mut write_buf)?;
        let mut stream = self.writer.lock().unwrap();
        let old_stream = std::mem::replace(&mut *stream, SyncWriter::Disconnected);
        drop(stream);
        if let SyncWriter::Tcp(tcp) = old_stream {
            // Unblocks the background thread's in-progress socket read (which can otherwise
            // block for up to `keep_alive` seconds) so it notices `kill_bg_thread` promptly.
            let _ = tcp.shutdown(std::net::Shutdown::Both);
        }

        let mut backend = self.backend.lock().unwrap();
        let backend = backend.take();
        if let Some(backend) = backend {
            backend.join().unwrap().unwrap();
        }
        Ok(())
    }
}

impl<V, O> Client<V, O>
where
    V: MqttVersion,
    O: ClientOpts<V>,
{
    pub fn connect_tcp(
        opts: O,
        broker: String,
    ) -> Result<
        (
            std::sync::mpsc::Receiver<Publish<V, QosPacketIdentifier>>,
            Self,
        ),
        ConnectError,
    > {
        let stream = TcpStream::connect(&broker)?;
        let mut reader = SyncReader::Tcp(stream.try_clone().unwrap());
        let mut writer = SyncWriter::Tcp(stream);
        let opts = Arc::new(opts);
        let mut read_buf = BytesMut::with_capacity(STREAM_READ_CHUNK_SIZE);
        let mut write_buf = BytesMut::with_capacity(STREAM_READ_CHUNK_SIZE);

        reader.set_read_timeout(Some(Duration::from_secs(opts.keep_alive().into())))?;

        let (msg_sender, msg_receiver) = mpsc::channel();
        let suback_comm = ClientCommunicator::<SyncData<_>, SyncWakeup>::new(opts.ack_retention());
        let unsuback_comm =
            ClientCommunicator::<SyncData<_>, SyncWakeup>::new(opts.ack_retention());
        let (inflight_sender, inflight_receiver) = mpsc::channel();

        let online = Arc::new(RwLock::new(true));
        let killer = Arc::new(Mutex::new(false));
        let next_packet_identifier = Arc::new(std::sync::atomic::AtomicU16::new(1));

        let connack = connect_sync(
            &*opts,
            &mut read_buf,
            &mut write_buf,
            &mut reader,
            &mut writer,
        )?;

        let writer = Arc::new(Mutex::new(writer));

        let subscriptions = Arc::new(Mutex::new(Vec::new()));

        if connack.rc_is_success() {
            let bg_opts = opts.clone();
            let be = crate::backend::sync_backend::Backend {
                broker_addr: broker,
                read_buf,
                write_buf,
                opts: bg_opts,
                reader,
                writer: writer.clone(),
                msg_ch: msg_sender,
                suback_comm: suback_comm.clone(),
                unsuback_comm: unsuback_comm.clone(),
                inflight_msgs: HashMap::new(),
                inflight_ch: inflight_receiver,
                online: online.clone(),
                receive_inflight: Vec::new(),
                next_resend_deadline: None,
                should_die: killer.clone(),
                retry_count: 0,
                subscriptions: subscriptions.clone(),
                next_packet_identifier: next_packet_identifier.clone(),
            };
            let backend = Arc::new(Mutex::new(Some(std::thread::spawn(|| be.bg_thread()))));
            let client = Self {
                write_buf: Arc::new(Mutex::new(BytesMut::with_capacity(STREAM_READ_CHUNK_SIZE))),
                opts,
                writer,
                next_packet_identifier,
                backend,
                suback_comm,
                unsuback_comm,
                inflight_ch: inflight_sender,
                online,
                kill_bg_thread: killer,
                subscriptions,
            };
            Ok((msg_receiver, client))
        } else {
            Err(O::connect_error(&connack))
        }
    }

    pub fn publish(
        &self,
        msg: Publish<V, Qos>,
    ) -> Result<Option<Arc<InflightMessage<V>>>, ClientError> {
        let mut write_buf = self.write_buf.lock().unwrap();
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
                .is_err()
            {
                return Err(self.handle_recv_error());
            }
            inflight = Some(_inflight);
        } else {
            msg.write_to_buf(&mut *write_buf);
        }
        self.flush(&mut write_buf)?;
        Ok(inflight)
    }
}

impl Client<MqttV5_0_0, ClientOptsV5> {
    pub fn subscribe(
        &self,
        topics: Vec<impl IntoTopicSubscription<MqttV5_0_0>>,
        qos: Qos,
        timeout: Duration,
    ) -> Result<SubAck<MqttV5_0_0>, ClientError> {
        let subs = topics
            .into_iter()
            .map(|topic| {
                topic.into_topic_subscription(
                    qos,
                    self.opts.subscription_no_local,
                    self.opts.subscription_keep_retain,
                    self.opts.subscription_retain_handling,
                )
            })
            .collect();

        let packet_identifier = self.next_packet_identifier();
        let msg = Subscribe::new_with_options(packet_identifier, subs, None, Vec::new());
        self.subscribe_with(msg, timeout)
    }
    pub fn unsubscribe(
        &self,
        topics: Vec<String>,
        timeout: Duration,
    ) -> Result<UnsubAck<MqttV5_0_0>, ClientError> {
        let packet_identifier = self.next_packet_identifier();
        let msg = Unsubscribe::new_v5(packet_identifier, topics.clone(), Vec::new());
        self.unsubscribe_with(msg, timeout)
    }

    pub fn disconnect(self) -> Result<(), ClientError> {
        self.disconnect_with(Disconnect::new_v5(
            modular_mqtt_protocol::DisconnectReasonCode::Normal,
            None,
            None,
            Vec::new(),
            None,
        ))
    }
}

impl Client<MqttV3_1_1, ClientOptsV3> {
    pub fn subscribe(
        &self,
        topics: Vec<impl IntoTopicSubscription<MqttV3_1_1>>,
        qos: Qos,
        timeout: Duration,
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
        self.subscribe_with(msg, timeout)
    }
    pub fn unsubscribe(
        &self,
        topics: Vec<String>,
        timeout: Duration,
    ) -> Result<UnsubAck<MqttV3_1_1>, ClientError> {
        let packet_identifier = self.next_packet_identifier();
        let msg = Unsubscribe::new_v3(packet_identifier, topics.clone());
        self.unsubscribe_with(msg, timeout)
    }

    pub fn disconnect(self) -> Result<(), ClientError> {
        self.disconnect_with(Disconnect::new_v3())
    }
}

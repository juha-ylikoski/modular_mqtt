use bytes::BytesMut;
use modular_mqtt_protocol::{
    MqttVersion, Packet, Publish, Qos, QosPacketIdentifier, SubAck, TopicSubscription, UnsubAck,
};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::Duration,
};

use crate::{
    backend::Shared,
    client_communication::{
        async_communicator::{AsyncData, AsyncWakeup},
        ClientCommunicator,
    },
    client_opts::ClientOpts,
    connection::async_stream::{AsyncReader, AsyncWriter},
    error::{ClientError, ConnectError},
    util::{
        connect_async, InflightMessage, InflightMessageState, IntoTopicSubscription,
        STREAM_READ_CHUNK_SIZE,
    },
    Instant,
};

use tokio::{
    io::AsyncWriteExt,
    net::TcpStream,
    sync::{mpsc, Mutex},
};

type Backend = tokio::task::JoinHandle<Result<(), crate::error::BackendError>>;

#[derive(Clone)]
pub struct Client<V, O>
where
    V: MqttVersion,
    O: ClientOpts<V>,
{
    write_buf: Arc<Mutex<BytesMut>>,
    writer: Arc<Mutex<AsyncWriter>>,
    backend: Arc<Mutex<Option<Backend>>>,
    suback_comm: ClientCommunicator<AsyncData<SubAck<V>>, AsyncWakeup>,
    unsuback_comm: ClientCommunicator<AsyncData<UnsubAck<V>>, AsyncWakeup>,
    inflight_ch: mpsc::Sender<(u16, Arc<InflightMessage<V, crate::util::Async>>)>,

    shared: Arc<Shared<V, O>>,
}

impl<V, O> Client<V, O>
where
    V: MqttVersion,
    O: ClientOpts<V>,
{
    fn next_packet_identifier(&self) -> u16 {
        self.shared
            .next_packet_identifier
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }
    fn assert_online(&self) {
        if !*self.shared.online.read().unwrap() {
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
            match be.await {
                Ok(Ok(_)) => {
                    panic!("Backend thread has freed sender but did not error out");
                }
                Ok(Err(e)) => ClientError::BackendError(e),
                Err(e) => ClientError::BackendCrashed(Box::new(e)),
            }
        } else {
            panic!("Backend thread has freed sender but is not dead");
        }
    }

    pub fn online(&self) -> bool {
        *self.shared.online.read().unwrap()
    }
}

impl<V, O> Client<V, O>
where
    V: MqttVersion,
    O: ClientOpts<V>,
{
    pub async fn connect(
        opts: O,
        broker: String,
    ) -> Result<(mpsc::Receiver<Publish<V, QosPacketIdentifier>>, Self), ConnectError> {
        let stream = TcpStream::connect(&broker).await?;
        let (read_half, write_half) = stream.into_split();
        let mut reader = AsyncReader::Tcp(read_half);
        let mut writer = AsyncWriter::Tcp(write_half);
        let mut read_buf = BytesMut::with_capacity(STREAM_READ_CHUNK_SIZE);
        let mut write_buf = BytesMut::with_capacity(STREAM_READ_CHUNK_SIZE);

        let (msg_sender, msg_receiver) = mpsc::channel(100);
        let suback_comm =
            ClientCommunicator::<AsyncData<_>, AsyncWakeup>::new(opts.ack_retention());
        let unsuback_comm =
            ClientCommunicator::<AsyncData<_>, AsyncWakeup>::new(opts.ack_retention());
        let (inflight_sender, inflight_receiver) = mpsc::channel(100);

        let connack = match tokio::time::timeout(
            Duration::from_secs(opts.keep_alive().into()),
            connect_async(
                &opts,
                &mut read_buf,
                &mut write_buf,
                &mut reader,
                &mut writer,
            ),
        )
        .await
        {
            Ok(Ok(connack)) => Ok(connack),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(ConnectError::IoError(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "Timed out waiting for connack",
            ))),
        }?;

        let writer = Arc::new(Mutex::new(writer));

        let shared = Arc::new(Shared {
            broker_addr: broker,
            opts,
            online: RwLock::new(true),
            subscriptions: std::sync::Mutex::new(Vec::new()),
            next_packet_identifier: std::sync::atomic::AtomicU16::new(1),
        });

        if connack.rc_is_success() {
            let be = crate::backend::async_backend::Backend {
                read_buf,
                reader,
                writer: writer.clone(),
                msg_ch: msg_sender,
                suback_comm: suback_comm.clone(),
                unsuback_comm: unsuback_comm.clone(),
                inflight_ch: inflight_receiver,
                retry_count: 0,
                state_machine: crate::backend::BackendStateMachine {
                    version: std::marker::PhantomData,
                    receive_inflight: Vec::new(),
                    write_buf,
                    inflight_msgs: HashMap::new(),
                    next_resend_deadline: None,
                    shared: shared.clone(),
                },
            };
            let backend = Arc::new(Mutex::new(Some(tokio::task::spawn(be.bg_task()))));
            let client = Self {
                write_buf: Arc::new(Mutex::new(BytesMut::with_capacity(STREAM_READ_CHUNK_SIZE))),
                writer,
                backend,
                suback_comm,
                unsuback_comm,
                inflight_ch: inflight_sender,
                shared,
            };
            Ok((msg_receiver, client))
        } else {
            Err(O::connect_error(&connack))
        }
    }
    pub async fn publish(
        &self,
        msg: Publish<V, Qos>,
    ) -> Result<Option<Arc<InflightMessage<V, crate::util::Async>>>, ClientError> {
        let mut write_buf = self.write_buf.lock().await;
        self.assert_online();

        let msg = msg.assign_packet_identifier(
            || {
                self.shared
                    .next_packet_identifier
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
                    state: std::sync::Mutex::new(InflightMessageState::PubAck(Instant::now())),
                    delivered: tokio::sync::Notify::new(),
                    packet_identifier,
                    msg,
                },
                Qos::ExactlyOnce => InflightMessage {
                    state: std::sync::Mutex::new(InflightMessageState::PubRec(Instant::now())),
                    delivered: tokio::sync::Notify::new(),
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

    pub async fn subscribe(
        &self,
        topics: Vec<impl IntoTopicSubscription<V>>,
        qos: Qos,
    ) -> Result<SubAck<V>, ClientError> {
        self.assert_online();
        let subs = topics
            .into_iter()
            .map(|topic| self.shared.opts.topic_subscription(topic, qos))
            .collect();

        let packet_identifier = self.next_packet_identifier();
        let msg = self.shared.opts.subscribe_packet(packet_identifier, subs);

        tracing::debug!("Sending subscribe: {msg:?}");
        {
            let mut write_buf = self.write_buf.lock().await;
            msg.write_to_buf(&mut *write_buf);
            self.flush(&mut write_buf).await?;
            drop(write_buf);
        }

        let suback = self.suback_comm.get(msg.packet_identifier()).await?;
        let mut subs = self.shared.subscriptions.lock().unwrap();
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

    pub async fn unsubscribe(&self, topics: Vec<String>) -> Result<UnsubAck<V>, ClientError> {
        self.assert_online();
        let packet_identifier = self.next_packet_identifier();
        let msg = self
            .shared
            .opts
            .unsubscribe_packet(packet_identifier, topics.clone());
        tracing::debug!("Sending unsubscribe: {msg:?}");
        {
            let mut write_buf = self.write_buf.lock().await;
            msg.write_to_buf(&mut *write_buf);
            self.flush(&mut write_buf).await?;
        }
        let suback = self.unsuback_comm.get(msg.packet_identifier()).await?;
        let mut subs = self.shared.subscriptions.lock().unwrap();
        for topic in msg.topics() {
            if let Some(index) = subs.iter().position(|v| v.topic() == topic) {
                subs.remove(index);
            }
        }
        Ok(suback)
    }

    pub async fn disconnect(self) -> Result<(), ClientError> {
        let mut write_buf = self.write_buf.lock().await;
        self.assert_online();
        tracing::debug!("Sending Disconnect");

        self.shared
            .opts
            .disconnect_packet()
            .write_to_buf(&mut *write_buf);
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
            backend.abort();
        }
        Ok(())
    }
}

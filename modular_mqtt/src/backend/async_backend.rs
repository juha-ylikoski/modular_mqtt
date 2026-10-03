use crate::{
    client_communication::async_communicator::{AsyncData, AsyncWakeup},
    connection::async_stream::{AsyncReader, AsyncWriter},
    util::{connect_async, read_into_buf_async},
};
use bytes::{Bytes, BytesMut};
use modular_mqtt_protocol::{
    FixedHeader, MqttVersion, Packet, PingReq, Publish, QosPacketIdentifier, SubAck, Subscribe,
    UnsubAck,
};
use std::{sync::Arc, time::Duration};
use tracing::instrument;

use crate::{
    client_communication::ClientCommunicator,
    client_opts::{exponential_backoff, ClientOpts, OnDisconnectBehavior},
    error::BackendError,
    util::InflightMessage,
};

use tokio::sync::{mpsc, Mutex};
use tokio::{io::AsyncWriteExt, net::TcpStream};

use super::{Action, BackendStateMachine};

enum ReadFinished {
    Success(FixedHeader, Bytes),
    TimedOut,
}

pub struct Backend<V, O>
where
    V: MqttVersion,
    O: ClientOpts<V>,
{
    pub read_buf: BytesMut,
    pub reader: AsyncReader,
    pub writer: Arc<Mutex<AsyncWriter>>,
    pub msg_ch: mpsc::Sender<Publish<V, QosPacketIdentifier>>,
    pub suback_comm: ClientCommunicator<AsyncData<SubAck<V>>, AsyncWakeup>,
    pub unsuback_comm: ClientCommunicator<AsyncData<UnsubAck<V>>, AsyncWakeup>,
    pub inflight_ch: mpsc::Receiver<(u16, Arc<InflightMessage<V, crate::util::Async>>)>,
    pub retry_count: u32,

    pub state_machine: BackendStateMachine<V, O, crate::util::Async>,
}

impl<V, O> Backend<V, O>
where
    V: MqttVersion,
    O: ClientOpts<V>,
{
    #[instrument(skip_all, fields(client_id=%self.state_machine.shared.opts.client_id()))]
    pub async fn bg_task(self) -> Result<(), BackendError> {
        let res = self.task().await;
        if res.is_ok() {
            tracing::info!("Background thread exited gracefully");
        } else {
            tracing::error!("Background thread exited due to {res:?}");
        }
        res
    }

    async fn prune_communicators(&mut self) {
        self.unsuback_comm.prune().await;
        self.unsuback_comm.prune().await;
    }

    async fn task(mut self) -> Result<(), BackendError> {
        let mut i = 0;
        loop {
            tracing::debug!("Start listening for mqtt messages");
            let mut res = self.loop_bg_task().await;

            match self.state_machine.shared.opts.on_disconnect() {
                OnDisconnectBehavior::Panic => return res,
                OnDisconnectBehavior::ReconnectExponentialBackoff {
                    min_retry_interval,
                    max_retry_interval,
                } => loop {
                    if let Err(BackendError::IoError(e)) = &res {
                        self.retry_count += 1;
                        tracing::warn!(
                            "Got io error: {e}. Trying to reconnect after {min_retry_interval:?}."
                        );
                        let wait = exponential_backoff(
                            min_retry_interval,
                            max_retry_interval,
                            self.retry_count,
                        );
                        tokio::time::sleep(wait).await;
                        res = self.reconnect().await;
                        if res.is_ok() {
                            tracing::info!("Successfully reconnected!");
                            self.retry_count = 0;
                            break;
                        }
                    } else {
                        return res;
                    }
                },
            }
            if i > 50 {
                self.prune_communicators().await;
                i = 0;
            }
            i += 1;
        }
    }

    async fn reconnect(&mut self) -> Result<(), BackendError> {
        tracing::info!("Reconnecting to mqtt broker");
        self.read_buf.clear();
        self.state_machine.write_buf.clear();
        let stream = TcpStream::connect(&self.state_machine.shared.broker_addr).await?;
        let (read_half, write_half) = stream.into_split();
        let mut reader = AsyncReader::Tcp(read_half);
        let mut writer = AsyncWriter::Tcp(write_half);

        let connack = match tokio::time::timeout(
            Duration::from_secs(self.state_machine.shared.opts.keep_alive().into()),
            connect_async(
                &self.state_machine.shared.opts,
                &mut self.read_buf,
                &mut self.state_machine.write_buf,
                &mut reader,
                &mut writer,
            ),
        )
        .await
        {
            Ok(Ok(connack)) => Ok(connack),
            Ok(Err(e)) => Err(BackendError::ReconnectError(e)),
            Err(_) => Err(BackendError::IoError(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "Timed out waiting for connack",
            ))),
        }?;

        if !connack.rc_is_success() {
            return Err(BackendError::ReconnectError(O::connect_error(&connack)));
        }

        if self.state_machine.shared.opts.should_resubscribe(&connack) {
            let subs = self
                .state_machine
                .shared
                .subscriptions
                .lock()
                .unwrap()
                .clone();
            if !subs.is_empty() {
                let msg = Subscribe::<V>::new(
                    self.state_machine
                        .shared
                        .next_packet_identifier
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                    subs,
                );
                msg.write_to_buf(&mut self.state_machine.write_buf);
            }
        }

        self.state_machine.re_write_qos1_and_qos2_to_write_buf();
        *self.writer.lock().await = writer;
        self.reader = reader;
        self.write_buf_to_stream().await?;
        Ok(())
    }

    async fn loop_bg_task(&mut self) -> Result<(), BackendError> {
        loop {
            self.state_machine.check_resend_msgs();
            loop {
                match self.inflight_ch.try_recv() {
                    Ok((packet_identifier, msg)) => {
                        self.state_machine
                            .inflight_msgs
                            .insert(packet_identifier, msg);
                    }
                    Err(mpsc::error::TryRecvError::Empty) => break,
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        return Err(BackendError::ChannelError)
                    }
                }
            }
            self.write_buf_to_stream().await?;
            match self.read_next_timeout().await? {
                ReadFinished::Success(fixed_header, buf) => {
                    match self.state_machine.handle_msg(fixed_header, buf)? {
                        Action::None => (),
                        Action::SubAckSend(sub_ack) => {
                            self.suback_comm
                                .insert(sub_ack.packet_identifier(), sub_ack)
                                .await
                        }
                        Action::UnsubAckSend(unsub_ack) => {
                            self.unsuback_comm
                                .insert(unsub_ack.packet_identifier(), unsub_ack)
                                .await
                        }
                        Action::ReceiveMsg(publish) => self.msg_ch.send(publish).await?,
                        Action::ServerDisconnect(dc) => {
                            *self.state_machine.shared.online.write().unwrap() = false;
                            return Err(BackendError::Disconnected(dc.maybe_reason_code()));
                        }
                        Action::WakeupClient(msg) => {
                            msg.mark_delivered();
                        }
                    }
                }
                ReadFinished::TimedOut => {
                    tracing::debug!("Sending ping request to broker");
                    PingReq.write_to_buf(&mut self.state_machine.write_buf);
                }
            }
            self.write_buf_to_stream().await?;
        }
    }

    async fn write_buf_to_stream(&mut self) -> std::io::Result<()> {
        if self.state_machine.write_buf.is_empty() {
            return Ok(());
        }

        let mut writer = self.writer.lock().await;
        let write = self.state_machine.write_buf.split();
        tracing::trace!("Send data: {write:?}");
        writer.write_all(&write[..]).await?;
        writer.flush().await
    }

    async fn read_next_msg(&mut self) -> Result<Option<(FixedHeader, Bytes)>, BackendError> {
        // Check if we already have a packet in buffer
        if !self.read_buf.is_empty() {
            tracing::trace!("Buffer has data {:?}. Try to parse it", self.read_buf);
            match FixedHeader::parse(
                &mut self.read_buf,
                self.state_machine.shared.opts.max_packet_size(),
            ) {
                Ok(Some(v)) => return Ok(Some(v)),
                Err(e) => return Err(BackendError::MqttError(e)),
                Ok(None) => (),
            }
        }

        match read_into_buf_async(
            &mut self.reader,
            &mut self.read_buf,
            self.state_machine.next_read_timeout(),
        )
        .await
        {
            Ok(Ok(0)) => Err(BackendError::IoError(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "Stream disconnected",
            ))),
            Ok(Ok(_)) => FixedHeader::parse(
                &mut self.read_buf,
                self.state_machine.shared.opts.max_packet_size(),
            )
            .map_err(BackendError::MqttError),
            Ok(Err(e)) => Err(BackendError::IoError(e)),
            Err(_) => Ok(None),
        }
    }

    async fn read_next_timeout(&mut self) -> Result<ReadFinished, BackendError> {
        match self.read_next_msg().await {
            Ok(Some((header, buf))) => {
                tracing::trace!("Received new packet: header={header:?} body={buf:?}");
                Ok(ReadFinished::Success(header, buf))
            }
            Ok(None) => {
                tracing::trace!("Read timed out");
                Ok(ReadFinished::TimedOut)
            }
            Err(e) => Err(e),
        }
    }
}

use bytes::{Bytes, BytesMut};
use modular_mqtt_protocol::{
    FixedHeader, MqttVersion, Packet, PingReq, Publish, QosPacketIdentifier, SubAck, Subscribe,
    UnsubAck,
};
use std::{
    io::Write,
    net::TcpStream,
    sync::{mpsc, Arc, Mutex},
    time::Duration,
};
use tracing::instrument;

use crate::{
    client_communication::{ClientCommunicator, SyncData, SyncWakeup},
    client_opts::{exponential_backoff, ClientOpts, OnDisconnectBehavior},
    connection::{SyncReader, SyncWriter},
    error::{BackendError, ConnectError},
    util::{connect_sync, read_into_buf, InflightMessage},
};

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
    pub reader: SyncReader,
    pub writer: Arc<Mutex<SyncWriter>>,
    pub msg_ch: mpsc::Sender<Publish<V, QosPacketIdentifier>>,
    pub suback_comm: ClientCommunicator<SyncData<SubAck<V>>, SyncWakeup>,
    pub unsuback_comm: ClientCommunicator<SyncData<UnsubAck<V>>, SyncWakeup>,
    pub inflight_ch: mpsc::Receiver<(u16, Arc<InflightMessage<V, crate::util::Sync>>)>,
    pub retry_count: u32,

    pub backend_killer: Arc<Mutex<bool>>,

    pub state_machine: BackendStateMachine<V, O, crate::util::Sync>,
}

impl<V, O> Backend<V, O>
where
    V: MqttVersion,
    O: ClientOpts<V>,
{
    #[instrument(skip_all,fields(client_id=%self.state_machine.shared.opts.client_id()))]
    pub fn bg_thread(self) -> Result<(), BackendError> {
        let res = self.run_thread();
        if res.is_ok() {
            tracing::info!("Background thread exited gracefully");
        } else {
            tracing::error!("Background thread exited due to {res:?}");
        }
        res
    }

    fn run_thread(mut self) -> Result<(), BackendError> {
        loop {
            tracing::debug!("Start listening for mqtt messages");
            let mut res = self.loop_bg_thread();
            if res.is_err() && *self.backend_killer.lock().unwrap() {
                tracing::info!("Shutting down!");
                return Ok(());
            }

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
                        std::thread::sleep(wait);
                        res = self.reconnect();
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
        }
    }

    fn reconnect(&mut self) -> Result<(), BackendError> {
        tracing::info!("Reconnecting to mqtt broker");
        self.read_buf.clear();
        self.state_machine.write_buf.clear();
        let stream = TcpStream::connect(&self.state_machine.shared.broker_addr)?;
        let mut reader = SyncReader::Tcp(stream.try_clone().unwrap());
        reader.set_read_timeout(Some(Duration::from_secs(
            self.state_machine.shared.opts.keep_alive().into(),
        )))?;
        let mut writer = SyncWriter::Tcp(stream);

        let connack = connect_sync(
            &self.state_machine.shared.opts,
            &mut self.read_buf,
            &mut self.state_machine.write_buf,
            &mut reader,
            &mut writer,
        )
        .map_err(|e| {
            if let ConnectError::IoError(e) = e {
                BackendError::IoError(e)
            } else {
                BackendError::ReconnectError(e)
            }
        })?;
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
        *self.writer.lock().unwrap() = writer;
        self.reader = reader;
        self.write_buf_to_stream()?;
        Ok(())
    }

    fn prune_communicators(&mut self) {
        self.unsuback_comm.prune();
        self.unsuback_comm.prune();
    }

    fn loop_bg_thread(&mut self) -> Result<(), BackendError> {
        let mut i = 0;
        loop {
            self.state_machine.check_resend_msgs();
            loop {
                match self.inflight_ch.try_recv() {
                    Ok((packet_identifier, msg)) => {
                        self.state_machine
                            .inflight_msgs
                            .insert(packet_identifier, msg);
                    }
                    Err(mpsc::TryRecvError::Empty) => break,
                    Err(mpsc::TryRecvError::Disconnected) => {
                        return Err(BackendError::ChannelError)
                    }
                }
            }
            self.write_buf_to_stream()?;
            match self.read_next_timeout()? {
                ReadFinished::Success(fixed_header, buf) => {
                    match self.state_machine.handle_msg(fixed_header, buf)? {
                        Action::None => (),
                        Action::SubAckSend(sub_ack) => self
                            .suback_comm
                            .insert(sub_ack.packet_identifier(), sub_ack),
                        Action::UnsubAckSend(unsub_ack) => self
                            .unsuback_comm
                            .insert(unsub_ack.packet_identifier(), unsub_ack),
                        Action::ReceiveMsg(publish) => self.msg_ch.send(publish)?,
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
            self.write_buf_to_stream()?;

            if i > 10 {
                self.prune_communicators();
                i = 0;
            }
            i += 1;
        }
    }

    fn write_buf_to_stream(&mut self) -> std::io::Result<()> {
        if self.state_machine.write_buf.is_empty() {
            return Ok(());
        }

        let mut writer = self.writer.lock().unwrap();
        let write = self.state_machine.write_buf.split();
        tracing::trace!("Send data: {write:?}");
        writer.write_all(&write[..])?;
        writer.flush()
    }

    fn read_next_msg(&mut self) -> Result<Option<(FixedHeader, Bytes)>, BackendError> {
        if *self.backend_killer.lock().unwrap() {
            return Err(BackendError::IoError(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "Disconnected",
            )));
        }

        // Check if we already have a packet in buffer
        if !self.read_buf.is_empty() {
            tracing::trace!("Buffer has data. Try to parse it");
            match FixedHeader::parse(
                &mut self.read_buf,
                self.state_machine.shared.opts.max_packet_size(),
            ) {
                Ok(Some(v)) => return Ok(Some(v)),
                Err(e) => return Err(BackendError::MqttError(e)),
                Ok(None) => (),
            }
        }

        self.reader
            .set_read_timeout(Some(self.state_machine.next_read_timeout()))?;
        match read_into_buf(&mut self.reader, &mut self.read_buf) {
            Ok(0) => Err(BackendError::IoError(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "Stream disconnected",
            ))),
            Ok(_) => FixedHeader::parse(
                &mut self.read_buf,
                self.state_machine.shared.opts.max_packet_size(),
            )
            .map_err(BackendError::MqttError),
            Err(e) => match e.kind() {
                std::io::ErrorKind::WouldBlock => Ok(None),
                _ => Err(e)?,
            },
        }
    }

    fn read_next_timeout(&mut self) -> Result<ReadFinished, BackendError> {
        match self.read_next_msg() {
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

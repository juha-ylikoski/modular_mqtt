use std::{
    io::{Read, Write},
    net::TcpStream,
    time::Duration,
};

pub enum SyncReader {
    Tcp(TcpStream),
    Disconnected,
}

pub enum SyncWriter {
    Tcp(TcpStream),
    Disconnected,
}

impl Read for SyncReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            SyncReader::Tcp(reader) => reader.read(buf),
            SyncReader::Disconnected => Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "Client disconnected",
            )),
        }
    }
}

impl SyncReader {
    pub fn set_read_timeout(&self, dur: Option<Duration>) -> Result<(), std::io::Error> {
        match self {
            SyncReader::Tcp(reader) => reader.set_read_timeout(dur),
            SyncReader::Disconnected => Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "Client disconnected",
            )),
        }
    }
}

impl Write for SyncWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            SyncWriter::Tcp(tcp_stream) => tcp_stream.write(buf),
            SyncWriter::Disconnected => Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "Client disconnected",
            )),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            SyncWriter::Tcp(tcp_stream) => tcp_stream.flush(),
            SyncWriter::Disconnected => Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "Client disconnected",
            )),
        }
    }
}

#[cfg(feature = "async")]
pub mod async_stream {
    use super::Writer;

    pub enum AsyncReader {
        Tcp(tokio::net::tcp::OwnedReadHalf),
        Disconnected,
    }
    pub enum AsyncWriter {
        Tcp(tokio::net::tcp::OwnedWriteHalf),
        Disconnected,
    }

    impl Writer for AsyncWriter {
        type MpscSender<T> = tokio::sync::mpsc::Sender<T>;
        type MpscReceiver<T> = tokio::sync::mpsc::Receiver<T>;
        type CommunicatorData<T> = crate::client_communication::async_communicator::AsyncData<T>;
        type CommunicatorWakeup = crate::client_communication::async_communicator::AsyncWakeup;
        type BgTask = tokio::task::JoinHandle<Result<(), crate::error::BackendError>>;
        type Mutex<T> = tokio::sync::Mutex<T>;
        type RwLock<T> = tokio::sync::RwLock<T>;
    }

    impl tokio::io::AsyncRead for AsyncReader {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            match self.get_mut() {
                Self::Tcp(tcp_stream) => std::pin::pin!(tcp_stream).poll_read(cx, buf),
                Self::Disconnected => std::task::Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::NotConnected,
                    "Client disconnected",
                ))),
            }
        }
    }

    impl tokio::io::AsyncWrite for AsyncWriter {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            match self.get_mut() {
                Self::Tcp(tcp_stream) => std::pin::pin!(tcp_stream).poll_write(cx, buf),
                Self::Disconnected => std::task::Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::NotConnected,
                    "Client disconnected",
                ))),
            }
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            match self.get_mut() {
                Self::Tcp(tcp_stream) => std::pin::pin!(tcp_stream).poll_flush(cx),
                Self::Disconnected => std::task::Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::NotConnected,
                    "Client disconnected",
                ))),
            }
        }

        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            match self.get_mut() {
                Self::Tcp(tcp_stream) => std::pin::pin!(tcp_stream).poll_shutdown(cx),
                Self::Disconnected => std::task::Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::NotConnected,
                    "Client disconnected",
                ))),
            }
        }
    }
}

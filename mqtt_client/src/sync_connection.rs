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

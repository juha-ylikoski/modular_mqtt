use std::{
    io::{Read, Write},
    net::TcpStream,
    time::Duration,
};

pub trait SyncStream: Read + Write + Sized + Send {
    /// Sets the read timeout to the timeout specified.
    ///
    /// Expects this method to behave like [https://doc.rust-lang.org/std/net/struct.TcpStream.html#method.try_clone]
    fn set_read_timeout(&self, dur: Option<Duration>) -> Result<(), std::io::Error>;
    /// Creates a new independently owned handle to the underlying socket.
    ///
    /// Expects this method to behave like [https://doc.rust-lang.org/std/net/struct.TcpStream.html#method.try_clone]
    fn try_clone(&self) -> Result<Self, std::io::Error>;
}

impl SyncStream for std::net::TcpStream {
    fn set_read_timeout(&self, dur: Option<Duration>) -> Result<(), std::io::Error> {
        TcpStream::set_read_timeout(&self, dur)
    }
    fn try_clone(&self) -> Result<Self, std::io::Error> {
        TcpStream::try_clone(&self)
    }
}

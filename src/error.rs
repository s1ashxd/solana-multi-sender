#[derive(Debug)]
pub enum TransportError {
    Connect(std::io::Error),
    Io(std::io::Error),
    Tls(String),
    Quic(String),
    Envelope(EnvelopeError),
    Closed,
}

#[derive(Debug, PartialEq, Eq)]
pub enum TriggerError {
    Backpressure,
}

#[derive(Debug)]
pub enum SenderError {
    NoProviders,
    NoSource,
    NoSink,
    NoTransport,
    NoTls,
    Unsupported(std::io::Error),
    Connect { provider: u16, source: TransportError },
    QuicConfig { provider: u16, reason: String },
}

#[derive(Debug, PartialEq, Eq)]
pub enum EnvelopeError {
    BodySentinelMissing,
    BodyMaxTooSmall { max_len: usize, sentinel: usize },
    BodyTooLarge { encoded: usize, body_max: usize },
    ContentLengthOverflow { value: usize, width: u8 },
    UnknownUserSlot { name: String },
    UserSlotOverflow { name: String, value: usize, sentinel: usize },
}

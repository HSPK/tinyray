use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use socket2::{Domain, Protocol, Socket, Type};
use std::collections::HashMap;
use std::fmt;
use std::io::{self, Cursor, IoSlice};
use std::net::{TcpListener as StdTcpListener, ToSocketAddrs};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const LENGTH_PREFIX_BYTES: usize = 4;
pub const MAX_REQUEST_FRAME_BYTES: usize = 512 << 10;
pub const MAX_RESPONSE_FRAME_BYTES: usize = 64 << 20;

pub const OP_BEAT: &str = "beat";
pub const OP_BEAT_ACK: &str = "beat_ack";
pub const OP_HEALTH: &str = "health";
pub const OP_HEALTH_ACK: &str = "health_ack";
pub const OP_DEBUG_POOLS: &str = "debug_pools";
pub const OP_DEBUG_POOLS_ACK: &str = "debug_pools_ack";
pub const OP_ERROR: &str = "error";

/// Ask the kernel for its largest configured accept queue.
///
/// Unix kernels clamp an oversized backlog to their runtime SOMAXCONN; on
/// Winsock, `i32::MAX` is the documented SOMAXCONN value.
pub const OS_MAX_LISTEN_BACKLOG: i32 = i32::MAX;

pub fn bind_tcp_listener(address: impl ToSocketAddrs) -> io::Result<StdTcpListener> {
    let mut last_error = None;
    let mut resolved = false;
    for address in address.to_socket_addrs()? {
        resolved = true;
        let attempt = (|| {
            let socket = Socket::new(
                Domain::for_address(address),
                Type::STREAM,
                Some(Protocol::TCP),
            )?;
            socket.set_reuse_address(true)?;
            socket.bind(&address.into())?;
            socket.listen(OS_MAX_LISTEN_BACKLOG)?;
            socket.set_nonblocking(true)?;
            Ok(StdTcpListener::from(socket))
        })();
        match attempt {
            Ok(listener) => return Ok(listener),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or_else(|| {
        io::Error::new(
            io::ErrorKind::AddrNotAvailable,
            if resolved {
                "none of the resolved listener addresses could be bound"
            } else {
                "the listener address resolved to no socket addresses"
            },
        )
    }))
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RegistryEnvelope<T> {
    pub request_id: u64,
    pub operation: String,
    pub payload: T,
}

impl<T> RegistryEnvelope<T> {
    pub fn new(request_id: u64, operation: impl Into<String>, payload: T) -> Self {
        Self {
            request_id,
            operation: operation.into(),
            payload,
        }
    }
}

#[derive(Deserialize)]
pub struct RegistryRequestIdHeader {
    pub request_id: u64,
}

#[derive(Deserialize)]
pub struct RegistryEnvelopeHeader {
    pub request_id: u64,
    pub operation: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegistryHealth {
    pub status: String,
    pub version: String,
    pub protocol: u32,
    #[serde(default)]
    pub connections_accepted: u64,
    #[serde(default)]
    pub connections_active: u64,
    #[serde(default)]
    pub frames_received: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DebugPool {
    pub version: u64,
    pub roster: u64,
    pub members: usize,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct DebugPools {
    pub pools: HashMap<String, DebugPool>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegistryProtocolError {
    pub code: String,
    pub message: String,
}

impl RegistryProtocolError {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
}

#[derive(Debug)]
pub enum FrameError {
    Io(io::Error),
    TruncatedPrefix { received: usize },
    EmptyFrame,
    FrameTooLarge { length: usize, maximum: usize },
    TruncatedBody { expected: usize, received: usize },
    Encode(String),
    Decode(String),
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "{error}"),
            Self::TruncatedPrefix { received } => {
                write!(
                    f,
                    "length prefix ended after {received} of {LENGTH_PREFIX_BYTES} bytes"
                )
            }
            Self::EmptyFrame => write!(f, "a zero-length frame is not a MessagePack envelope"),
            Self::FrameTooLarge { length, maximum } => {
                write!(
                    f,
                    "frame declares {length} bytes, over the {maximum}-byte limit"
                )
            }
            Self::TruncatedBody { expected, received } => {
                write!(f, "frame body ended after {received} of {expected} bytes")
            }
            Self::Encode(error) => write!(f, "cannot encode MessagePack: {error}"),
            Self::Decode(error) => write!(f, "cannot decode MessagePack: {error}"),
        }
    }
}

impl std::error::Error for FrameError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for FrameError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

pub fn encode_message<T: Serialize>(value: &T, maximum: usize) -> Result<Vec<u8>, FrameError> {
    let bytes =
        rmp_serde::to_vec_named(value).map_err(|error| FrameError::Encode(error.to_string()))?;
    if bytes.is_empty() {
        return Err(FrameError::EmptyFrame);
    }
    if bytes.len() > maximum || bytes.len() > u32::MAX as usize {
        return Err(FrameError::FrameTooLarge {
            length: bytes.len(),
            maximum: maximum.min(u32::MAX as usize),
        });
    }
    Ok(bytes)
}

pub fn decode_message<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, FrameError> {
    let mut decoder = rmp_serde::Deserializer::new(Cursor::new(bytes));
    let value =
        T::deserialize(&mut decoder).map_err(|error| FrameError::Decode(error.to_string()))?;
    let consumed = decoder.get_ref().position() as usize;
    if consumed != bytes.len() {
        return Err(FrameError::Decode(format!(
            "{} trailing byte(s) after the envelope",
            bytes.len() - consumed
        )));
    }
    Ok(value)
}

pub async fn read_frame_length<R: AsyncRead + Unpin>(
    reader: &mut R,
    maximum: usize,
) -> Result<Option<usize>, FrameError> {
    let mut prefix = [0u8; LENGTH_PREFIX_BYTES];
    let mut received = 0;
    while received < prefix.len() {
        match reader.read(&mut prefix[received..]).await {
            Ok(0) if received == 0 => return Ok(None),
            Ok(0) => return Err(FrameError::TruncatedPrefix { received }),
            Ok(count) => received += count,
            Err(error) => return Err(FrameError::Io(error)),
        }
    }
    let length = u32::from_be_bytes(prefix) as usize;
    if length == 0 {
        return Err(FrameError::EmptyFrame);
    }
    if length > maximum {
        return Err(FrameError::FrameTooLarge { length, maximum });
    }
    Ok(Some(length))
}

pub async fn read_frame_body<R: AsyncRead + Unpin>(
    reader: &mut R,
    length: usize,
) -> Result<Vec<u8>, FrameError> {
    let mut body = vec![0u8; length];
    let mut received = 0;
    while received < body.len() {
        match reader.read(&mut body[received..]).await {
            Ok(0) => {
                return Err(FrameError::TruncatedBody {
                    expected: length,
                    received,
                })
            }
            Ok(count) => received += count,
            Err(error) => return Err(FrameError::Io(error)),
        }
    }
    Ok(body)
}

pub async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
    maximum: usize,
) -> Result<Option<Vec<u8>>, FrameError> {
    let Some(length) = read_frame_length(reader, maximum).await? else {
        return Ok(None);
    };
    read_frame_body(reader, length).await.map(Some)
}

pub async fn write_frame<W: AsyncWrite + Unpin, T: Serialize>(
    writer: &mut W,
    value: &T,
    maximum: usize,
) -> Result<(), FrameError> {
    let body = encode_message(value, maximum)?;
    write_frame_bytes(writer, &body, maximum).await
}

pub async fn write_frame_bytes<W: AsyncWrite + Unpin>(
    writer: &mut W,
    body: &[u8],
    maximum: usize,
) -> Result<(), FrameError> {
    if body.is_empty() {
        return Err(FrameError::EmptyFrame);
    }
    if body.len() > maximum || body.len() > u32::MAX as usize {
        return Err(FrameError::FrameTooLarge {
            length: body.len(),
            maximum: maximum.min(u32::MAX as usize),
        });
    }
    let prefix = (body.len() as u32).to_be_bytes();
    let mut prefix_offset = 0;
    let mut body_offset = 0;
    while prefix_offset < prefix.len() || body_offset < body.len() {
        let written = if prefix_offset < prefix.len() {
            let slices = [
                IoSlice::new(&prefix[prefix_offset..]),
                IoSlice::new(&body[body_offset..]),
            ];
            writer.write_vectored(&slices).await?
        } else {
            writer.write(&body[body_offset..]).await?
        };
        if written == 0 {
            return Err(FrameError::Io(io::Error::new(
                io::ErrorKind::WriteZero,
                "failed to write the complete frame",
            )));
        }
        let prefix_written = written.min(prefix.len() - prefix_offset);
        prefix_offset += prefix_written;
        body_offset += written - prefix_written;
    }
    Ok(())
}

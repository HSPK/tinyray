//! Error types for the core protocol layer.

use thiserror::Error;

/// Errors produced while decoding the wire framing.
///
/// Every variant here is *fatal for the connection*: the framing is a strict
/// binary format, so once we cannot make sense of the byte stream there is no
/// safe resynchronisation point. The decoder poisons itself accordingly.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum FrameError {
    #[error("bad magic: expected {expected:?}, found {found:?}")]
    BadMagic { expected: [u8; 4], found: [u8; 4] },

    #[error("header too large: {len} bytes exceeds limit of {max}")]
    HeaderTooLarge { len: u64, max: u64 },

    #[error("too many frames: {n} exceeds limit of {max}")]
    TooManyFrames { n: u64, max: u64 },

    #[error("frame {index} too large: {len} bytes exceeds limit of {max}")]
    FrameTooLarge { index: usize, len: u64, max: u64 },

    #[error("message too large: {len} bytes exceeds limit of {max}")]
    MessageTooLarge { len: u64, max: u64 },

    #[error("decoder is poisoned by a previous fatal error")]
    Poisoned,
}

/// Errors produced while interpreting a decoded message as a protocol envelope.
#[derive(Debug, Error)]
pub enum ProtoError {
    #[error("failed to decode msgpack header: {0}")]
    HeaderDecode(#[from] rmp_serde::decode::Error),

    #[error("failed to encode msgpack header: {0}")]
    HeaderEncode(#[from] rmp_serde::encode::Error),

    #[error("framing error: {0}")]
    Frame(#[from] FrameError),

    #[error("invalid identifier {value:?}: {reason}")]
    BadId { value: String, reason: &'static str },
}

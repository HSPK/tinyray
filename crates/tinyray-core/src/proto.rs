//! Protocol envelopes carried in the msgpack header of every [`Message`].
//!
//! [`Message`]: crate::framing::Message

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::error::ProtoError;
use crate::framing::Message;
use crate::ids::{ActorId, CallerId, TaskId};

/// Everything that can appear in a message header.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Envelope {
    /// Invoke a method on an actor. Arguments travel in the frames.
    Call(Call),
    /// Acknowledge a call. Carries the result inline when it is small enough.
    CallAck(CallAck),
    /// Ask the owner of a task for its result.
    Fetch(Fetch),
    /// A completed result. The value travels in the frames.
    Result(ResultHeader),
    /// A failure, either of the user's method or of the runtime.
    Error(RemoteError),
}

/// A method invocation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Call {
    pub task_id: TaskId,
    pub actor_id: ActorId,
    /// Identifies the caller so the actor can restore per-caller ordering.
    pub caller_id: CallerId,
    /// Monotonic per `(caller, actor)` pair. Out-of-order arrivals are buffered
    /// until the gap is filled, which is what makes HTTP concurrency safe.
    pub seq: u64,
    pub method: String,
    /// Caller would like the result inline in the ack if it is small.
    pub want_inline: bool,
}

/// Response to a [`Call`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallAck {
    pub task_id: TaskId,
    /// When true the frames of this message already hold the result, and no
    /// follow-up fetch is needed.
    pub inline: bool,
}

/// Request for a previously produced result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fetch {
    pub task_id: TaskId,
    /// How long the owner may hold the request open before replying that the
    /// result is not ready yet. Zero means reply immediately.
    pub timeout_ms: u64,
    /// Ask only whether the result has settled, without sending it.
    ///
    /// `wait` needs to know which references are ready, not what they contain.
    /// Without this it would drag every payload to the driver and discard it,
    /// which is precisely the relay the design exists to avoid: 32 rollouts of
    /// 10 MB would move 320 MB to answer a yes/no question.
    #[serde(default)]
    pub status_only: bool,
}

/// Header accompanying a successful result payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResultHeader {
    pub task_id: TaskId,
}

/// A failure crossing the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteError {
    pub task_id: TaskId,
    pub kind: ErrorKind,
    pub message: String,
    /// Remote Python traceback, when the failure came from user code. Keeping
    /// this on the wire is what makes distributed debugging tolerable.
    pub traceback: Option<String>,
}

/// Coarse failure classification. The Python layer maps these onto exception
/// types, so the set is deliberately small and stable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorKind {
    /// The user's method raised.
    UserException,
    /// The result is gone: evicted, expired, or its owner restarted.
    ObjectLost,
    /// The target actor is no longer alive.
    ActorDied,
    /// No such actor, task or method.
    NotFound,
    /// The target is over its queue or memory watermark; retry later.
    Backpressure,
    /// A bug in tinyray itself.
    Internal,
}

impl ErrorKind {
    /// Stable name used when mapping to a Python exception class.
    pub fn as_str(&self) -> &'static str {
        match self {
            ErrorKind::UserException => "UserException",
            ErrorKind::ObjectLost => "ObjectLost",
            ErrorKind::ActorDied => "ActorDied",
            ErrorKind::NotFound => "NotFound",
            ErrorKind::Backpressure => "Backpressure",
            ErrorKind::Internal => "Internal",
        }
    }

    /// Whether retrying the same request unchanged could plausibly succeed.
    pub fn is_retryable(&self) -> bool {
        matches!(self, ErrorKind::Backpressure)
    }
}

impl std::fmt::Display for RemoteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.kind.as_str(), self.message)?;
        if let Some(traceback) = &self.traceback {
            write!(f, "\n--- remote traceback ---\n{traceback}")?;
        }
        Ok(())
    }
}

impl std::error::Error for RemoteError {}

impl Envelope {
    /// Serialise this envelope to msgpack.
    pub fn encode(&self) -> Result<Bytes, ProtoError> {
        Ok(Bytes::from(rmp_serde::to_vec_named(self)?))
    }

    /// Parse an envelope out of a message header.
    pub fn decode(header: &[u8]) -> Result<Envelope, ProtoError> {
        Ok(rmp_serde::from_slice(header)?)
    }

    /// Build a complete [`Message`] from this envelope plus payload frames.
    pub fn into_message(self, frames: Vec<Bytes>) -> Result<Message, ProtoError> {
        Ok(Message::new(self.encode()?, frames))
    }

    /// The task this envelope concerns, if any.
    pub fn task_id(&self) -> TaskId {
        match self {
            Envelope::Call(c) => c.task_id,
            Envelope::CallAck(a) => a.task_id,
            Envelope::Fetch(f) => f.task_id,
            Envelope::Result(r) => r.task_id,
            Envelope::Error(e) => e.task_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::limits::Limits;

    fn sample_call() -> Envelope {
        Envelope::Call(Call {
            task_id: TaskId::from_parts(1, 2),
            actor_id: ActorId::from_parts(3, 4),
            caller_id: CallerId::from_parts(5, 6),
            seq: 42,
            method: "step".to_string(),
            want_inline: true,
        })
    }

    #[test]
    fn every_variant_roundtrips() {
        let envelopes = vec![
            sample_call(),
            Envelope::CallAck(CallAck {
                task_id: TaskId::from_parts(1, 2),
                inline: false,
            }),
            Envelope::Fetch(Fetch {
                task_id: TaskId::from_parts(1, 2),
                timeout_ms: 5_000,
                status_only: false,
            }),
            Envelope::Result(ResultHeader {
                task_id: TaskId::from_parts(1, 2),
            }),
            Envelope::Error(RemoteError {
                task_id: TaskId::from_parts(1, 2),
                kind: ErrorKind::UserException,
                message: "boom".into(),
                traceback: Some("Traceback...\n  ValueError: boom".into()),
            }),
        ];
        for envelope in envelopes {
            let encoded = envelope.encode().unwrap();
            let decoded = Envelope::decode(&encoded).unwrap();
            assert_eq!(decoded, envelope);
        }
    }

    #[test]
    fn envelope_travels_inside_the_framing() {
        let envelope = sample_call();
        let frames = vec![
            Bytes::from_static(b"pickle-body"),
            Bytes::from(vec![9u8; 64]),
        ];
        let message = envelope.clone().into_message(frames.clone()).unwrap();

        let encoded = message.encode_to_vec(&Limits::DEFAULT).unwrap();
        let mut buf = bytes::BytesMut::from(&encoded[..]);
        let mut decoder = crate::framing::Decoder::default();
        let decoded = decoder.decode(&mut buf).unwrap().unwrap();

        assert_eq!(Envelope::decode(&decoded.header).unwrap(), envelope);
        assert_eq!(decoded.frames, frames);
    }

    #[test]
    fn task_id_accessor_covers_all_variants() {
        let id = TaskId::from_parts(7, 8);
        let envelopes = vec![
            Envelope::CallAck(CallAck {
                task_id: id,
                inline: false,
            }),
            Envelope::Fetch(Fetch {
                task_id: id,
                timeout_ms: 0,
                status_only: true,
            }),
            Envelope::Result(ResultHeader { task_id: id }),
            Envelope::Error(RemoteError {
                task_id: id,
                kind: ErrorKind::Internal,
                message: String::new(),
                traceback: None,
            }),
        ];
        for envelope in envelopes {
            assert_eq!(envelope.task_id(), id);
        }
    }

    #[test]
    fn error_kinds_have_stable_names() {
        assert_eq!(ErrorKind::ObjectLost.as_str(), "ObjectLost");
        assert_eq!(ErrorKind::ActorDied.as_str(), "ActorDied");
        // Only backpressure is safe to retry blindly; a stateful call must not
        // be replayed just because it raised.
        assert!(ErrorKind::Backpressure.is_retryable());
        assert!(!ErrorKind::UserException.is_retryable());
        assert!(!ErrorKind::ActorDied.is_retryable());
    }

    #[test]
    fn decoding_garbage_is_an_error_not_a_panic() {
        assert!(Envelope::decode(b"").is_err());
        assert!(Envelope::decode(b"not msgpack at all").is_err());
        assert!(Envelope::decode(&[0xc1, 0xc1, 0xc1]).is_err());
    }

    #[test]
    fn headers_stay_small() {
        // Headers ride in front of every call; keep an eye on their size.
        let encoded = sample_call().encode().unwrap();
        assert!(
            encoded.len() < 256,
            "call header grew to {} bytes",
            encoded.len()
        );
    }
}

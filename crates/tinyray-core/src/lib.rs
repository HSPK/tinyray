//! tinyray core: wire protocol, framing and identifiers.
//!
//! This crate is deliberately IO-free and Python-free. Everything here is pure
//! data manipulation so it can be unit tested exhaustively and reused by the
//! head, node agent, worker runtime and the PyO3 bindings alike.

pub mod error;
pub mod framing;
pub mod ids;
pub mod limits;
pub mod proto;

pub use error::{FrameError, ProtoError};
pub use framing::{Decoder, Message};
pub use ids::{ActorId, CallerId, Id, NodeId, TaskId};
pub use limits::Limits;

//! The heartbeat loop and the local roster cache.
//!
//! This runs on native OS threads owned by tokio, never on a Python thread.
//! That is the entire reason this crate is Rust: when the main thread sits
//! inside `dist.all_reduce()` holding the GIL, a Python thread cannot run and
//! the lease would expire -- declaring a healthy rank dead and voiding the
//! round. A native thread does not need the GIL as long as it never calls
//! into Python, and this loop never does.

use std::collections::{BTreeSet, HashMap};
use std::hash::{Hash, Hasher};
#[cfg(unix)]
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant};
use tinyray_core::FdTable;
use tinyray_proto::wire::{
    decode_message, read_frame_body, read_frame_length, write_frame, FrameError, RegistryEnvelope,
    RegistryEnvelopeHeader, RegistryProtocolError, MAX_REQUEST_FRAME_BYTES,
    MAX_RESPONSE_FRAME_BYTES, OP_BEAT, OP_BEAT_ACK, OP_ERROR,
};
use tinyray_proto::{Beat, BeatAck, Member, PoolDelta};
use tokio::io::AsyncRead;
use tokio::net::TcpStream;
use tokio::sync::Notify;

#[cfg(unix)]
use std::os::fd::AsRawFd;

mod cache;
mod heartbeat;
mod shared;
mod wait;

pub use cache::{CachedPool, FrozenMembers, FrozenPool};
pub use heartbeat::{beat_once, coalesce_gap, spawn};
pub use shared::{Published, Shared};
pub use wait::{CacheWaiter, EpochWaitResult, WaitResult, WaitStatus};

#[cfg(test)]
mod tests;

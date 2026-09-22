//! Embedded Rust SDK for tinyray membership and native method RPC.
//!
//! The crate has no Python or PyO3 dependency. Rust and Python services use
//! the same length-prefixed MessagePack registry and method protocols.

mod blob;
mod discovery;
mod member;
mod service;
mod transport;

mod fds {
    pub(crate) use tinyray_core::{FdTable, RawFd};
}

mod membership_core {
    pub(crate) use tinyray_membership::*;
}

pub use blob::{
    clear_inherited_rpc_blob_owners, decode as decode_msgpack, decode_with_blob_limits,
    BlobDecodeLimits, BlobDescriptor, BlobError, BlobRef, RpcBlobOwners, BLOB_EXT_CODE,
    BLOB_PROTOCOL, DEFAULT_MAX_BLOB_BYTES, MAX_BLOB_MAPPED_BYTES_PER_MESSAGE,
    MAX_BLOB_REFS_PER_MESSAGE, MAX_DECODED_BLOB_BYTES, MAX_DECODED_BLOB_HANDLES,
    MAX_DECODED_BLOB_MAPPINGS,
};
pub use discovery::{DiscoveryPool, Epoch, MemberRef, Snapshot};
pub use member::{Member, MemberBuilder, MemberError, MemberStats};
pub use service::{
    CallContext, CancellationToken, Router, Service, ServiceError, ServiceRequest, ServiceResponse,
};
pub use tinyray_proto::rpc::{RpcOperation, RpcStatus};
pub use tinyray_proto::Member as DiscoveredMember;
pub use transport::{
    reset_blob_reply_budgets_after_fork, CallError, Client, ClientConfig,
    ClientRequestCancellation, ClientStats, ReceivedRawReply, ReceivedRpcReply, RpcRuntime, Server,
    ServerConfig, ServerStats, Target, MAX_BLOB_BYTES_PER_REPLY, MAX_BLOB_REFS_PER_REPLY,
    MAX_UNACKED_BLOB_BYTES_PER_CONNECTION, MAX_UNACKED_BLOB_BYTES_PER_SERVER,
    MAX_UNACKED_BLOB_BYTES_PROCESS, MAX_UNACKED_BLOB_REFS_PER_CONNECTION,
    MAX_UNACKED_BLOB_REFS_PER_SERVER, MAX_UNACKED_BLOB_REFS_PROCESS,
};

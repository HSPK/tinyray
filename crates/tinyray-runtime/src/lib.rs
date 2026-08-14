//! tinyray runtime: result store, ordered dispatch and HTTP transport.
//!
//! Scope note: tinyray is a control plane. Tensors move inside whatever
//! framework the user is running -- Megatron, SGLang, vLLM -- so there is no
//! shared-memory fast path here and there will not be one. The store exists for
//! control data and for pure-Python rollouts, not as an object store.

pub mod actor;
pub mod client;
pub mod cluster;
pub mod collective;
pub mod queue;
pub mod store;
pub mod transport;

pub use actor::{ActorConfig, ActorRuntime, ActorStats, Dispatch};
pub use client::{ClientError, ClientRuntime, FetchedValue, OwnerRef};
pub use queue::{OrderedQueue, QueuedTask, RejectReason};
pub use store::{Fetched, LocalStore, StoreConfig, StoreStats, StoredValue};
pub use transport::client::{ClientConfig, TransportClient, TransportError};
pub use transport::server::{serve, Handler, Reply, RunningServer, ServerConfig};

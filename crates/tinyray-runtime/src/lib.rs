//! tinyray runtime: result store, ordered dispatch and HTTP transport.

pub mod actor;
pub mod client;
pub mod cluster;
pub mod collective;
pub mod queue;
pub mod shm;
pub mod store;
pub mod transport;

pub use actor::{ActorConfig, ActorRuntime, ActorStats, Dispatch};
pub use client::{ClientError, ClientRuntime, FetchedValue, OwnerRef};
pub use queue::{OrderedQueue, QueuedTask, RejectReason};
pub use store::{Fetched, LocalStore, StoreConfig, StoreStats, StoredValue};
pub use transport::client::{ClientConfig, TransportClient, TransportError};
pub use transport::server::{serve, Handler, Reply, RunningServer, ServerConfig};

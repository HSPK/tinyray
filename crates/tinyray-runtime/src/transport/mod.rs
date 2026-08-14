//! HTTP transport: the wire that carries calls and results.
//!
//! HTTP is a deliberate choice, not a compromise. With ~200 ms of compute per
//! call, a ~150 us round trip is 0.075% overhead, and in exchange every message
//! is inspectable with ordinary tools. What *does* matter at 10 MB payloads is
//! that nothing copies the body more than it must, and that serving a result
//! never enters the Python interpreter.
//!
//! Bodies use the tinyray framing (see `tinyray_core::framing`) with
//! `Content-Type: application/x-tinyray`.

pub mod client;
pub mod server;

/// Content type for framed tinyray messages.
pub const CONTENT_TYPE: &str = "application/x-tinyray";

/// Paths in the actor-facing API. Separate paths rather than one `/rpc`
/// endpoint, so that logs and packet captures stay readable.
pub mod paths {
    /// Submit a call to an actor.
    pub const CALL: &str = "/actor/call";
    /// Fetch a result from the actor that produced it.
    pub const FETCH: &str = "/task/fetch";
    /// Release a result the consumer is done with.
    pub const RELEASE: &str = "/task/release";
    /// Liveness probe.
    pub const HEALTH: &str = "/health";
    /// Queue depths, store watermark, current method.
    pub const INTROSPECT: &str = "/introspect";
    /// Ask the actor to shut down cleanly.
    pub const SHUTDOWN: &str = "/shutdown";
}

//! The HTTP client side of the transport.
//!
//! One pool per process, shared by the driver and by actors fetching each
//! other's results. Two details are load-bearing at 10 MB payloads:
//!
//! * **keep-alive with a real pool.** A TCP handshake per call would cost more
//!   than the call.
//! * **several connections per peer.** HTTP/1.1 has head-of-line blocking, so
//!   a 10 MB response on a single connection stalls every small control
//!   message queued behind it.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Request, StatusCode};
use parking_lot::Mutex;
use tinyray_core::framing::{Decoder, Message};
use tinyray_core::Limits;

use super::CONTENT_TYPE;

/// Failure modes a caller has to distinguish.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("connection to {endpoint} failed: {source}")]
    Connect {
        endpoint: String,
        #[source]
        source: std::io::Error,
    },

    #[error("request to {endpoint}{path} failed: {detail}")]
    Request {
        endpoint: String,
        path: String,
        detail: String,
    },

    /// The peer is over its watermark. Retry after backing off; this is the
    /// only failure that is safe to retry blindly.
    #[error("peer {endpoint} applied backpressure: {detail}")]
    Backpressure { endpoint: String, detail: String },

    #[error("peer {endpoint} returned {status}: {detail}")]
    Status {
        endpoint: String,
        status: u16,
        detail: String,
    },

    #[error("malformed response from {endpoint}: {source}")]
    Protocol {
        endpoint: String,
        #[source]
        source: tinyray_core::FrameError,
    },

    #[error("response from {endpoint} was truncated")]
    Truncated { endpoint: String },

    #[error("request to {endpoint} timed out after {0:?}", timeout)]
    Timeout { endpoint: String, timeout: Duration },
}

impl TransportError {
    /// Whether resending the identical request could succeed.
    ///
    /// Deliberately narrow: a stateful actor call must never be replayed just
    /// because the response was lost.
    pub fn is_retryable(&self) -> bool {
        matches!(self, TransportError::Backpressure { .. })
    }
}

/// Tuning for the connection pool.
#[derive(Debug, Clone, Copy)]
pub struct ClientConfig {
    /// Connections kept per peer. Above one purely to dodge HTTP/1.1
    /// head-of-line blocking between big results and small control messages.
    pub connections_per_peer: usize,
    /// Ceiling on a single request, including the long-poll fetch.
    pub request_timeout: Duration,
    /// How long to wait before retrying a backpressured request.
    pub backoff: Duration,
    /// How many times to retry a retryable failure.
    pub max_retries: usize,
    pub limits: Limits,
}

impl Default for ClientConfig {
    fn default() -> Self {
        ClientConfig {
            connections_per_peer: 4,
            request_timeout: Duration::from_secs(300),
            backoff: Duration::from_millis(25),
            max_retries: 16,
            limits: Limits::DEFAULT,
        }
    }
}

type HyperClient = hyper_util::client::legacy::Client<
    hyper_util::client::legacy::connect::HttpConnector,
    Full<Bytes>,
>;

/// A pooled HTTP client for talking to actors, the head and node agents.
pub struct TransportClient {
    config: ClientConfig,
    inner: HyperClient,
    stats: Mutex<HashMap<String, PeerStats>>,
}

/// Per-peer counters surfaced through `/introspect`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PeerStats {
    pub requests: u64,
    pub retries: u64,
    pub failures: u64,
}

impl TransportClient {
    pub fn new(config: ClientConfig) -> Arc<TransportClient> {
        let mut connector = hyper_util::client::legacy::connect::HttpConnector::new();
        connector.set_nodelay(true);
        connector.set_keepalive(Some(Duration::from_secs(60)));

        let inner =
            hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
                .pool_max_idle_per_host(config.connections_per_peer)
                .pool_idle_timeout(Duration::from_secs(90))
                .build(connector);

        Arc::new(TransportClient {
            config,
            inner,
            stats: Mutex::new(HashMap::new()),
        })
    }

    pub fn config(&self) -> &ClientConfig {
        &self.config
    }

    pub fn stats(&self) -> HashMap<String, PeerStats> {
        self.stats.lock().clone()
    }

    /// Send a framed message and await a framed reply.
    ///
    /// Retries only on backpressure: everything else is surfaced so the caller
    /// can decide, because replaying a stateful call is not safe.
    pub async fn request(
        &self,
        endpoint: &str,
        path: &str,
        message: &Message,
    ) -> Result<Message, TransportError> {
        let body = message
            .encode_to_vec(&self.config.limits)
            .map_err(|source| TransportError::Protocol {
                endpoint: endpoint.to_string(),
                source,
            })?;
        let body = Bytes::from(body);

        let mut attempt = 0usize;
        loop {
            self.bump(endpoint, |s| s.requests += 1);
            match self.attempt(endpoint, path, body.clone()).await {
                Ok(reply) => return Ok(reply),
                Err(err) if err.is_retryable() && attempt < self.config.max_retries => {
                    attempt += 1;
                    self.bump(endpoint, |s| s.retries += 1);
                    // Linear backoff, capped: the peer is draining a queue, not
                    // recovering from an outage.
                    let wait = self.config.backoff * (attempt.min(8) as u32);
                    tokio::time::sleep(wait).await;
                }
                Err(err) => {
                    self.bump(endpoint, |s| s.failures += 1);
                    return Err(err);
                }
            }
        }
    }

    async fn attempt(
        &self,
        endpoint: &str,
        path: &str,
        body: Bytes,
    ) -> Result<Message, TransportError> {
        let uri = format!("http://{endpoint}{path}");
        let request = Request::builder()
            .method("POST")
            .uri(&uri)
            .header(hyper::header::CONTENT_TYPE, CONTENT_TYPE)
            .header(hyper::header::CONTENT_LENGTH, body.len())
            .body(Full::new(body))
            .map_err(|err| TransportError::Request {
                endpoint: endpoint.to_string(),
                path: path.to_string(),
                detail: err.to_string(),
            })?;

        let response =
            tokio::time::timeout(self.config.request_timeout, self.inner.request(request))
                .await
                .map_err(|_| TransportError::Timeout {
                    endpoint: endpoint.to_string(),
                    timeout: self.config.request_timeout,
                })?
                .map_err(|err| TransportError::Request {
                    endpoint: endpoint.to_string(),
                    path: path.to_string(),
                    detail: err.to_string(),
                })?;

        let status = response.status();
        let bytes = response
            .into_body()
            .collect()
            .await
            .map_err(|err| TransportError::Request {
                endpoint: endpoint.to_string(),
                path: path.to_string(),
                detail: err.to_string(),
            })?
            .to_bytes();

        if status == StatusCode::TOO_MANY_REQUESTS {
            return Err(TransportError::Backpressure {
                endpoint: endpoint.to_string(),
                detail: String::from_utf8_lossy(&bytes).to_string(),
            });
        }
        if !status.is_success() {
            return Err(TransportError::Status {
                endpoint: endpoint.to_string(),
                status: status.as_u16(),
                detail: String::from_utf8_lossy(&bytes).to_string(),
            });
        }

        let mut buf = bytes::BytesMut::from(&bytes[..]);
        let mut decoder = Decoder::new(self.config.limits);
        match decoder.decode(&mut buf) {
            Ok(Some(message)) => Ok(message),
            Ok(None) => Err(TransportError::Truncated {
                endpoint: endpoint.to_string(),
            }),
            Err(source) => Err(TransportError::Protocol {
                endpoint: endpoint.to_string(),
                source,
            }),
        }
    }

    /// Fetch a plain (non-framed) endpoint such as `/health`.
    pub async fn get_text(&self, endpoint: &str, path: &str) -> Result<String, TransportError> {
        let uri = format!("http://{endpoint}{path}");
        let request = Request::builder()
            .method("GET")
            .uri(&uri)
            .body(Full::new(Bytes::new()))
            .map_err(|err| TransportError::Request {
                endpoint: endpoint.to_string(),
                path: path.to_string(),
                detail: err.to_string(),
            })?;

        let response =
            tokio::time::timeout(self.config.request_timeout, self.inner.request(request))
                .await
                .map_err(|_| TransportError::Timeout {
                    endpoint: endpoint.to_string(),
                    timeout: self.config.request_timeout,
                })?
                .map_err(|err| TransportError::Request {
                    endpoint: endpoint.to_string(),
                    path: path.to_string(),
                    detail: err.to_string(),
                })?;

        let status = response.status();
        let bytes = response
            .into_body()
            .collect()
            .await
            .map_err(|err| TransportError::Request {
                endpoint: endpoint.to_string(),
                path: path.to_string(),
                detail: err.to_string(),
            })?
            .to_bytes();
        let text = String::from_utf8_lossy(&bytes).to_string();

        if !status.is_success() {
            return Err(TransportError::Status {
                endpoint: endpoint.to_string(),
                status: status.as_u16(),
                detail: text,
            });
        }
        Ok(text)
    }

    fn bump(&self, endpoint: &str, f: impl FnOnce(&mut PeerStats)) {
        let mut stats = self.stats.lock();
        f(stats.entry(endpoint.to_string()).or_default());
    }
}

//! The HTTP server side of the transport.
//!
//! Everything here runs on tokio worker threads and never touches the Python
//! interpreter. That is the whole point: an actor busy running a 200 ms
//! training step must still serve result fetches at line rate. Benchmarks put
//! the difference at roughly 1.0x versus 26x when the same work is initiated
//! from Python.

use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tinyray_core::framing::Message;
use tinyray_core::proto::{ErrorKind, RemoteError};
use tinyray_core::{Limits, TaskId};
use tokio::net::TcpListener;

use super::{paths, CONTENT_TYPE};

/// What a handler gives back.
pub enum Reply {
    /// A framed protocol message.
    Message(Message),
    /// A plain JSON body, for `/health` and `/introspect`.
    Json(String),
    /// Refuse the request. `retry_after` populates the header for backpressure.
    Status(StatusCode, String),
}

impl Reply {
    pub fn backpressure(detail: impl Into<String>) -> Reply {
        Reply::Status(StatusCode::TOO_MANY_REQUESTS, detail.into())
    }

    pub fn not_found(detail: impl Into<String>) -> Reply {
        Reply::Status(StatusCode::NOT_FOUND, detail.into())
    }
}

/// Implemented by the actor runtime, the head and the node agent.
///
/// Handlers are called on tokio worker threads and must not block. Anything
/// that needs the GIL belongs on the Python executor thread instead.
pub trait Handler: Send + Sync + 'static {
    /// Handle a framed request. `path` is already routed.
    fn handle<'a>(
        &'a self,
        path: &'a str,
        message: Message,
    ) -> Pin<Box<dyn Future<Output = Reply> + Send + 'a>>;

    /// Handle a bodyless GET, e.g. `/health` or `/introspect`.
    fn handle_get<'a>(&'a self, path: &'a str) -> Pin<Box<dyn Future<Output = Reply> + Send + 'a>>;
}

/// Configuration for a running server.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub bind: SocketAddr,
    pub limits: Limits,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            // Port 0: the OS picks a free port and we report it back. Actors
            // are addressed through the registry, never by a fixed port.
            bind: "127.0.0.1:0".parse().expect("valid literal"),
            limits: Limits::DEFAULT,
        }
    }
}

/// A bound, running HTTP server.
pub struct RunningServer {
    addr: SocketAddr,
    shutdown: tokio::sync::watch::Sender<bool>,
}

impl RunningServer {
    /// The address actually bound, which is what peers should be told.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Ask the accept loop to stop. In-flight requests are allowed to finish.
    pub fn shutdown(&self) {
        let _ = self.shutdown.send(true);
    }
}

/// Bind and serve until [`RunningServer::shutdown`] is called.
pub async fn serve<H: Handler>(
    config: ServerConfig,
    handler: Arc<H>,
) -> std::io::Result<RunningServer> {
    let listener = TcpListener::bind(config.bind).await?;
    let addr = listener.local_addr()?;
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

    let limits = config.limits;
    tokio::spawn(async move {
        loop {
            let accepted = tokio::select! {
                result = listener.accept() => result,
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() { break; }
                    continue;
                }
            };
            let Ok((stream, _peer)) = accepted else {
                continue;
            };
            // Small control messages dominate the latency budget, and Nagle
            // would add up to 40 ms of delay to them.
            let _ = stream.set_nodelay(true);

            let handler = handler.clone();
            tokio::spawn(async move {
                let service = service_fn(move |req| {
                    let handler = handler.clone();
                    async move { Ok::<_, Infallible>(dispatch(handler, req, limits).await) }
                });
                // HTTP/2 over cleartext (h2c), by prior knowledge -- there is
                // no upgrade dance because both ends are ours.
                //
                // This is not a performance tweak, it is a capacity fix.
                // HTTP/1.1 serves one request per connection at a time, so
                // concurrency N needs N connections. Measured on the previous
                // build: ~0.1 new connections per request, ~2,000 sockets/s at
                // 19k ops/s, and TIME_WAIT reaching 28,122 against an ephemeral
                // port range of 28,231 -- total collapse in about 14 seconds,
                // with the server sitting at 0% CPU because nothing could
                // connect. Raising the connection pool did not help.
                //
                // HTTP/2 multiplexes concurrent streams over one connection, so
                // concurrency stops consuming ports at all.
                // Auto-negotiated: HTTP/2 by prior knowledge when the client
                // sends the h2c preface, HTTP/1.1 otherwise.
                //
                // HTTP/2 is a capacity fix, not a performance tweak. HTTP/1.1
                // serves one request per connection at a time, so concurrency N
                // needs N connections. Measured on the previous build: ~0.1 new
                // connections per request, ~2,000 sockets/s at 19k ops/s, and
                // TIME_WAIT reaching 28,122 against an ephemeral port range of
                // 28,231 -- total collapse in ~14 s, with the server at 0% CPU
                // because nothing could connect. Raising the pool did not help.
                //
                // HTTP/1.1 is kept because `/health` and `/introspect` are
                // promised to work with `curl`, and going h2c-only silently
                // broke every plain HTTP client including the readiness probes.
                let mut builder =
                    hyper_util::server::conn::auto::Builder::new(TokioExecutor::new());
                builder.http1().keep_alive(true);
                builder.http2().max_concurrent_streams(None);
                if let Err(err) = builder
                    .serve_connection(TokioIo::new(stream), service)
                    .await
                {
                    tracing::debug!("connection closed: {err}");
                }
            });
        }
    });

    Ok(RunningServer {
        addr,
        shutdown: shutdown_tx,
    })
}

async fn dispatch<H: Handler>(
    handler: Arc<H>,
    req: Request<Incoming>,
    limits: Limits,
) -> Response<Full<Bytes>> {
    let path = req.uri().path().to_string();

    if req.method() == Method::GET {
        return render(handler.handle_get(&path).await);
    }
    if req.method() != Method::POST {
        return render(Reply::Status(
            StatusCode::METHOD_NOT_ALLOWED,
            format!("{} is not allowed on {path}", req.method()),
        ));
    }

    let collected = match req.into_body().collect().await {
        Ok(body) => body.to_bytes(),
        Err(err) => {
            return render(Reply::Status(
                StatusCode::BAD_REQUEST,
                format!("failed to read body: {err}"),
            ))
        }
    };

    let mut buf = bytes::BytesMut::from(&collected[..]);
    let mut decoder = tinyray_core::framing::Decoder::new(limits);
    let message = match decoder.decode(&mut buf) {
        Ok(Some(message)) => message,
        Ok(None) => {
            return render(Reply::Status(
                StatusCode::BAD_REQUEST,
                "incomplete tinyray message".into(),
            ))
        }
        Err(err) => {
            return render(Reply::Status(
                StatusCode::BAD_REQUEST,
                format!("malformed tinyray message: {err}"),
            ))
        }
    };

    render(handler.handle(&path, message).await)
}

fn render(reply: Reply) -> Response<Full<Bytes>> {
    match reply {
        Reply::Message(message) => match message.encode_to_vec(&Limits::DEFAULT) {
            Ok(bytes) => Response::builder()
                .status(StatusCode::OK)
                .header(hyper::header::CONTENT_TYPE, CONTENT_TYPE)
                .body(Full::new(Bytes::from(bytes)))
                .expect("valid response"),
            Err(err) => Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Full::new(Bytes::from(format!("encode failed: {err}"))))
                .expect("valid response"),
        },
        Reply::Json(body) => Response::builder()
            .status(StatusCode::OK)
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .body(Full::new(Bytes::from(body)))
            .expect("valid response"),
        Reply::Status(status, detail) => Response::builder()
            .status(status)
            .header(hyper::header::CONTENT_TYPE, "text/plain")
            .body(Full::new(Bytes::from(detail)))
            .expect("valid response"),
    }
}

/// Build an error reply carrying a remote failure.
pub fn error_reply(task_id: TaskId, kind: ErrorKind, message: impl Into<String>) -> Reply {
    let envelope = tinyray_core::proto::Envelope::Error(RemoteError {
        task_id,
        kind,
        message: message.into(),
        traceback: None,
    });
    match envelope.into_message(vec![]) {
        Ok(message) => Reply::Message(message),
        Err(err) => Reply::Status(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to encode error: {err}"),
        ),
    }
}

/// Route table, exposed so the tests and `/introspect` agree on the set.
pub fn known_paths() -> &'static [&'static str] {
    &[
        paths::CALL,
        paths::FETCH,
        paths::RELEASE,
        paths::HEALTH,
        paths::INTROSPECT,
        paths::SHUTDOWN,
    ]
}

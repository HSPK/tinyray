//! h2c front end. Uses `auto::Builder` so plain HTTP/1.1 clients (curl,
//! probes) keep working -- an earlier version bound http2-only and broke
//! every non-h2 caller including the readiness probe.

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use std::convert::Infallible;
use std::sync::Arc;
use tokio::net::TcpListener;

use crate::state::Registry;

const MAX_BODY: usize = 8 << 20;

async fn handle(
    req: Request<hyper::body::Incoming>,
    reg: Arc<Registry>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let reply = |code: StatusCode, body: Vec<u8>| {
        Ok(Response::builder()
            .status(code)
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(body)))
            .unwrap())
    };
    match (req.method().as_str(), req.uri().path()) {
        ("POST", "/v1/beat") => {
            let limited = http_body_util::Limited::new(req.into_body(), MAX_BODY);
            let bytes = match limited.collect().await {
                Ok(b) => b.to_bytes(),
                Err(_) => return reply(StatusCode::PAYLOAD_TOO_LARGE, b"{}".to_vec()),
            };
            match serde_json::from_slice(&bytes) {
                Ok(beat) => reply(StatusCode::OK, serde_json::to_vec(&reg.beat(&beat)).unwrap()),
                Err(e) => reply(
                    StatusCode::BAD_REQUEST,
                    serde_json::to_vec(&serde_json::json!({"error": e.to_string()})).unwrap(),
                ),
            }
        }
        ("GET", "/v1/pools") => {
            let snap: std::collections::HashMap<_, _> = reg
                .snapshot()
                .into_iter()
                .map(|(k, (v, r, n))| {
                    (k, serde_json::json!({"version": v, "roster": r, "members": n}))
                })
                .collect();
            reply(StatusCode::OK, serde_json::to_vec(&snap).unwrap())
        }
        ("GET", "/health") => reply(StatusCode::OK, b"{\"status\":\"ok\"}".to_vec()),
        _ => reply(StatusCode::NOT_FOUND, b"{}".to_vec()),
    }
}

pub async fn serve(listener: TcpListener, reg: Arc<Registry>) {
    loop {
        let (stream, _) = match listener.accept().await {
            Ok(v) => v,
            Err(_) => continue,
        };
        let reg = reg.clone();
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let _ = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                .serve_connection(io, service_fn(move |r| handle(r, reg.clone())))
                .await;
        });
    }
}

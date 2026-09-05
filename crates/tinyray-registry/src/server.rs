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
use std::time::Duration;
use tokio::net::TcpListener;

use crate::{delta::SharedBeatAck, state::Registry};

/// A beat is a few hundred bytes; the roomiest legal one -- 64 watched pools,
/// 256 method names and a full-sized state -- is around 210 KB. Eight
/// megabytes left room for a single member to hand the registry something it
/// would then copy to every subscriber.
const MAX_BODY: usize = 512 << 10;

/// Answer a beat, holding the answer back while there is nothing to say.
///
/// The lease is renewed by `beat()` the moment the request lands -- only the
/// *reply* waits. Holding the renewal too would let a member expire while it
/// was parked, which is the opposite of the point.
///
/// Callers that ask for no hold, including every client that predates the
/// field, are answered immediately and see none of this.
async fn hold(reg: &Registry, beat: &tinyray_proto::Beat) -> SharedBeatAck {
    let mut ack = reg.beat_shared(beat);
    // Capped at half a lease: a member is renewed by the arrival of its beat,
    // so parking longer than that would starve its own lease.
    let budget = beat.hold_ms.min(reg.ttl.as_millis() as u64 / 2);
    if budget == 0 || !ack.accepted || !ack.pools.is_empty() || beat.watch.is_empty() {
        return ack;
    }
    // Up to an eighth of the budget, keyed off the caller so it is stable for
    // them and spread across everyone else. A pool watched by thousands wakes
    // all of them at once; measured at 40,000 parked on one pool, that single
    // change cost 1.75 core-seconds, and arriving together is what makes it a
    // spike rather than a shoulder.
    let jitter = beat.id % (budget / 8 + 1);
    let deadline = tokio::time::Instant::now() + Duration::from_millis(budget + jitter);
    let bell = Arc::new(tokio::sync::Notify::new());
    loop {
        // Register first, then look. The other order loses a change that lands
        // between the look and the wait, and the caller then waits out the
        // whole budget holding an answer that was already stale.
        reg.park(&beat.watch, &bell);
        let waiting = bell.notified();
        let fresh = reg.deltas_shared_for(beat);
        if !fresh.is_empty() {
            ack.pools = fresh;
            return ack;
        }
        if tokio::time::timeout_at(deadline, waiting).await.is_err() {
            return ack;
        }
    }
}

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
            match serde_json::from_slice::<tinyray_proto::Beat>(&bytes) {
                Ok(beat) => {
                    let ack = hold(&reg, &beat).await;
                    reply(StatusCode::OK, serde_json::to_vec(&ack).unwrap())
                }
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
                    (
                        k,
                        serde_json::json!({"version": v, "roster": r, "members": n}),
                    )
                })
                .collect();
            reply(StatusCode::OK, serde_json::to_vec(&snap).unwrap())
        }
        // Says who it is as well as that it is up, so a deployment can check
        // for a version-skewed registry without joining one.
        ("GET", "/health") => reply(
            StatusCode::OK,
            format!(
                "{{\"status\":\"ok\",\"version\":\"{}\",\"protocol\":{}}}",
                env!("CARGO_PKG_VERSION"),
                tinyray_proto::PROTOCOL
            )
            .into_bytes(),
        ),
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::task::{Context, Poll, Waker};

    #[tokio::test]
    async fn held_replies_share_one_consistent_delta_after_a_change() {
        let reg = Registry::new(Duration::from_secs(10));
        let mut member: tinyray_proto::Beat = serde_json::from_value(serde_json::json!({
            "pool": "p", "id": 1, "incarnation": 1,
            "publication": 0, "policy": "churn"
        }))
        .unwrap();
        assert!(reg.beat(&member).accepted);
        let seen = reg.snapshot()["p"].0;
        let watchers: Vec<tinyray_proto::Beat> = (0..4)
            .map(|id| {
                serde_json::from_value(serde_json::json!({
                    "pool": "watchers", "id": id, "incarnation": 1,
                    "publication": 0, "policy": "churn",
                    "watch": ["p"], "seen": {"p": seen}, "hold_ms": 1000
                }))
                .unwrap()
            })
            .collect();
        let mut context = Context::from_waker(Waker::noop());
        let mut replies: Vec<_> = watchers.iter().map(|b| Box::pin(hold(&reg, b))).collect();
        for reply in &mut replies {
            assert!(matches!(reply.as_mut().poll(&mut context), Poll::Pending));
        }
        member.publication = Some(1);
        member.state = serde_json::json!({"step": 7});
        member.ready = true;
        assert!(reg.beat(&member).accepted);
        let mut first = None;
        for reply in replies {
            let ack = tokio::time::timeout(Duration::from_millis(100), reply)
                .await
                .expect("notification was lost");
            let d = &ack.pools["p"];
            assert!(d.changed[0].ready);
            assert_eq!(d.changed[0].state, member.state);
            if let Some(previous) = &first {
                assert!(Arc::ptr_eq(previous, d));
            }
            first = Some(d.clone());
            let decoded: tinyray_proto::BeatAck =
                serde_json::from_slice(&serde_json::to_vec(&ack).unwrap()).unwrap();
            assert_eq!(decoded.pools["p"].version, d.version);
            assert!(decoded.accepted);
        }
    }
}

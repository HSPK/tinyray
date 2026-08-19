//! Drives many members against a registry from one process, so scale tests do
//! not need thousands of Python interpreters.

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioTimer};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tinyray_proto::{Beat, BeatAck};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut endpoint = "http://127.0.0.1:8760".to_string();
    let mut members = 1000usize;
    let mut secs = 5u64;
    let mut interval_ms = 500u64;
    let mut watchers = 1usize;
    let mut offset = 0usize;
    let mut conns = 16usize;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--endpoint" => endpoint = args.next().map(|e| format!("http://{e}")).unwrap(),
            "--members" => members = args.next().unwrap().parse()?,
            "--seconds" => secs = args.next().unwrap().parse()?,
            "--interval-ms" => interval_ms = args.next().unwrap().parse()?,
            "--watchers" => watchers = args.next().unwrap().parse()?,
            "--offset" => offset = args.next().unwrap().parse()?,
            "--conns" => conns = args.next().unwrap().parse()?,
            o => return Err(format!("unknown argument {o}").into()),
        }
    }

    // One h2 connection multiplexes, but only up to the server's stream limit.
    // A real process holds one member; simulating thousands from one process
    // needs several connections or we measure stream queueing, not the server.
    let clients: Vec<Arc<Client<_, Full<Bytes>>>> = (0..conns)
        .map(|_| {
            Arc::new(
                Client::builder(TokioExecutor::new())
                    .timer(TokioTimer::new())
                    .http2_only(true)
                    .pool_max_idle_per_host(64)
                    .build_http(),
            )
        })
        .collect();
    let ok = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let lat_us = Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
    let deadline = Instant::now() + Duration::from_secs(secs);

    let mut tasks = Vec::new();
    for i in 0..members {
        let http = clients[i % conns].clone();
        let (ok, failed, ep) = (ok.clone(), failed.clone(), endpoint.clone());
        let lat = lat_us.clone();
        tasks.push(tokio::spawn(async move {
            // Only the first member watches, mirroring the rule that a big
            // pool is watched by few: everyone watching everyone is O(N^2).
            // Only a few members watch: a big pool watched by everyone is
            // O(N^2) traffic, which is a design constraint, not a setting.
            let watch = if i < watchers { vec!["load".to_string()] } else { vec![] };
            let mut seen: HashMap<String, u64> = HashMap::new();
            let mut last_count = 0usize;
            while Instant::now() < deadline {
                let beat = Beat {
                    pool: "load".into(),
                    slot: None,
                    id: (offset + i) as u64,
                    incarnation: 1,
                    policy: "churn".into(),
                    size: None,
                    url: Some(format!("http://10.0.0.1:{}", 10000 + i)),
                    state: serde_json::json!({"shard": i % 8}),
                    ready: true,
                    leaving: false,
                    methods: vec![],
                    watch: watch.clone(),
                    seen: seen.clone(),
                };
                let body = Full::new(Bytes::from(serde_json::to_vec(&beat).unwrap()));
                let req = hyper::Request::builder()
                    .method("POST")
                    .uri(format!("{ep}/v1/beat"))
                    .header("content-type", "application/json")
                    .body(body)
                    .unwrap();
                let sent = Instant::now();
                match http.request(req).await {
                    Ok(r) => {
                        lat.lock().unwrap().push(sent.elapsed().as_micros() as u64);
                        // h2 trips its own flood protection when one connection
                        // carries thousands of requests a second. A real member
                        // beats about once a second, so this is a harness limit.
                        let Ok(collected) = r.into_body().collect().await else {
                            failed.fetch_add(1, Ordering::Relaxed);
                            tokio::time::sleep(Duration::from_millis(interval_ms)).await;
                            continue;
                        };
                        let Ok(ack) = serde_json::from_slice::<BeatAck>(&collected.to_bytes()) else {
                            failed.fetch_add(1, Ordering::Relaxed);
                            tokio::time::sleep(Duration::from_millis(interval_ms)).await;
                            continue;
                        };
                        for (n, d) in &ack.pools {
                            seen.insert(n.clone(), d.version);
                            if d.full {
                                last_count = d.changed.len();
                            } else {
                                last_count = last_count + d.changed.len() - d.removed.len();
                            }
                        }
                        ok.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        failed.fetch_add(1, Ordering::Relaxed);
                    }
                }
                tokio::time::sleep(Duration::from_millis(interval_ms)).await;
            }
            last_count
        }));
    }

    let t0 = Instant::now();
    let mut watcher_saw = 0usize;
    for (i, t) in tasks.into_iter().enumerate() {
        let n = t.await?;
        if i == 0 {
            watcher_saw = n;
        }
    }
    let el = t0.elapsed().as_secs_f64();
    let mut lat = lat_us.lock().unwrap().clone();
    lat.sort_unstable();
    let q = |p: f64| -> u64 {
        if lat.is_empty() { 0 } else { lat[((lat.len() as f64 - 1.0) * p) as usize] }
    };
    println!(
        "{{\"members\":{},\"watcher_saw\":{},\"beats_ok\":{},\"beats_failed\":{},\"ops_per_s\":{:.0},\"p50_us\":{},\"p99_us\":{},\"max_us\":{}}}",
        members,
        watcher_saw,
        ok.load(Ordering::Relaxed),
        failed.load(Ordering::Relaxed),
        ok.load(Ordering::Relaxed) as f64 / el,
        q(0.50), q(0.99), q(1.0)
    );
    Ok(())
}

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
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--endpoint" => endpoint = args.next().map(|e| format!("http://{e}")).unwrap(),
            "--members" => members = args.next().unwrap().parse()?,
            "--seconds" => secs = args.next().unwrap().parse()?,
            o => return Err(format!("unknown argument {o}").into()),
        }
    }

    let http: Client<_, Full<Bytes>> =
        Client::builder(TokioExecutor::new()).timer(TokioTimer::new()).build_http();
    let http = Arc::new(http);
    let ok = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let deadline = Instant::now() + Duration::from_secs(secs);

    let mut tasks = Vec::new();
    for i in 0..members {
        let (http, ok, failed, ep) = (http.clone(), ok.clone(), failed.clone(), endpoint.clone());
        tasks.push(tokio::spawn(async move {
            // Only the first member watches, mirroring the rule that a big
            // pool is watched by few: everyone watching everyone is O(N^2).
            let watch = if i == 0 { vec!["load".to_string()] } else { vec![] };
            let mut seen: HashMap<String, u64> = HashMap::new();
            let mut last_count = 0usize;
            while Instant::now() < deadline {
                let beat = Beat {
                    pool: "load".into(),
                    slot: None,
                    id: i as u64,
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
                match http.request(req).await {
                    Ok(r) => {
                        let b = r.into_body().collect().await.unwrap().to_bytes();
                        let ack: BeatAck = serde_json::from_slice(&b).unwrap();
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
                tokio::time::sleep(Duration::from_millis(500)).await;
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
    println!(
        "{{\"members\":{},\"watcher_saw\":{},\"beats_ok\":{},\"beats_failed\":{},\"ops_per_s\":{:.0}}}",
        members,
        watcher_saw,
        ok.load(Ordering::Relaxed),
        failed.load(Ordering::Relaxed),
        ok.load(Ordering::Relaxed) as f64 / el
    );
    Ok(())
}

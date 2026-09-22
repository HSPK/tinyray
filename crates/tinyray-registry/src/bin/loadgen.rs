//! Drives many members against a registry from one process, so scale tests do
//! not need thousands of Python interpreters.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tinyray_proto::wire::{
    decode_message, read_frame, write_frame, RegistryEnvelope, RegistryEnvelopeHeader,
    RegistryProtocolError, MAX_REQUEST_FRAME_BYTES, MAX_RESPONSE_FRAME_BYTES, OP_BEAT, OP_BEAT_ACK,
    OP_ERROR,
};
use tinyray_proto::{Beat, BeatAck};
use tokio::net::TcpStream;

async fn request_beat(
    endpoint: &str,
    body: &Beat,
    connection: Option<TcpStream>,
    connections: &AtomicU64,
) -> Result<(BeatAck, TcpStream), String> {
    static REQUEST_ID: AtomicU64 = AtomicU64::new(1);
    let request_id = REQUEST_ID.fetch_add(1, Ordering::Relaxed);
    let mut stream = match connection {
        Some(stream) => stream,
        None => {
            let stream = TcpStream::connect(endpoint)
                .await
                .map_err(|e| format!("connect: {e}"))?;
            stream
                .set_nodelay(true)
                .map_err(|e| format!("TCP_NODELAY: {e}"))?;
            connections.fetch_add(1, Ordering::Relaxed);
            stream
        }
    };
    let request = RegistryEnvelope::new(request_id, OP_BEAT, body);
    write_frame(&mut stream, &request, MAX_REQUEST_FRAME_BYTES)
        .await
        .map_err(|e| format!("write: {e}"))?;
    let raw = read_frame(&mut stream, MAX_RESPONSE_FRAME_BYTES)
        .await
        .map_err(|e| format!("read: {e}"))?
        .ok_or_else(|| "read: EOF before reply".to_string())?;
    let header: RegistryEnvelopeHeader =
        decode_message(&raw).map_err(|e| format!("envelope: {e}"))?;
    if header.request_id != request_id {
        return Err(format!(
            "correlation: got {}, expected {request_id}",
            header.request_id
        ));
    }
    match header.operation.as_str() {
        OP_BEAT_ACK => {
            let reply: RegistryEnvelope<BeatAck> =
                decode_message(&raw).map_err(|e| format!("BeatAck: {e}"))?;
            Ok((reply.payload, stream))
        }
        OP_ERROR => {
            let reply: RegistryEnvelope<RegistryProtocolError> =
                decode_message(&raw).map_err(|e| format!("protocol error: {e}"))?;
            Err(format!("{}: {}", reply.payload.code, reply.payload.message))
        }
        operation => Err(format!("unexpected reply operation {operation:?}")),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut endpoint = "127.0.0.1:8760".to_string();
    let mut members = 1000usize;
    let mut secs = 5u64;
    let mut interval_ms = 500u64;
    let mut watchers = 1usize;
    let mut offset = 0usize;
    let mut _connections_hint = 16usize;
    let mut watch_pool = "load".to_string();
    let mut hold = 0u64;
    let mut reconnect_every_beat = false;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--endpoint" => endpoint = args.next().unwrap(),
            "--members" => members = args.next().unwrap().parse()?,
            "--seconds" => secs = args.next().unwrap().parse()?,
            "--interval-ms" => interval_ms = args.next().unwrap().parse()?,
            "--watchers" => watchers = args.next().unwrap().parse()?,
            "--offset" => offset = args.next().unwrap().parse()?,
            // Kept so existing benchmark command lines continue to parse.
            // Each synthetic member now owns one serial persistent socket.
            "--conns" => _connections_hint = args.next().unwrap().parse()?,
            "--watch-pool" => watch_pool = args.next().unwrap(),
            "--hold-ms" => hold = args.next().unwrap().parse()?,
            "--reconnect-every-beat" => reconnect_every_beat = true,
            o => return Err(format!("unknown argument {o}").into()),
        }
    }

    let ok = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let connections = Arc::new(AtomicU64::new(0));
    let lat_us = Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
    let deadline = Instant::now() + Duration::from_secs(secs);

    let mut tasks = Vec::new();
    for i in 0..members {
        let (ok, failed, ep) = (ok.clone(), failed.clone(), endpoint.clone());
        let connections = connections.clone();
        let lat = lat_us.clone();
        let watch_pool = watch_pool.clone();
        tasks.push(tokio::spawn(async move {
            // Only the first member watches, mirroring the rule that a big
            // pool is watched by few: everyone watching everyone is O(N^2).
            // Only a few members watch: a big pool watched by everyone is
            // O(N^2) traffic, which is a design constraint, not a setting.
            let watch = if i < watchers {
                vec![watch_pool.clone()]
            } else {
                vec![]
            };
            let mut seen: HashMap<String, u64> = HashMap::new();
            let mut last_count = 0usize;
            let mut connection = None;
            while Instant::now() < deadline {
                let beat = Beat {
                    pool: "load".into(),
                    slot: None,
                    id: (offset + i) as u64,
                    incarnation: 1,
                    publication: Some(0),
                    policy: "churn".into(),
                    size: None,
                    url: Some(format!("http://10.0.0.1:{}", 10000 + i)),
                    state: serde_json::json!({"shard": i % 8}),
                    ready: true,
                    leaving: false,
                    exclusive: false,
                    methods: vec![],
                    watch: watch.clone(),
                    seen: seen.clone(),
                    hold_ms: hold,
                };
                let sent = Instant::now();
                match request_beat(&ep, &beat, connection.take(), &connections).await {
                    Ok((ack, returned)) => {
                        if !reconnect_every_beat {
                            connection = Some(returned);
                        }
                        lat.lock().unwrap().push(sent.elapsed().as_micros() as u64);
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
        if lat.is_empty() {
            0
        } else {
            lat[((lat.len() as f64 - 1.0) * p) as usize]
        }
    };
    println!(
        "{{\"members\":{},\"watcher_saw\":{},\"beats_ok\":{},\"beats_failed\":{},\"connections\":{},\"reuses\":{},\"ops_per_s\":{:.0},\"p50_us\":{},\"p99_us\":{},\"max_us\":{}}}",
        members,
        watcher_saw,
        ok.load(Ordering::Relaxed),
        failed.load(Ordering::Relaxed),
        connections.load(Ordering::Relaxed),
        ok.load(Ordering::Relaxed).saturating_sub(connections.load(Ordering::Relaxed)),
        ok.load(Ordering::Relaxed) as f64 / el,
        q(0.50), q(0.99), q(1.0)
    );
    Ok(())
}

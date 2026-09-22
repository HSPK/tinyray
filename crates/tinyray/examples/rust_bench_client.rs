use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};
use tinyray::{Client, RpcStatus};
use tinyray_proto::rpc::RpcRequest;

static REQUEST_ID: AtomicU64 = AtomicU64::new(1);
const SERIAL_IDS: [&str; 16] = [
    "serial-0", "serial-1", "serial-2", "serial-3", "serial-4", "serial-5", "serial-6", "serial-7",
    "serial-8", "serial-9", "serial-a", "serial-b", "serial-c", "serial-d", "serial-e", "serial-f",
];

#[derive(Clone, Deserialize, Serialize)]
struct Job {
    task_id: String,
    step: u64,
    scores: Vec<f64>,
}

#[derive(Serialize)]
struct BatchCall {
    method: &'static str,
    args: Vec<serde_json::Value>,
    kwargs: HashMap<String, serde_json::Value>,
}

#[derive(Serialize)]
struct Batch {
    calls: Vec<BatchCall>,
}

fn request_id(prefix: &str) -> String {
    format!("{prefix}-{}", REQUEST_ID.fetch_add(1, Ordering::Relaxed))
}

fn percentiles(mut samples: Vec<Duration>) -> serde_json::Value {
    samples.sort_unstable();
    let at = |fraction: f64| {
        samples[((samples.len() - 1) as f64 * fraction) as usize].as_secs_f64() * 1000.0
    };
    json!({
        "p50_ms": at(0.5),
        "p90_ms": at(0.9),
        "p99_ms": at(0.99),
        "max_ms": at(1.0),
    })
}

fn concurrency(
    client: &Client,
    endpoint: &str,
    target: &str,
    callers: usize,
    no_args: &[u8],
    pong: &[u8],
) -> serde_json::Value {
    let stop = Arc::new(AtomicBool::new(false));
    let gate = Arc::new(Barrier::new(callers + 1));
    let started = Instant::now();
    let samples = std::thread::scope(|scope| {
        let mut threads = Vec::new();
        for _ in 0..callers {
            let client = client.clone();
            let endpoint = endpoint.to_owned();
            let target = target.to_owned();
            let no_args = no_args.to_vec();
            let pong = pong.to_vec();
            let stop = stop.clone();
            let gate = gate.clone();
            threads.push(scope.spawn(move || {
                let mut samples = Vec::new();
                gate.wait();
                while !stop.load(Ordering::Relaxed) {
                    let call_started = Instant::now();
                    let response = client
                        .call_raw(
                            &endpoint,
                            &target,
                            "ping_raw",
                            "rust-bench/0#1",
                            request_id("concurrent"),
                            no_args.clone(),
                            Duration::from_secs(5),
                        )
                        .unwrap();
                    assert_eq!(response, pong);
                    samples.push(call_started.elapsed());
                }
                samples
            }));
        }
        gate.wait();
        std::thread::sleep(Duration::from_secs(2));
        stop.store(true, Ordering::Relaxed);
        threads
            .into_iter()
            .flat_map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>()
    });
    let elapsed = started.elapsed().as_secs_f64();
    let calls = samples.len();
    let stats = client.stats();
    let mut result = percentiles(samples);
    let object = result.as_object_mut().unwrap();
    object.insert("calls".into(), calls.into());
    object.insert(
        "calls_per_s".into(),
        ((calls as f64 / elapsed).round() as u64).into(),
    );
    object.insert("connections".into(), stats.connections.into());
    result
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let endpoint = args.next().ok_or("missing endpoint")?;
    let target = args.next().ok_or("missing target")?;
    let client = Client::from_current()?;
    let no_args = rmp_serde::to_vec_named(&NoArgs {
        args: Vec::<()>::new(),
        kwargs: HashMap::<String, ()>::new(),
    })?;
    let pong = rmp_serde::to_vec_named("pong")?;

    for index in 0..100 {
        let response = client
            .call_raw_async(
                &endpoint,
                &target,
                "ping_raw",
                "rust-bench/0#1",
                SERIAL_IDS[index % SERIAL_IDS.len()],
                no_args.clone(),
                Duration::from_secs(5),
            )
            .await?;
        assert_eq!(response, pong);
    }
    let mut no_op = Vec::with_capacity(2_000);
    for index in 0..2_000 {
        let started = Instant::now();
        let response = client
            .call_raw_async(
                &endpoint,
                &target,
                "ping_raw",
                "rust-bench/0#1",
                SERIAL_IDS[index % SERIAL_IDS.len()],
                no_args.clone(),
                Duration::from_secs(5),
            )
            .await?;
        assert_eq!(response, pong);
        no_op.push(started.elapsed());
    }

    let blob = "x".repeat(64 << 10);
    let mut payload_64k = Vec::with_capacity(300);
    for index in 0..300 {
        let started = Instant::now();
        let response: String = client
            .call_arg_async(
                &endpoint,
                &target,
                "echo",
                "rust-bench/0#1",
                SERIAL_IDS[index % SERIAL_IDS.len()],
                &blob,
                Duration::from_secs(5),
            )
            .await?;
        assert_eq!(response.len(), blob.len());
        payload_64k.push(started.elapsed());
    }

    let job = Job {
        task_id: "rollout".into(),
        step: 7,
        scores: vec![0.25, 0.5, 0.75],
    };
    let mut typed = Vec::with_capacity(1_000);
    for index in 0..1_000 {
        let started = Instant::now();
        let response: Job = client
            .call_arg_async(
                &endpoint,
                &target,
                "job",
                "rust-bench/0#1",
                SERIAL_IDS[index % SERIAL_IDS.len()],
                &job,
                Duration::from_secs(5),
            )
            .await?;
        assert_eq!(response.step, job.step);
        typed.push(started.elapsed());
    }

    let batch = Batch {
        calls: (0..32)
            .map(|_| BatchCall {
                method: "ping_raw",
                args: Vec::new(),
                kwargs: HashMap::new(),
            })
            .collect(),
    };
    let batch_payload = rmp_serde::to_vec_named(&batch)?;
    let mut batch_samples = Vec::with_capacity(100);
    for index in 0..100 {
        let started = Instant::now();
        let reply = client
            .request_async(
                RpcRequest::batch(
                    SERIAL_IDS[index % SERIAL_IDS.len()],
                    "rust-bench/0#1",
                    &target,
                    32,
                    batch_payload.clone(),
                ),
                endpoint.clone(),
                Duration::from_secs(5),
            )
            .await?;
        assert_eq!(reply.status, RpcStatus::Success);
        batch_samples.push(started.elapsed());
    }

    let concurrency = [1, 8, 32, 128]
        .into_iter()
        .map(|callers| {
            (
                callers.to_string(),
                concurrency(&client, &endpoint, &target, callers, &no_args, &pong),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    println!(
        "{}",
        serde_json::to_string(&json!({
            "no_op": percentiles(no_op),
            "payload_64k": percentiles(payload_64k),
            "typed": percentiles(typed),
            "batch_32": percentiles(batch_samples),
            "concurrency": concurrency,
        }))?
    );
    Ok(())
}

#[derive(Serialize)]
struct NoArgs {
    args: Vec<()>,
    kwargs: HashMap<String, ()>,
}

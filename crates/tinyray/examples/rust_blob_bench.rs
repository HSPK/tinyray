use serde::Serialize;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tinyray::{BlobRef, Client, Router, Server, ServerConfig};

const IDS: [&str; 16] = [
    "blob-0", "blob-1", "blob-2", "blob-3", "blob-4", "blob-5", "blob-6", "blob-7", "blob-8",
    "blob-9", "blob-a", "blob-b", "blob-c", "blob-d", "blob-e", "blob-f",
];

#[derive(Serialize)]
struct OneArg<'a, T> {
    args: [&'a T; 1],
    kwargs: HashMap<String, ()>,
}

fn median_ms(mut samples: Vec<Duration>) -> f64 {
    samples.sort_unstable();
    samples[samples.len() / 2].as_secs_f64() * 1000.0
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut router = Router::new();
    router
        .typed_arg("bytes_len", |_context, value: Vec<u8>| async move {
            Ok(value.len())
        })?
        .typed_arg("blob_len", |_context, value: BlobRef| async move {
            Ok(value.len())
        })?;
    let mut server = Server::start(
        ServerConfig::new("127.0.0.1:0", "blob-bench/0#1"),
        Arc::new(router),
    )?;
    let client = Client::from_current()?;
    let target = client
        .target(server.endpoint(), "blob-bench/0#1")
        .caller("rust-blob-bench/0#1");
    let mut result = serde_json::Map::new();

    for size in [64 << 10, 1 << 20, 16 << 20] {
        let data = vec![7u8; size];
        let creation_rounds = if size >= 16 << 20 { 5 } else { 20 };
        let creation = (0..creation_rounds)
            .map(|_| {
                let started = Instant::now();
                let blob = BlobRef::from_bytes(&data).unwrap();
                assert_eq!(blob.len(), size);
                started.elapsed()
            })
            .collect();
        let blob = BlobRef::from_bytes(&data)?;
        let ordinary_rounds = if size >= 16 << 20 { 5 } else { 30 };
        let blob_rounds = if size >= 16 << 20 { 50 } else { 100 };
        let ordinary = {
            let mut samples = Vec::new();
            for index in 0..ordinary_rounds {
                let started = Instant::now();
                let length: usize = target
                    .call_arg_async(
                        "bytes_len",
                        IDS[index % IDS.len()],
                        &data,
                        Duration::from_secs(10),
                    )
                    .await?;
                assert_eq!(length, size);
                samples.push(started.elapsed());
            }
            samples
        };
        let shared = {
            let mut samples = Vec::new();
            for index in 0..blob_rounds {
                let started = Instant::now();
                let length: usize = target
                    .call_arg_async(
                        "blob_len",
                        IDS[index % IDS.len()],
                        &blob,
                        Duration::from_secs(10),
                    )
                    .await?;
                assert_eq!(length, size);
                samples.push(started.elapsed());
            }
            samples
        };
        let access = (0..500)
            .map(|_| {
                let started = Instant::now();
                std::hint::black_box(blob.as_slice().unwrap()[0]);
                started.elapsed()
            })
            .collect();
        let ordinary_wire = rmp_serde::to_vec_named(&OneArg {
            args: [&data],
            kwargs: HashMap::new(),
        })?
        .len();
        let blob_wire = rmp_serde::to_vec_named(&OneArg {
            args: [&blob],
            kwargs: HashMap::new(),
        })?
        .len();
        result.insert(
            size.to_string(),
            json!({
                "creation_ms": median_ms(creation),
                "ordinary_call_ms": median_ms(ordinary),
                "blob_call_ms": median_ms(shared),
                "access_ms": median_ms(access),
                "ordinary_wire_bytes": ordinary_wire,
                "blob_wire_bytes": blob_wire,
            }),
        );
    }
    server.close();
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}

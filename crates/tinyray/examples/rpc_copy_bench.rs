use serde::Deserialize;
use serde_json::json;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;
use tinyray_proto::rpc::{RpcOperation, RpcRequest, MAX_RPC_FRAME_BYTES};
use tinyray_proto::wire::{decode_message, encode_message};

#[derive(Deserialize)]
struct BorrowedRequest<'a> {
    #[serde(rename = "v")]
    _protocol: u16,
    #[serde(rename = "id")]
    _request_id: String,
    #[serde(rename = "from")]
    _caller: String,
    #[serde(rename = "to")]
    _target: String,
    #[serde(rename = "op")]
    _operation: RpcOperation,
    #[serde(default)]
    _method: Option<String>,
    #[serde(rename = "batch", default)]
    _batch_len: Option<u16>,
    #[serde(borrow, rename = "body", with = "serde_bytes")]
    payload: &'a [u8],
}

fn median_us(mut samples: Vec<f64>) -> f64 {
    samples.sort_by(f64::total_cmp);
    samples[samples.len() / 2]
}

fn measure(mut operation: impl FnMut(), rounds: usize) -> f64 {
    let mut samples = Vec::with_capacity(rounds);
    for _ in 0..rounds {
        let started = Instant::now();
        operation();
        samples.push(started.elapsed().as_secs_f64() * 1_000_000.0);
    }
    median_us(samples)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut out = serde_json::Map::new();
    for size in [64usize, 64 << 10, 1 << 20] {
        let rounds = match size {
            ..=64 => 5_000,
            65..=65536 => 1_000,
            _ => 200,
        };
        let payload = vec![0x5a; size];
        let request = RpcRequest::call(
            "copy-profile",
            "caller/0#1",
            "worker/0#1",
            "echo",
            payload.clone(),
        );
        let encoded = encode_message(&request, MAX_RPC_FRAME_BYTES)?;
        let clone_us = measure(
            || {
                black_box(payload.clone());
            },
            rounds,
        );
        let encode_us = measure(
            || {
                black_box(encode_message(&request, MAX_RPC_FRAME_BYTES).unwrap());
            },
            rounds,
        );
        let decode_us = measure(
            || {
                black_box(decode_message::<RpcRequest>(&encoded).unwrap());
            },
            rounds,
        );
        let borrowed_decode_us = measure(
            || {
                let request: BorrowedRequest<'_> = rmp_serde::from_slice(&encoded).unwrap();
                black_box(Arc::<[u8]>::from(request.payload));
            },
            rounds,
        );
        let round_trip_us = measure(
            || {
                let encoded = encode_message(&request, MAX_RPC_FRAME_BYTES).unwrap();
                black_box(decode_message::<RpcRequest>(&encoded).unwrap());
            },
            rounds,
        );
        out.insert(
            size.to_string(),
            json!({
                "bytes": size,
                "wire_bytes": encoded.len(),
                "clone_us": clone_us,
                "encode_us": encode_us,
                "decode_us": decode_us,
                "borrowed_decode_us": borrowed_decode_us,
                "decode_speedup": decode_us / borrowed_decode_us,
                "round_trip_us": round_trip_us,
            }),
        );
    }
    println!("{}", serde_json::Value::Object(out));
    Ok(())
}

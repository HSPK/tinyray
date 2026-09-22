use serde::Serialize;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tinyray::{BlobRef, MemberBuilder, Router, ServiceError};

#[derive(Serialize)]
struct SeenContext {
    caller: String,
    request_id: String,
}

#[derive(serde::Deserialize, Serialize)]
struct Job {
    task_id: String,
    step: u64,
    scores: Vec<f64>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let registry = args.next().unwrap_or_else(|| "127.0.0.1:8760".into());
    let pool = args.next().unwrap_or_else(|| "rust-service".into());
    let slot = args
        .next()
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(0);
    let size = args
        .next()
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(1);

    let mut router = Router::new();
    let pong = std::sync::Arc::new(rmp_serde::to_vec_named("pong")?);
    let retained = Arc::new(Mutex::new(None::<BlobRef>));
    router
        .raw("ping_raw", {
            let pong = pong.clone();
            move |_context, _payload| {
                let pong = pong.clone();
                async move { Ok(pong.as_ref().clone()) }
            }
        })?
        .typed_no_args("ping", |_context| async move { Ok("pong") })?
        .typed_arg("echo", |_context, value: serde_json::Value| async move {
            Ok(value)
        })?
        .typed_arg(
            "echo_bytes",
            |_context, value: Vec<u8>| async move { Ok(value) },
        )?
        .typed_arg("job", |_context, value: Job| async move { Ok(value) })?
        .typed_arg("blob_len", |_context, value: BlobRef| async move {
            Ok(value.len())
        })?
        .typed_arg("bytes_len", |_context, value: Vec<u8>| async move {
            Ok(value.len())
        })?
        .typed_arg(
            "echo_blob",
            |_context, value: BlobRef| async move { Ok(value) },
        )?
        .typed_no_args("make_blob", |_context| async move {
            BlobRef::from_bytes(b"made-by-rust")
                .map_err(|error| ServiceError::remote("BlobError", error.to_string()))
        })?
        .typed_arg("retain_blob", {
            let retained = retained.clone();
            move |_context, value: BlobRef| {
                let retained = retained.clone();
                async move {
                    let length = value.len();
                    *retained.lock().unwrap() = Some(value);
                    Ok(length)
                }
            }
        })?
        .typed_no_args("retained_len", {
            let retained = retained.clone();
            move |_context| {
                let retained = retained.clone();
                async move { Ok(retained.lock().unwrap().as_ref().map_or(0, BlobRef::len)) }
            }
        })?
        .typed_no_args("retained_blob", {
            let retained = retained.clone();
            move |_context| {
                let retained = retained.clone();
                async move {
                    retained.lock().unwrap().clone().ok_or_else(|| {
                        ServiceError::remote("BlobError", "no BlobRef has been retained")
                    })
                }
            }
        })?
        .typed_no_args("context", |context| async move {
            Ok(SeenContext {
                caller: context.caller.to_string(),
                request_id: context.request_id.to_string(),
            })
        })?
        .typed_arg("sleep_ms", |context, millis: u64| async move {
            tokio::select! {
                _ = context.cancellation.cancelled() => Err(ServiceError::Cancelled),
                _ = tokio::time::sleep(Duration::from_millis(millis)) => Ok(millis),
            }
        })?
        .typed_no_args::<(), _, _>("fail", |_context| async move {
            Err(ServiceError::remote("RustError", "expected Rust failure"))
        })?;

    let member = MemberBuilder::new(registry, pool)
        .policy("stateful")
        .slot(slot)
        .size(size)
        .router(router)
        .join(Duration::from_secs(15))?;
    member.ready(&serde_json::json!({"language": "rust"}))?;
    member.flush(Duration::from_secs(5))?;
    println!("READY {} {}", member.identity(), member.endpoint().unwrap());

    let mut line = String::new();
    io::stdin().read_line(&mut line)?;
    member.leave();
    Ok(())
}

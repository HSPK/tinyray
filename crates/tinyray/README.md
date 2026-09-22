# tinyray Rust SDK

`tinyray` embeds the native registry membership and multiplexed method RPC
protocol without Python or PyO3.

```rust
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use tinyray::{Client, MemberBuilder, Router};

#[derive(Deserialize, Serialize)]
struct Job {
    step: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut router = Router::new();
    router.typed_arg("run", |context, job: Job| async move {
        println!("{} {}", context.caller, context.request_id);
        Ok(Job { step: job.step + 1 })
    })?;

    let member = MemberBuilder::new("127.0.0.1:8760", "workers")
        .policy("stateful")
        .slot(0)
        .size(1)
        .router(router)
        .join_async(Duration::from_secs(15))
        .await?;
    member.ready(&serde_json::json!({"language": "rust"}))?;
    member.flush_async(Duration::from_secs(5)).await?;

    let client = Client::from_current()?;
    let target = client
        .target(member.endpoint().unwrap(), member.identity())
        .caller("driver/0#1");
    let answer: Job =
        target.call_arg_async("run", "job-1", &Job { step: 4 }, Duration::from_secs(2)).await?;
    assert_eq!(answer.step, 5);

    member.leave_async().await?;
    Ok(())
}
```

`Router::raw` receives the opaque application MessagePack bytes as
`Arc<[u8]>`. `typed`, `typed_arg`, and `typed_no_args` opt into serde decoding.
Requests are never retried automatically; callers own request IDs and retry
policy.

Linux applications can opt into same-host shared-memory payloads:

```rust
use std::time::Duration;
use tinyray::BlobRef;

let blob = BlobRef::from_bytes(&weights)?;
let loaded: usize =
    target.call_arg("load", "load-1", &blob, Duration::from_secs(5))?;
```

`BlobRef` copies once into a sealed 0600/CLOEXEC `memfd`; receivers validate
the Linux boot identity, pid/fd, device/inode, size, seals, and an in-file token
before mapping read-only. It never falls back to ordinary bytes across hosts or
unsupported platforms. Each decoded message is limited to 64 references and
512 MiB of distinct mappings, with identical descriptors sharing one
`Arc<BlobInner>`. Request owners survive delivered cancellation/timeouts, and
response owners remain live until the caller acknowledges successful decoding.
Every forwarded descriptor names the current process's own verified fd. Raw
calls return `ReceivedRawReply` (and low-level requests return
`ReceivedRpcReply`); keep the guard alive while inspecting bytes or call
`decode<T>()`. Reply owners are bounded per reply, connection, server, and
process.

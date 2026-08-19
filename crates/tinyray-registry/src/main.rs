mod server;
mod state;

use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut listen = "127.0.0.1:8760".to_string();
    let mut ttl_ms = 20_000u64;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--listen" => listen = args.next().unwrap_or(listen),
            "--ttl-ms" => ttl_ms = args.next().and_then(|v| v.parse().ok()).unwrap_or(ttl_ms),
            other => return Err(format!("unknown argument {other}").into()),
        }
    }

    let reg = Arc::new(state::Registry::new(Duration::from_millis(ttl_ms)));

    // Expiry runs on a timer, never on the request path.
    let sweeper = reg.clone();
    let every = Duration::from_millis((ttl_ms / 4).clamp(50, 1000));
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(every);
        loop {
            tick.tick().await;
            sweeper.sweep();
        }
    });

    let listener = TcpListener::bind(&listen).await?;
    println!("tinyray-registry listening on {} ttl={}ms", listener.local_addr()?, ttl_ms);
    server::serve(listener, reg).await;
    Ok(())
}

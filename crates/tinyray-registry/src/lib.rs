//! The registry, usable both as a standalone binary and from the Python
//! package, so `pip install tinyray` gives you the server too.

pub mod server;
pub mod state;

use std::sync::Arc;
use std::time::Duration;

/// Bind and serve until the process ends. Returns the bound address through
/// `on_ready` first, so callers that asked for port 0 can learn it.
pub fn run(listen: &str, ttl_ms: u64, on_ready: impl FnOnce(String)) -> std::io::Result<()> {
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
    rt.block_on(async move {
        let reg = Arc::new(state::Registry::new(Duration::from_millis(ttl_ms)));

        // Expiry runs on a timer, never on the request path: an earlier
        // version swept inside lookup and made it O(N).
        let sweeper = reg.clone();
        let every = Duration::from_millis((ttl_ms / 4).clamp(50, 1000));
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(every);
            loop {
                tick.tick().await;
                sweeper.sweep();
            }
        });

        let listener = tokio::net::TcpListener::bind(listen).await?;
        on_ready(listener.local_addr()?.to_string());
        server::serve(listener, reg).await;
        Ok(())
    })
}

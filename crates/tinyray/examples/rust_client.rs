use std::time::Duration;
use tinyray::Client;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let endpoint = args.next().ok_or("missing endpoint")?;
    let target = args.next().ok_or("missing target")?;
    let value: serde_json::Value =
        serde_json::from_str(&args.next().unwrap_or_else(|| "null".into()))?;
    let client = Client::default();
    let response: serde_json::Value = client.call_arg(
        endpoint,
        target,
        "echo",
        "rust-client/0#1",
        "rust-client-call",
        &value,
        Duration::from_secs(5),
    )?;
    println!("{}", serde_json::to_string(&response)?);
    Ok(())
}

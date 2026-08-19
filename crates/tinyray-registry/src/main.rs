fn main() -> Result<(), Box<dyn std::error::Error>> {
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
    tinyray_registry::run(&listen, ttl_ms, |addr| {
        println!("tinyray listening on {addr} ttl={ttl_ms}ms");
    })?;
    Ok(())
}

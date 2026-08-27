fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut listen = "127.0.0.1:8760".to_string();
    let mut ttl_ms = 20_000u64;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        // A value that will not parse is a typo, and falling back to the
        // default hid it: `--ttl-ms abc` started a registry on the default
        // 20000 and said so as if it had been asked to. The shipped entry
        // point is the Python one, which refuses both, so this refuses both.
        match a.as_str() {
            "--listen" => listen = args.next().ok_or("--listen needs a value")?,
            "--ttl-ms" => {
                let v = args.next().ok_or("--ttl-ms needs a value")?;
                ttl_ms = v
                    .parse()
                    .map_err(|_| format!("--ttl-ms {v:?} is not a length of time"))?;
            }
            other => return Err(format!("unknown argument {other}").into()),
        }
    }
    tinyray_registry::run(&listen, ttl_ms, |addr| {
        println!("tinyray listening on {addr} ttl={ttl_ms}ms");
    })?;
    Ok(())
}

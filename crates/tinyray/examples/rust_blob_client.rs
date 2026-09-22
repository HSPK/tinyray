use std::time::Duration;
use tinyray::{BlobRef, Client};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let endpoint = args.next().ok_or("missing endpoint")?;
    let target = args.next().ok_or("missing target")?;
    let size = args
        .next()
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(1 << 20);
    let data = (0..size)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let mut source = BlobRef::from_bytes(&data)?;
    let client = Client::default();
    let target = client
        .target(endpoint, target)
        .caller("rust-blob-client/0#1");
    let length: usize = target.call_arg(
        "retain_blob",
        "rust-blob-retain",
        &source,
        Duration::from_secs(5),
    )?;
    let received: BlobRef = target.call_arg(
        "echo_blob",
        "rust-blob-echo",
        &source,
        Duration::from_secs(5),
    )?;
    source.close();
    assert_eq!(length, data.len());
    assert_eq!(received.as_slice()?, data);
    let retained: BlobRef = target.call_no_args(
        "retained_blob",
        "rust-blob-retained",
        Duration::from_secs(5),
    )?;
    assert_eq!(retained.as_slice()?, data);
    println!("{length}");
    Ok(())
}

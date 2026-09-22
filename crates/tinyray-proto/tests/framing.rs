use tinyray_proto::wire::{
    decode_message, encode_message, read_frame, write_frame, write_frame_bytes, FrameError,
    RegistryEnvelope, MAX_REQUEST_FRAME_BYTES, OP_HEALTH,
};
use tokio::io::{AsyncWriteExt, DuplexStream};

#[cfg(target_os = "linux")]
use tinyray_proto::wire::bind_tcp_listener;

async fn fragmented_write(mut writer: DuplexStream, bytes: Vec<u8>) {
    for byte in bytes {
        writer.write_all(&[byte]).await.unwrap();
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn preencoded_frames_use_the_same_size_checks() {
    let mut bytes = Vec::new();
    write_frame_bytes(&mut bytes, b"abc", 3).await.unwrap();
    assert_eq!(&bytes[..4], &3u32.to_be_bytes());
    assert_eq!(&bytes[4..], b"abc");
    assert!(matches!(
        write_frame_bytes(&mut Vec::new(), b"abcd", 3).await,
        Err(FrameError::FrameTooLarge {
            length: 4,
            maximum: 3
        })
    ));
}

#[tokio::test]
async fn partial_prefixes_and_bodies_are_reassembled() {
    let envelope = RegistryEnvelope::new(17, OP_HEALTH, ());
    let payload = encode_message(&envelope, MAX_REQUEST_FRAME_BYTES).unwrap();
    let mut framed = (payload.len() as u32).to_be_bytes().to_vec();
    framed.extend_from_slice(&payload);
    let (writer, mut reader) = tokio::io::duplex(1);
    let sending = tokio::spawn(fragmented_write(writer, framed));
    let received = read_frame(&mut reader, MAX_REQUEST_FRAME_BYTES)
        .await
        .unwrap()
        .unwrap();
    sending.await.unwrap();
    let decoded: RegistryEnvelope<()> = decode_message(&received).unwrap();
    assert_eq!(decoded.request_id, 17);
    assert_eq!(decoded.operation, OP_HEALTH);
}

#[tokio::test]
async fn write_frame_completes_across_partial_writes() {
    let envelope = RegistryEnvelope::new(23, OP_HEALTH, ());
    let (mut writer, mut reader) = tokio::io::duplex(2);
    let sending = tokio::spawn(async move {
        write_frame(&mut writer, &envelope, MAX_REQUEST_FRAME_BYTES)
            .await
            .unwrap();
    });
    let received = read_frame(&mut reader, MAX_REQUEST_FRAME_BYTES)
        .await
        .unwrap()
        .unwrap();
    sending.await.unwrap();
    let decoded: RegistryEnvelope<()> = decode_message(&received).unwrap();
    assert_eq!(decoded.request_id, 23);
}

#[tokio::test]
async fn clean_eof_is_distinct_from_truncated_prefix_and_body() {
    let (writer, mut reader) = tokio::io::duplex(8);
    drop(writer);
    assert!(read_frame(&mut reader, MAX_REQUEST_FRAME_BYTES)
        .await
        .unwrap()
        .is_none());

    let (mut writer, mut reader) = tokio::io::duplex(8);
    writer.write_all(&[0, 1]).await.unwrap();
    writer.shutdown().await.unwrap();
    assert!(matches!(
        read_frame(&mut reader, MAX_REQUEST_FRAME_BYTES).await,
        Err(FrameError::TruncatedPrefix { received: 2 })
    ));

    let (mut writer, mut reader) = tokio::io::duplex(16);
    writer.write_all(&5u32.to_be_bytes()).await.unwrap();
    writer.write_all(&[1, 2]).await.unwrap();
    writer.shutdown().await.unwrap();
    assert!(matches!(
        read_frame(&mut reader, MAX_REQUEST_FRAME_BYTES).await,
        Err(FrameError::TruncatedBody {
            expected: 5,
            received: 2
        })
    ));
}

#[tokio::test]
async fn an_oversized_length_is_rejected_before_waiting_for_a_body() {
    let (mut writer, mut reader) = tokio::io::duplex(8);
    writer
        .write_all(&((MAX_REQUEST_FRAME_BYTES + 1) as u32).to_be_bytes())
        .await
        .unwrap();
    let result = tokio::time::timeout(
        std::time::Duration::from_millis(50),
        read_frame(&mut reader, MAX_REQUEST_FRAME_BYTES),
    )
    .await
    .expect("reader waited for a body after rejecting its declared length");
    assert!(matches!(
        result,
        Err(FrameError::FrameTooLarge {
            length,
            maximum: MAX_REQUEST_FRAME_BYTES
        }) if length == MAX_REQUEST_FRAME_BYTES + 1
    ));
}

#[test]
fn malformed_messagepack_is_not_an_envelope() {
    assert!(matches!(
        decode_message::<RegistryEnvelope<()>>(&[0xc1]),
        Err(FrameError::Decode(_))
    ));
    let mut trailing = encode_message(
        &RegistryEnvelope::new(1, OP_HEALTH, ()),
        MAX_REQUEST_FRAME_BYTES,
    )
    .unwrap();
    trailing.push(0xc0);
    assert!(matches!(
        decode_message::<RegistryEnvelope<()>>(&trailing),
        Err(FrameError::Decode(_))
    ));
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn listener_backlog_holds_a_burst_larger_than_the_std_default() {
    let listener = bind_tcp_listener("127.0.0.1:0").unwrap();
    let endpoint = listener.local_addr().unwrap();
    let mut connections = tokio::task::JoinSet::new();
    for _ in 0..400 {
        connections.spawn(async move {
            tokio::time::timeout(
                std::time::Duration::from_millis(750),
                tokio::net::TcpStream::connect(endpoint),
            )
            .await
        });
    }
    let mut held = Vec::new();
    while let Some(result) = connections.join_next().await {
        if let Ok(Ok(Ok(stream))) = result {
            held.push(stream);
        }
    }
    assert_eq!(
        held.len(),
        400,
        "only {} of 400 connections completed before the first SYN retransmit; \
         the listener backlog is below the burst",
        held.len()
    );
}

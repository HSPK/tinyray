use tinyray_proto::rpc::{
    RpcError, RpcOperation, RpcReply, RpcRequest, RpcRequestHeader, RpcStatus, MAX_RPC_FRAME_BYTES,
    RPC_PROTOCOL,
};
use tinyray_proto::wire::{decode_message, encode_message};

#[test]
fn method_frames_keep_the_documented_control_plane_cap() {
    assert_eq!(MAX_RPC_FRAME_BYTES, 32 << 20);
}

#[test]
fn call_envelope_round_trips_opaque_bytes() {
    let request = RpcRequest::call(
        "caller/0#1-7",
        "caller/0#1",
        "worker/3#9",
        "assign",
        vec![0xc4, 0x02, 0x00, 0xff],
    );
    let raw = encode_message(&request, MAX_RPC_FRAME_BYTES).unwrap();
    let decoded: RpcRequest = decode_message(&raw).unwrap();
    assert_eq!(decoded, request);
    assert_eq!(decoded.protocol, RPC_PROTOCOL);
    assert_eq!(decoded.operation, RpcOperation::Call);
}

#[test]
fn batch_failure_carries_correlation_and_prefix_metadata() {
    let reply = RpcReply {
        protocol: RPC_PROTOCOL,
        request_id: "batch-4".into(),
        status: RpcStatus::RemoteError,
        batch_index: Some(2),
        completed: Some(2),
        error: Some(RpcError::new("ValueError", "bad item", "trace")),
        blob_refs: false,
        payload: vec![0x92, 0x01, 0x02],
    };
    let raw = encode_message(&reply, MAX_RPC_FRAME_BYTES).unwrap();
    let decoded: RpcReply = decode_message(&raw).unwrap();
    assert_eq!(decoded, reply);
}

#[test]
fn the_minimal_request_header_recovers_an_id_from_an_untyped_body() {
    let raw = rmp_serde::to_vec_named(&serde_json::json!({
        "id": "correlated",
        "op": 7,
        "body": {"not": "bytes"}
    }))
    .unwrap();
    let header: RpcRequestHeader = decode_message(&raw).unwrap();
    assert_eq!(header.request_id, "correlated");
    assert!(decode_message::<RpcRequest>(&raw).is_err());
}

#[test]
fn blob_ack_is_a_payload_free_correlated_control_frame() {
    let request = RpcRequest::blob_ack("blob-response");
    let raw = encode_message(&request, MAX_RPC_FRAME_BYTES).unwrap();
    let decoded: RpcRequest = decode_message(&raw).unwrap();
    assert_eq!(decoded.operation, RpcOperation::BlobAck);
    assert_eq!(decoded.request_id, "blob-response");
    assert!(decoded.caller.is_empty());
    assert!(decoded.target.is_empty());
    assert!(decoded.payload.is_empty());
}

#[test]
#[ignore = "microbenchmark: run only in an idle measurement window"]
fn rpc_envelope_codec_microbenchmark() {
    let request = RpcRequest::call(
        "bench-1",
        "caller/0#1",
        "worker/0#1",
        "echo",
        vec![b'x'; 64 << 10],
    );
    let rounds = 10_000;
    let started = std::time::Instant::now();
    for _ in 0..rounds {
        let raw = encode_message(&request, MAX_RPC_FRAME_BYTES).unwrap();
        let decoded: RpcRequest = decode_message(&raw).unwrap();
        std::hint::black_box(decoded);
    }
    println!(
        "64 KiB request encode+decode: {:.3} us",
        started.elapsed().as_secs_f64() * 1e6 / rounds as f64
    );
}

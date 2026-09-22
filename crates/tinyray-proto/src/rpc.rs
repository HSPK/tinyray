use serde::{Deserialize, Serialize};

pub const RPC_PROTOCOL: u16 = 1;
/// Method RPC is a control-plane protocol, not a bulk data transport.
///
/// Thirty-two MiB leaves room for model-heavy control messages and complete
/// large exception text plus traceback while
/// bounding both the allocation made from a declared frame length and the
/// amount held by the server-side byte admission budgets.
pub const MAX_RPC_FRAME_BYTES: usize = 32 << 20;
pub const MAX_RPC_REQUEST_ID_BYTES: usize = 200;
pub const MAX_RPC_BATCH: u16 = 128;

fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RpcOperation {
    Call,
    Batch,
    BlobAck,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RpcStatus {
    Success,
    MethodNotFound,
    Fenced,
    CallerFault,
    ConcurrencyRefused,
    RemoteError,
    MalformedProtocol,
    Internal,
}

#[derive(Debug, Deserialize)]
pub struct RpcRequestHeader {
    #[serde(rename = "id")]
    pub request_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RpcError {
    #[serde(rename = "type", default, skip_serializing_if = "String::is_empty")]
    pub type_name: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub traceback: String,
}

impl RpcError {
    pub fn new(
        type_name: impl Into<String>,
        message: impl Into<String>,
        traceback: impl Into<String>,
    ) -> Self {
        Self {
            type_name: type_name.into(),
            message: message.into(),
            traceback: traceback.into(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RpcRequest {
    #[serde(rename = "v")]
    pub protocol: u16,
    #[serde(rename = "id")]
    pub request_id: String,
    #[serde(rename = "from")]
    pub caller: String,
    #[serde(rename = "to")]
    pub target: String,
    #[serde(rename = "op")]
    pub operation: RpcOperation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    #[serde(rename = "batch", default, skip_serializing_if = "Option::is_none")]
    pub batch_len: Option<u16>,
    #[serde(rename = "body", with = "serde_bytes")]
    pub payload: Vec<u8>,
}

impl RpcRequest {
    pub fn call(
        request_id: impl Into<String>,
        caller: impl Into<String>,
        target: impl Into<String>,
        method: impl Into<String>,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            protocol: RPC_PROTOCOL,
            request_id: request_id.into(),
            caller: caller.into(),
            target: target.into(),
            operation: RpcOperation::Call,
            method: Some(method.into()),
            batch_len: None,
            payload,
        }
    }

    pub fn batch(
        request_id: impl Into<String>,
        caller: impl Into<String>,
        target: impl Into<String>,
        batch_len: u16,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            protocol: RPC_PROTOCOL,
            request_id: request_id.into(),
            caller: caller.into(),
            target: target.into(),
            operation: RpcOperation::Batch,
            method: None,
            batch_len: Some(batch_len),
            payload,
        }
    }

    pub fn blob_ack(request_id: impl Into<String>) -> Self {
        Self {
            protocol: RPC_PROTOCOL,
            request_id: request_id.into(),
            caller: String::new(),
            target: String::new(),
            operation: RpcOperation::BlobAck,
            method: None,
            batch_len: None,
            payload: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RpcReply {
    #[serde(rename = "v")]
    pub protocol: u16,
    #[serde(rename = "id")]
    pub request_id: String,
    pub status: RpcStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_index: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
    #[serde(rename = "blobs", default, skip_serializing_if = "is_false")]
    pub blob_refs: bool,
    #[serde(rename = "body", with = "serde_bytes")]
    pub payload: Vec<u8>,
}

impl RpcReply {
    pub fn success(request_id: impl Into<String>, payload: Vec<u8>) -> Self {
        Self {
            protocol: RPC_PROTOCOL,
            request_id: request_id.into(),
            status: RpcStatus::Success,
            batch_index: None,
            completed: None,
            error: None,
            blob_refs: false,
            payload,
        }
    }

    pub fn error(
        request_id: impl Into<String>,
        status: RpcStatus,
        type_name: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            protocol: RPC_PROTOCOL,
            request_id: request_id.into(),
            status,
            batch_index: None,
            completed: None,
            error: Some(RpcError::new(type_name, message, "")),
            blob_refs: false,
            payload: Vec::new(),
        }
    }
}

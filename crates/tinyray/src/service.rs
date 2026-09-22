use async_trait::async_trait;
use rmpv::Value as MsgpackValue;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tinyray_proto::rpc::{RpcError, RpcOperation, RpcStatus, MAX_RPC_REQUEST_ID_BYTES};
use tokio::sync::Notify;

use crate::blob::{
    blob_decode_session, decode_with_blob_session, encode_with_blob_owners, BlobDecodeSession,
    BlobRef,
};

#[derive(Clone, Default)]
pub struct CancellationToken {
    inner: Arc<CancellationInner>,
}

#[derive(Default)]
struct CancellationInner {
    cancelled: AtomicBool,
    notify: Notify,
}

impl CancellationToken {
    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::Acquire)
    }

    pub async fn cancelled(&self) {
        if self.is_cancelled() {
            return;
        }
        self.inner.notify.notified().await;
    }

    pub(crate) fn cancel(&self) {
        if !self.inner.cancelled.swap(true, Ordering::AcqRel) {
            self.inner.notify.notify_waiters();
        }
    }
}

#[derive(Clone)]
pub struct CallContext {
    pub caller: Arc<str>,
    pub request_id: Arc<str>,
    pub target: Arc<str>,
    pub cancellation: CancellationToken,
}

impl CallContext {
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }
}

pub struct ServiceRequest {
    pub context: CallContext,
    pub operation: RpcOperation,
    pub method: Option<String>,
    pub batch_len: Option<u16>,
    pub payload: Arc<[u8]>,
}

#[derive(Clone, Debug)]
pub struct ServiceResponse {
    pub status: RpcStatus,
    pub payload: Vec<u8>,
    pub error: Option<RpcError>,
    pub batch_index: Option<u16>,
    pub completed: Option<u16>,
    #[doc(hidden)]
    pub blob_owners: Vec<BlobRef>,
}

impl ServiceResponse {
    pub fn success(payload: Vec<u8>) -> Self {
        Self {
            status: RpcStatus::Success,
            payload,
            error: None,
            batch_index: None,
            completed: None,
            blob_owners: Vec::new(),
        }
    }

    pub fn success_with_blob_owners(payload: Vec<u8>, blob_owners: Vec<BlobRef>) -> Self {
        Self {
            status: RpcStatus::Success,
            payload,
            error: None,
            batch_index: None,
            completed: None,
            blob_owners,
        }
    }

    pub fn error(error: ServiceError) -> Self {
        let status = error.status();
        Self {
            status,
            payload: Vec::new(),
            error: Some(error.into_rpc()),
            batch_index: None,
            completed: None,
            blob_owners: Vec::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum ServiceError {
    MethodNotFound(String),
    Fenced(String),
    CallerFault(String),
    Busy(String),
    Remote {
        type_name: String,
        message: String,
        traceback: String,
    },
    Internal(String),
    Cancelled,
}

impl ServiceError {
    pub fn remote(type_name: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Remote {
            type_name: type_name.into(),
            message: message.into(),
            traceback: String::new(),
        }
    }

    pub fn status(&self) -> RpcStatus {
        match self {
            Self::MethodNotFound(_) => RpcStatus::MethodNotFound,
            Self::Fenced(_) => RpcStatus::Fenced,
            Self::CallerFault(_) => RpcStatus::CallerFault,
            Self::Busy(_) => RpcStatus::ConcurrencyRefused,
            Self::Remote { .. } => RpcStatus::RemoteError,
            Self::Internal(_) | Self::Cancelled => RpcStatus::Internal,
        }
    }

    fn into_rpc(self) -> RpcError {
        match self {
            Self::MethodNotFound(message) => RpcError::new("AttributeError", message, ""),
            Self::Fenced(message) => RpcError::new("Fenced", message, ""),
            Self::CallerFault(message) => RpcError::new("TypeError", message, ""),
            Self::Busy(message) => RpcError::new("Busy", message, ""),
            Self::Remote {
                type_name,
                message,
                traceback,
            } => RpcError::new(type_name, message, traceback),
            Self::Internal(message) => RpcError::new("RuntimeError", message, ""),
            Self::Cancelled => RpcError::new("Cancelled", "the request was cancelled", ""),
        }
    }
}

impl std::fmt::Display for ServiceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MethodNotFound(message)
            | Self::Fenced(message)
            | Self::CallerFault(message)
            | Self::Busy(message)
            | Self::Internal(message) => f.write_str(message),
            Self::Remote {
                type_name, message, ..
            } => write!(f, "{type_name}: {message}"),
            Self::Cancelled => f.write_str("the request was cancelled"),
        }
    }
}

impl std::error::Error for ServiceError {}

#[async_trait]
pub trait Service: Send + Sync + 'static {
    fn methods(&self) -> &[String];
    async fn dispatch(&self, request: ServiceRequest) -> ServiceResponse;
}

#[async_trait]
trait Handler: Send + Sync {
    async fn call(
        &self,
        context: CallContext,
        payload: Arc<[u8]>,
        session: BlobDecodeSession,
    ) -> Result<ServicePayload, ServiceError>;
}

struct HandlerFn<F>(F);

#[async_trait]
impl<F, Fut> Handler for HandlerFn<F>
where
    F: Fn(CallContext, Arc<[u8]>, BlobDecodeSession) -> Fut + Send + Sync,
    Fut: Future<Output = Result<ServicePayload, ServiceError>> + Send,
{
    async fn call(
        &self,
        context: CallContext,
        payload: Arc<[u8]>,
        session: BlobDecodeSession,
    ) -> Result<ServicePayload, ServiceError> {
        (self.0)(context, payload, session).await
    }
}

struct ServicePayload {
    bytes: Vec<u8>,
    blob_owners: Vec<BlobRef>,
}

#[derive(Default)]
pub struct Router {
    handlers: BTreeMap<String, Arc<dyn Handler>>,
    methods: Vec<String>,
}

impl Router {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn raw<F, Fut>(
        &mut self,
        name: impl Into<String>,
        handler: F,
    ) -> Result<&mut Self, ServiceError>
    where
        F: Fn(CallContext, Arc<[u8]>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<u8>, ServiceError>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        self.raw_internal(name.into(), move |context, payload, _session| {
            let handler = handler.clone();
            async move {
                handler(context, payload).await.map(|bytes| ServicePayload {
                    bytes,
                    blob_owners: Vec::new(),
                })
            }
        })?;
        Ok(self)
    }

    fn raw_internal<F, Fut>(&mut self, name: String, handler: F) -> Result<(), ServiceError>
    where
        F: Fn(CallContext, Arc<[u8]>, BlobDecodeSession) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<ServicePayload, ServiceError>> + Send + 'static,
    {
        self.insert(name, Arc::new(HandlerFn(handler)))
    }

    pub fn typed<Req, Res, F, Fut>(
        &mut self,
        name: impl Into<String>,
        handler: F,
    ) -> Result<&mut Self, ServiceError>
    where
        Req: DeserializeOwned + Send + 'static,
        Res: Serialize + Send + 'static,
        F: Fn(CallContext, Req) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Res, ServiceError>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        self.raw_internal(name.into(), move |context, payload, session| {
            let handler = handler.clone();
            async move {
                let request = decode_with_blob_session::<Req>(&payload, &session)
                    .map_err(|error| ServiceError::CallerFault(error.to_string()))?;
                let response = handler(context, request).await?;
                let (bytes, blob_owners) = encode_with_blob_owners(&response)
                    .map_err(|error| ServiceError::Internal(error.to_string()))?;
                Ok(ServicePayload { bytes, blob_owners })
            }
        })?;
        Ok(self)
    }

    pub fn typed_arg<Req, Res, F, Fut>(
        &mut self,
        name: impl Into<String>,
        handler: F,
    ) -> Result<&mut Self, ServiceError>
    where
        Req: DeserializeOwned + Send + 'static,
        Res: Serialize + Send + 'static,
        F: Fn(CallContext, Req) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Res, ServiceError>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        self.raw_internal(name.into(), move |context, payload, session| {
            let handler = handler.clone();
            async move {
                let request: OneArg<Req> = decode_with_blob_session(&payload, &session)
                    .map_err(|error| ServiceError::CallerFault(error.to_string()))?;
                if !request.kwargs.is_empty() || request.args.len() != 1 {
                    return Err(ServiceError::CallerFault(
                        "expected exactly one positional argument and no keyword arguments".into(),
                    ));
                }
                let response = handler(context, request.args.into_iter().next().unwrap()).await?;
                let (bytes, blob_owners) = encode_with_blob_owners(&response)
                    .map_err(|error| ServiceError::Internal(error.to_string()))?;
                Ok(ServicePayload { bytes, blob_owners })
            }
        })?;
        Ok(self)
    }

    pub fn typed_no_args<Res, F, Fut>(
        &mut self,
        name: impl Into<String>,
        handler: F,
    ) -> Result<&mut Self, ServiceError>
    where
        Res: Serialize + Send + 'static,
        F: Fn(CallContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Res, ServiceError>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        self.raw_internal(name.into(), move |context, payload, session| {
            let handler = handler.clone();
            async move {
                let request: NoArgs = decode_with_blob_session(&payload, &session)
                    .map_err(|error| ServiceError::CallerFault(error.to_string()))?;
                if !request.args.is_empty() || !request.kwargs.is_empty() {
                    return Err(ServiceError::CallerFault(
                        "expected no positional or keyword arguments".into(),
                    ));
                }
                let response = handler(context).await?;
                let (bytes, blob_owners) = encode_with_blob_owners(&response)
                    .map_err(|error| ServiceError::Internal(error.to_string()))?;
                Ok(ServicePayload { bytes, blob_owners })
            }
        })?;
        Ok(self)
    }

    fn insert(&mut self, name: String, handler: Arc<dyn Handler>) -> Result<(), ServiceError> {
        if !public_method(&name) {
            return Err(ServiceError::CallerFault(format!(
                "method {name:?} is not a public ASCII identifier"
            )));
        }
        if self.handlers.insert(name.clone(), handler).is_some() {
            return Err(ServiceError::CallerFault(format!(
                "method {name:?} was registered twice"
            )));
        }
        self.methods = self.handlers.keys().cloned().collect();
        Ok(())
    }

    async fn call(
        &self,
        context: CallContext,
        method: &str,
        payload: Arc<[u8]>,
        session: &BlobDecodeSession,
    ) -> Result<ServicePayload, ServiceError> {
        let handler = self
            .handlers
            .get(method)
            .ok_or_else(|| ServiceError::MethodNotFound(format!("no method {method:?}")))?;
        handler.call(context, payload, session.clone()).await
    }

    async fn batch(&self, request: ServiceRequest) -> ServiceResponse {
        let expected = request.batch_len.unwrap_or_default() as usize;
        let session = blob_decode_session();
        let envelope: BatchEnvelope = match decode_with_blob_session(&request.payload, &session) {
            Ok(envelope) => envelope,
            Err(error) => {
                return ServiceResponse::error(ServiceError::CallerFault(error.to_string()))
            }
        };
        if envelope.calls.len() != expected {
            return ServiceResponse::error(ServiceError::CallerFault(
                "batch metadata does not match the application payload".into(),
            ));
        }
        let mut completed = Vec::with_capacity(expected);
        let mut blob_owners = Vec::new();
        for (index, item) in envelope.calls.into_iter().enumerate() {
            if request.context.is_cancelled() {
                return batch_error(ServiceError::Cancelled, completed, blob_owners, index);
            }
            if !public_method(&item.method) {
                return batch_error(
                    ServiceError::CallerFault("invalid batch method".into()),
                    completed,
                    blob_owners,
                    index,
                );
            }
            let payload = match rmp_serde::to_vec_named(&BatchCallPayload {
                args: item.args,
                kwargs: item.kwargs,
            }) {
                Ok(payload) => Arc::<[u8]>::from(payload),
                Err(error) => {
                    return batch_error(
                        ServiceError::CallerFault(error.to_string()),
                        completed,
                        blob_owners,
                        index,
                    )
                }
            };
            let mut context = request.context.clone();
            context.request_id = Arc::from(batch_request_id(&context.request_id, index));
            match self.call(context, &item.method, payload, &session).await {
                Ok(payload) => {
                    completed.push(payload.bytes);
                    blob_owners.extend(payload.blob_owners);
                }
                Err(error) => return batch_error(error, completed, blob_owners, index),
            }
        }
        ServiceResponse::success_with_blob_owners(raw_array(completed), blob_owners)
    }
}

#[async_trait]
impl Service for Router {
    fn methods(&self) -> &[String] {
        &self.methods
    }

    async fn dispatch(&self, request: ServiceRequest) -> ServiceResponse {
        match request.operation {
            RpcOperation::Call => {
                let Some(method) = request.method.as_deref() else {
                    return ServiceResponse::error(ServiceError::CallerFault(
                        "call metadata has no method".into(),
                    ));
                };
                let session = blob_decode_session();
                match self
                    .call(request.context, method, request.payload, &session)
                    .await
                {
                    Ok(payload) => ServiceResponse::success_with_blob_owners(
                        payload.bytes,
                        payload.blob_owners,
                    ),
                    Err(error) => ServiceResponse::error(error),
                }
            }
            RpcOperation::Batch => self.batch(request).await,
            RpcOperation::BlobAck => ServiceResponse::error(ServiceError::CallerFault(
                "blob acknowledgements are transport control frames".into(),
            )),
        }
    }
}

fn batch_error(
    error: ServiceError,
    completed: Vec<Vec<u8>>,
    blob_owners: Vec<BlobRef>,
    index: usize,
) -> ServiceResponse {
    let status = error.status();
    ServiceResponse {
        status,
        payload: raw_array(completed),
        error: Some(error.into_rpc()),
        batch_index: Some(index as u16),
        completed: Some(index as u16),
        blob_owners,
    }
}

fn raw_array(items: Vec<Vec<u8>>) -> Vec<u8> {
    let mut out = Vec::new();
    rmp::encode::write_array_len(&mut out, items.len() as u32)
        .expect("writing MessagePack to Vec cannot fail");
    for item in items {
        out.extend_from_slice(&item);
    }
    out
}

fn public_method(name: &str) -> bool {
    let mut bytes = name.bytes();
    matches!(bytes.next(), Some(first) if first.is_ascii_alphabetic())
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn batch_request_id(root: &str, index: usize) -> String {
    let suffix = format!(":{index}");
    if root.len() + suffix.len() <= MAX_RPC_REQUEST_ID_BYTES {
        return format!("{root}{suffix}");
    }
    let digest = format!("{:x}", Sha256::digest(root.as_bytes()));
    let prefix_len = MAX_RPC_REQUEST_ID_BYTES - digest.len() - suffix.len() - 1;
    format!("{}~{digest}{suffix}", &root[..prefix_len])
}

#[derive(Deserialize)]
struct OneArg<T> {
    args: Vec<T>,
    kwargs: HashMap<String, serde::de::IgnoredAny>,
}

#[derive(Deserialize)]
struct NoArgs {
    args: Vec<serde::de::IgnoredAny>,
    kwargs: HashMap<String, serde::de::IgnoredAny>,
}

#[derive(Deserialize)]
struct BatchEnvelope {
    calls: Vec<BatchCall>,
}

#[derive(Deserialize)]
struct BatchCall {
    method: String,
    args: MsgpackValue,
    kwargs: MsgpackValue,
}

#[derive(Serialize)]
struct BatchCallPayload {
    args: MsgpackValue,
    kwargs: MsgpackValue,
}

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_bytes::ByteBuf;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Cursor;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

#[cfg(target_os = "linux")]
use sha2::{Digest, Sha256};
#[cfg(target_os = "linux")]
use std::ffi::CString;
#[cfg(target_os = "linux")]
use std::fs::OpenOptions;
#[cfg(target_os = "linux")]
use std::io::Write;
#[cfg(target_os = "linux")]
use std::io::{Seek, SeekFrom};
#[cfg(target_os = "linux")]
use std::os::fd::{AsRawFd, FromRawFd};
#[cfg(target_os = "linux")]
use std::os::unix::fs::{FileExt, MetadataExt, OpenOptionsExt};
#[cfg(target_os = "linux")]
use std::ptr::NonNull;

pub const BLOB_EXT_CODE: i8 = 124;
pub const BLOB_PROTOCOL: u8 = 1;
pub const DEFAULT_MAX_BLOB_BYTES: usize = 256 << 20;
pub const MAX_BLOB_REFS_PER_MESSAGE: usize = 64;
pub const MAX_BLOB_MAPPED_BYTES_PER_MESSAGE: usize = 512 << 20;
pub const MAX_DECODED_BLOB_HANDLES: usize = 128;
pub const MAX_DECODED_BLOB_MAPPINGS: usize = 64;
pub const MAX_DECODED_BLOB_BYTES: usize = 512 << 20;
const MAX_TRACKED_RPC_BLOB_OWNER_SETS: usize = 1_024;
const HEADER_BYTES: usize = 32;
#[cfg(target_os = "linux")]
const HEADER_MAGIC: &[u8; 8] = b"TRBLOB01";
#[cfg(target_os = "linux")]
const REQUIRED_SEALS: i32 =
    libc::F_SEAL_GROW | libc::F_SEAL_SHRINK | libc::F_SEAL_WRITE | libc::F_SEAL_SEAL;

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct BlobDescriptor {
    pub version: u8,
    pub boot: [u8; 32],
    pub owner_pid: u32,
    pub fd: i32,
    pub size: u64,
    pub device: u64,
    pub inode: u64,
    pub token: [u8; 16],
}

#[derive(Debug)]
pub enum BlobError {
    Unsupported(String),
    Invalid(String),
    TooLarge { size: u64, maximum: usize },
    DifferentHost,
    Stale(String),
    Permission(String),
    ResourceLimit(String),
    Io(std::io::Error),
    Codec(String),
    Closed,
}

impl std::fmt::Display for BlobError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unsupported(message)
            | Self::Invalid(message)
            | Self::Stale(message)
            | Self::Permission(message)
            | Self::ResourceLimit(message)
            | Self::Codec(message) => f.write_str(message),
            Self::TooLarge { size, maximum } => {
                write!(f, "BlobRef size {size} exceeds the {maximum}-byte limit")
            }
            Self::DifferentHost => f.write_str("BlobRef belongs to a different Linux boot/host"),
            Self::Io(error) => write!(f, "{error}"),
            Self::Closed => f.write_str("BlobRef is closed"),
        }
    }
}

impl std::error::Error for BlobError {}

impl From<std::io::Error> for BlobError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

pub struct BlobRef {
    inner: Option<Arc<BlobInner>>,
    decoded_handle: Option<DecodedHandle>,
}

struct BlobInner {
    #[cfg(target_os = "linux")]
    file: File,
    mapping: Mapping,
    descriptor: BlobDescriptor,
    _decoded_mapping: Option<DecodedMapping>,
}

#[derive(Clone, Copy, Debug)]
pub struct BlobDecodeLimits {
    pub max_refs: usize,
    pub max_mapped_bytes: usize,
}

impl Default for BlobDecodeLimits {
    fn default() -> Self {
        Self {
            max_refs: MAX_BLOB_REFS_PER_MESSAGE,
            max_mapped_bytes: MAX_BLOB_MAPPED_BYTES_PER_MESSAGE,
        }
    }
}

#[derive(Clone)]
pub(crate) struct BlobDecodeSession {
    state: Arc<Mutex<BlobDecodeState>>,
}

struct BlobDecodeState {
    limits: BlobDecodeLimits,
    refs: usize,
    mapped_bytes: usize,
    seen: HashSet<Vec<u8>>,
}

struct DecodedResources {
    handles: usize,
    mappings: usize,
    mapped_bytes: usize,
    cache: HashMap<BlobDescriptor, Weak<BlobInner>>,
}

struct DecodedHandle;

struct DecodedMapping {
    key: BlobDescriptor,
    bytes: usize,
}

pub struct RpcBlobOwners {
    state: Option<Arc<RpcBlobOwnerState>>,
    slot: usize,
}

struct RpcBlobOwnerState {
    released: AtomicBool,
    owners: std::cell::UnsafeCell<Option<Vec<BlobRef>>>,
}

unsafe impl Send for RpcBlobOwnerState {}
unsafe impl Sync for RpcBlobOwnerState {}

thread_local! {
    static DECODE_SESSION: RefCell<Option<BlobDecodeSession>> = const { RefCell::new(None) };
    static SERIALIZED_BLOBS: RefCell<Option<Vec<BlobRef>>> = const { RefCell::new(None) };
}

fn decoded_resources() -> &'static Mutex<DecodedResources> {
    static RESOURCES: OnceLock<Mutex<DecodedResources>> = OnceLock::new();
    RESOURCES.get_or_init(|| {
        Mutex::new(DecodedResources {
            handles: 0,
            mappings: 0,
            mapped_bytes: 0,
            cache: HashMap::new(),
        })
    })
}

fn rpc_blob_owner_slots() -> &'static [AtomicPtr<RpcBlobOwnerState>] {
    static SLOTS: OnceLock<Box<[AtomicPtr<RpcBlobOwnerState>]>> = OnceLock::new();
    SLOTS.get_or_init(|| {
        (0..MAX_TRACKED_RPC_BLOB_OWNER_SETS)
            .map(|_| AtomicPtr::new(std::ptr::null_mut()))
            .collect::<Vec<_>>()
            .into_boxed_slice()
    })
}

impl RpcBlobOwnerState {
    fn release(&self) {
        if !self.released.swap(true, Ordering::AcqRel) {
            unsafe {
                (*self.owners.get()).take();
            }
        }
    }
}

impl Drop for RpcBlobOwnerState {
    fn drop(&mut self) {
        self.release();
    }
}

impl RpcBlobOwners {
    pub fn track(owners: Vec<BlobRef>) -> Result<Self, BlobError> {
        if owners.is_empty() {
            return Ok(Self {
                state: None,
                slot: usize::MAX,
            });
        }
        let state = Arc::new(RpcBlobOwnerState {
            released: AtomicBool::new(false),
            owners: std::cell::UnsafeCell::new(Some(owners)),
        });
        let raw = Arc::into_raw(state.clone()) as *mut RpcBlobOwnerState;
        for (index, slot) in rpc_blob_owner_slots().iter().enumerate() {
            if slot
                .compare_exchange(
                    std::ptr::null_mut(),
                    raw,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                return Ok(Self {
                    state: Some(state),
                    slot: index,
                });
            }
        }
        unsafe {
            drop(Arc::from_raw(raw));
        }
        Err(BlobError::ResourceLimit(format!(
            "more than {MAX_TRACKED_RPC_BLOB_OWNER_SETS} RPC calls retain BlobRef owners"
        )))
    }
}

impl Drop for RpcBlobOwners {
    fn drop(&mut self) {
        let Some(state) = self.state.take() else {
            return;
        };
        state.release();
        let raw = rpc_blob_owner_slots()[self.slot].swap(std::ptr::null_mut(), Ordering::AcqRel);
        if !raw.is_null() {
            unsafe {
                drop(Arc::from_raw(raw));
            }
        }
    }
}

pub fn clear_inherited_rpc_blob_owners() {
    for slot in rpc_blob_owner_slots() {
        let raw = slot.swap(std::ptr::null_mut(), Ordering::AcqRel);
        if !raw.is_null() {
            let state = unsafe { Arc::from_raw(raw) };
            state.release();
        }
    }
}

impl Drop for DecodedHandle {
    fn drop(&mut self) {
        decoded_resources().lock().unwrap().handles -= 1;
    }
}

impl Drop for DecodedMapping {
    fn drop(&mut self) {
        let mut resources = decoded_resources().lock().unwrap();
        resources.mappings -= 1;
        resources.mapped_bytes -= self.bytes;
        if resources
            .cache
            .get(&self.key)
            .is_some_and(|inner| inner.strong_count() == 0)
        {
            resources.cache.remove(&self.key);
        }
    }
}

impl Clone for BlobRef {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            decoded_handle: None,
        }
    }
}

impl BlobRef {
    pub fn from_bytes(data: &[u8]) -> Result<Self, BlobError> {
        Self::from_bytes_with_limit(data, DEFAULT_MAX_BLOB_BYTES)
    }

    pub fn from_bytes_with_limit(data: &[u8], maximum: usize) -> Result<Self, BlobError> {
        check_size(data.len() as u64, maximum)?;
        #[cfg(not(target_os = "linux"))]
        {
            let _ = data;
            Err(BlobError::Unsupported(
                "BlobRef requires Linux memfd and /proc".into(),
            ))
        }
        #[cfg(target_os = "linux")]
        {
            ensure_proc()?;
            let name = CString::new("tinyray-blob").unwrap();
            let fd = unsafe {
                libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING)
            };
            if fd < 0 {
                let error = std::io::Error::last_os_error();
                return Err(if error.raw_os_error() == Some(libc::ENOSYS) {
                    BlobError::Unsupported("Linux memfd_create is unavailable".into())
                } else {
                    BlobError::Io(error)
                });
            }
            let mut file = unsafe { File::from_raw_fd(fd) };
            if unsafe { libc::fchmod(file.as_raw_fd(), 0o600) } != 0 {
                return Err(BlobError::Permission(
                    std::io::Error::last_os_error().to_string(),
                ));
            }
            let total = HEADER_BYTES
                .checked_add(data.len())
                .ok_or_else(|| BlobError::Invalid("BlobRef size overflow".into()))?;
            file.set_len(total as u64)?;
            let mut token = [0u8; 16];
            fill_token(&mut token)?;
            let mut header = [0u8; HEADER_BYTES];
            header[..8].copy_from_slice(HEADER_MAGIC);
            header[8..24].copy_from_slice(&token);
            header[24..].copy_from_slice(&(data.len() as u64).to_be_bytes());
            file.seek(SeekFrom::Start(0))?;
            file.write_all(&header)?;
            file.write_all(data)?;
            file.flush()?;
            let sealed =
                unsafe { libc::fcntl(file.as_raw_fd(), libc::F_ADD_SEALS, REQUIRED_SEALS) };
            if sealed != 0 {
                return Err(BlobError::Permission(format!(
                    "cannot seal BlobRef memfd: {}",
                    std::io::Error::last_os_error()
                )));
            }
            let metadata = file.metadata()?;
            let descriptor = BlobDescriptor {
                version: BLOB_PROTOCOL,
                boot: boot_fingerprint()?,
                owner_pid: std::process::id(),
                fd: file.as_raw_fd(),
                size: data.len() as u64,
                device: metadata.dev(),
                inode: metadata.ino(),
                token,
            };
            let mapping = Mapping::map(&file, total)?;
            verify_mapping(&mapping, &descriptor)?;
            Ok(Self {
                inner: Some(Arc::new(BlobInner {
                    file,
                    mapping,
                    descriptor,
                    _decoded_mapping: None,
                })),
                decoded_handle: None,
            })
        }
    }

    pub fn from_file(file: &File) -> Result<Self, BlobError> {
        Self::from_file_with_limit(file, DEFAULT_MAX_BLOB_BYTES)
    }

    pub fn from_file_with_limit(file: &File, maximum: usize) -> Result<Self, BlobError> {
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (file, maximum);
            Err(BlobError::Unsupported(
                "BlobRef requires Linux memfd and /proc".into(),
            ))
        }
        #[cfg(target_os = "linux")]
        {
            let size = file.metadata()?.len();
            check_size(size, maximum)?;
            let mut data = vec![0; size as usize];
            read_file_exact_at(file, &mut data)?;
            if file.metadata()?.len() != size {
                return Err(BlobError::Invalid(
                    "source file changed while creating BlobRef".into(),
                ));
            }
            Self::from_bytes_with_limit(&data, maximum)
        }
    }

    pub fn open_descriptor(bytes: &[u8]) -> Result<Self, BlobError> {
        Self::open_descriptor_with_limit(bytes, DEFAULT_MAX_BLOB_BYTES)
    }

    pub fn open_descriptor_with_limit(bytes: &[u8], maximum: usize) -> Result<Self, BlobError> {
        if bytes.len() > 4096 {
            return Err(BlobError::Invalid(
                "BlobRef descriptor exceeds 4096 bytes".into(),
            ));
        }
        parse_descriptor(bytes)?.open_decoded(maximum)
    }

    pub fn descriptor(&self) -> Result<BlobDescriptor, BlobError> {
        let inner = self.inner.as_ref().ok_or(BlobError::Closed)?;
        #[cfg(target_os = "linux")]
        {
            let mut descriptor = inner.descriptor.clone();
            descriptor.owner_pid = std::process::id();
            descriptor.fd = inner.file.as_raw_fd();
            Ok(descriptor)
        }
        #[cfg(not(target_os = "linux"))]
        {
            Ok(inner.descriptor.clone())
        }
    }

    pub fn descriptor_bytes(&self) -> Result<Vec<u8>, BlobError> {
        let descriptor = self.descriptor()?;
        record_serialized_blob(self);
        rmp_serde::to_vec_named(&descriptor).map_err(|error| BlobError::Codec(error.to_string()))
    }

    pub fn len(&self) -> usize {
        self.inner
            .as_ref()
            .map_or(0, |inner| inner.descriptor.size as usize)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_closed(&self) -> bool {
        self.inner.is_none()
    }

    pub fn as_slice(&self) -> Result<&[u8], BlobError> {
        let inner = self.inner.as_ref().ok_or(BlobError::Closed)?;
        Ok(&inner.mapping.as_slice()[HEADER_BYTES..])
    }

    pub fn close(&mut self) {
        self.inner.take();
        self.decoded_handle.take();
    }

    pub fn owner_fd(&self) -> Result<i32, BlobError> {
        #[cfg(target_os = "linux")]
        {
            Ok(self
                .inner
                .as_ref()
                .ok_or(BlobError::Closed)?
                .file
                .as_raw_fd())
        }
        #[cfg(not(target_os = "linux"))]
        {
            Err(BlobError::Unsupported(
                "BlobRef requires Linux memfd and /proc".into(),
            ))
        }
    }

    pub(crate) fn shares_storage(&self, other: &Self) -> bool {
        self.inner
            .as_ref()
            .zip(other.inner.as_ref())
            .is_some_and(|(left, right)| Arc::ptr_eq(left, right))
    }

    pub(crate) fn mapped_len(&self) -> usize {
        self.len().saturating_add(HEADER_BYTES)
    }
}

impl std::fmt::Debug for BlobRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobRef")
            .field("len", &self.len())
            .field("closed", &self.is_closed())
            .finish()
    }
}

impl Serialize for BlobRef {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let bytes = self.descriptor_bytes().map_err(serde::ser::Error::custom)?;
        ExtStruct((BLOB_EXT_CODE, ByteBuf::from(bytes))).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for BlobRef {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let ExtStruct((code, bytes)) = ExtStruct::deserialize(deserializer)?;
        if code != BLOB_EXT_CODE {
            return Err(serde::de::Error::custom(format!(
                "MessagePack extension {code} is not BlobRef"
            )));
        }
        open_for_decode(bytes.as_ref()).map_err(serde::de::Error::custom)
    }
}

#[derive(Deserialize, Serialize)]
#[serde(rename = "_ExtStruct")]
struct ExtStruct((i8, ByteBuf));

impl BlobDescriptor {
    fn open_decoded(self, maximum: usize) -> Result<BlobRef, BlobError> {
        let handle = reserve_decoded_handle()?;
        self.open(maximum, Some(handle))
    }

    fn open(
        self,
        maximum: usize,
        decoded_handle: Option<DecodedHandle>,
    ) -> Result<BlobRef, BlobError> {
        check_size(self.size, maximum)?;
        if self.version != BLOB_PROTOCOL {
            return Err(BlobError::Invalid(format!(
                "unsupported BlobRef protocol {}",
                self.version
            )));
        }
        if self.boot != boot_fingerprint()? {
            return Err(BlobError::DifferentHost);
        }
        if self.owner_pid == 0 || self.fd < 0 {
            return Err(BlobError::Invalid("invalid BlobRef pid/fd".into()));
        }
        ensure_proc()?;
        let path = format!("/proc/{}/fd/{}", self.owner_pid, self.fd);
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (path, decoded_handle);
            Err(BlobError::Unsupported(
                "BlobRef requires Linux memfd and /proc".into(),
            ))
        }
        #[cfg(target_os = "linux")]
        {
            let file = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_CLOEXEC)
                .open(path)
                .map_err(|error| match error.kind() {
                    std::io::ErrorKind::PermissionDenied => {
                        BlobError::Permission(format!("cannot open BlobRef owner fd: {error}"))
                    }
                    _ => BlobError::Stale(format!("cannot open BlobRef owner fd: {error}")),
                })?;
            let metadata = file.metadata()?;
            if metadata.dev() != self.device {
                return Err(BlobError::Stale(
                    "BlobRef fd device does not match the descriptor".into(),
                ));
            }
            if metadata.ino() != self.inode {
                return Err(BlobError::Stale(
                    "BlobRef fd was closed or reused for another object".into(),
                ));
            }
            let total = HEADER_BYTES
                .checked_add(self.size as usize)
                .ok_or_else(|| BlobError::Invalid("BlobRef size overflow".into()))?;
            if metadata.len() != total as u64 {
                return Err(BlobError::Stale(format!(
                    "BlobRef file size is {}, descriptor requires {total}",
                    metadata.len()
                )));
            }
            let seals = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GET_SEALS) };
            if seals < 0 || seals & REQUIRED_SEALS != REQUIRED_SEALS {
                return Err(BlobError::Permission(
                    "BlobRef memfd is not fully sealed read-only".into(),
                ));
            }
            verify_file_header(&file, &self)?;
            let key = self.clone();
            let mut resources = decoded_resources().lock().unwrap();
            resources.cache.retain(|_, inner| inner.strong_count() != 0);
            if let Some(inner) = resources.cache.get(&key).and_then(Weak::upgrade) {
                return Ok(BlobRef {
                    inner: Some(inner),
                    decoded_handle,
                });
            }
            if resources.mappings >= MAX_DECODED_BLOB_MAPPINGS {
                return Err(BlobError::ResourceLimit(format!(
                    "decoded BlobRef mapping count exceeds the process limit of {MAX_DECODED_BLOB_MAPPINGS}"
                )));
            }
            if resources
                .mapped_bytes
                .checked_add(total)
                .is_none_or(|bytes| bytes > MAX_DECODED_BLOB_BYTES)
            {
                return Err(BlobError::ResourceLimit(format!(
                    "decoded BlobRef mappings exceed the process byte limit of {MAX_DECODED_BLOB_BYTES}"
                )));
            }
            let mapping = Mapping::map(&file, total)?;
            verify_mapping(&mapping, &self)?;
            let mut local = self;
            local.owner_pid = std::process::id();
            local.fd = file.as_raw_fd();
            let inner = Arc::new(BlobInner {
                file,
                mapping,
                descriptor: local,
                _decoded_mapping: Some(DecodedMapping {
                    key: key.clone(),
                    bytes: total,
                }),
            });
            resources.mappings += 1;
            resources.mapped_bytes += total;
            resources.cache.insert(key, Arc::downgrade(&inner));
            drop(resources);
            Ok(BlobRef {
                inner: Some(inner),
                decoded_handle,
            })
        }
    }
}

fn check_size(size: u64, maximum: usize) -> Result<(), BlobError> {
    if size > maximum as u64 || size > usize::MAX as u64 {
        return Err(BlobError::TooLarge { size, maximum });
    }
    HEADER_BYTES
        .checked_add(size as usize)
        .ok_or_else(|| BlobError::Invalid("BlobRef size overflow".into()))?;
    Ok(())
}

fn parse_descriptor(bytes: &[u8]) -> Result<BlobDescriptor, BlobError> {
    if bytes.len() > 4096 {
        return Err(BlobError::Invalid(
            "BlobRef descriptor exceeds 4096 bytes".into(),
        ));
    }
    decode_exact(bytes)
        .map_err(|error| BlobError::Codec(format!("malformed BlobRef descriptor: {error}")))
}

fn reserve_decoded_handle() -> Result<DecodedHandle, BlobError> {
    let mut resources = decoded_resources().lock().unwrap();
    if resources.handles >= MAX_DECODED_BLOB_HANDLES {
        return Err(BlobError::ResourceLimit(format!(
            "decoded BlobRef count exceeds the process limit of {MAX_DECODED_BLOB_HANDLES}"
        )));
    }
    resources.handles += 1;
    Ok(DecodedHandle)
}

fn open_for_decode(bytes: &[u8]) -> Result<BlobRef, BlobError> {
    let descriptor = parse_descriptor(bytes)?;
    DECODE_SESSION.with(|slot| {
        let session = slot.borrow().clone();
        let Some(session) = session else {
            return descriptor.open_decoded(DEFAULT_MAX_BLOB_BYTES);
        };
        let mut state = session.state.lock().unwrap();
        state.refs += 1;
        if state.refs > state.limits.max_refs {
            return Err(BlobError::ResourceLimit(format!(
                "MessagePack contains more than {} BlobRef values",
                state.limits.max_refs
            )));
        }
        if state.seen.insert(bytes.to_vec()) {
            let size = usize::try_from(descriptor.size)
                .map_err(|_| BlobError::Invalid("BlobRef size does not fit this process".into()))?;
            let mapped = HEADER_BYTES
                .checked_add(size)
                .ok_or_else(|| BlobError::Invalid("BlobRef size overflow".into()))?;
            state.mapped_bytes = state
                .mapped_bytes
                .checked_add(mapped)
                .ok_or_else(|| BlobError::Invalid("BlobRef aggregate size overflow".into()))?;
            if state.mapped_bytes > state.limits.max_mapped_bytes {
                return Err(BlobError::ResourceLimit(format!(
                    "MessagePack BlobRef mappings exceed the {}-byte aggregate limit",
                    state.limits.max_mapped_bytes
                )));
            }
        }
        drop(state);
        descriptor.open_decoded(DEFAULT_MAX_BLOB_BYTES)
    })
}

pub(crate) fn decode_with_blob_session<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    session: &BlobDecodeSession,
) -> Result<T, rmp_serde::decode::Error> {
    DECODE_SESSION.with(|slot| {
        let previous = slot.replace(Some(session.clone()));
        let decoded = decode_exact(bytes);
        slot.replace(previous);
        decoded
    })
}

pub fn decode_with_blob_limits<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    limits: BlobDecodeLimits,
) -> Result<T, rmp_serde::decode::Error> {
    let session = BlobDecodeSession {
        state: Arc::new(Mutex::new(BlobDecodeState {
            limits,
            refs: 0,
            mapped_bytes: 0,
            seen: HashSet::new(),
        })),
    };
    decode_with_blob_session(bytes, &session)
}

pub fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, rmp_serde::decode::Error> {
    decode_with_blob_limits(bytes, BlobDecodeLimits::default())
}

fn decode_exact<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
) -> Result<T, rmp_serde::decode::Error> {
    let mut decoder = rmp_serde::Deserializer::new(Cursor::new(bytes));
    let value = T::deserialize(&mut decoder)?;
    if decoder.get_ref().position() != bytes.len() as u64 {
        return Err(rmp_serde::decode::Error::Syntax(
            "trailing bytes after MessagePack value".into(),
        ));
    }
    Ok(value)
}

pub(crate) fn blob_decode_session() -> BlobDecodeSession {
    BlobDecodeSession {
        state: Arc::new(Mutex::new(BlobDecodeState {
            limits: BlobDecodeLimits::default(),
            refs: 0,
            mapped_bytes: 0,
            seen: HashSet::new(),
        })),
    }
}

pub(crate) fn encode_with_blob_owners<T: Serialize>(
    value: &T,
) -> Result<(Vec<u8>, Vec<BlobRef>), rmp_serde::encode::Error> {
    SERIALIZED_BLOBS.with(|slot| {
        let previous = slot.replace(Some(Vec::new()));
        let encoded = rmp_serde::to_vec_named(value);
        let owners = slot.replace(previous).unwrap_or_default();
        encoded.map(|encoded| (encoded, owners))
    })
}

fn record_serialized_blob(blob: &BlobRef) {
    SERIALIZED_BLOBS.with(|slot| {
        let mut active = slot.borrow_mut();
        let Some(owners) = active.as_mut() else {
            return;
        };
        if owners.iter().any(|owner| {
            owner
                .inner
                .as_ref()
                .zip(blob.inner.as_ref())
                .is_some_and(|(left, right)| Arc::ptr_eq(left, right))
        }) {
            return;
        }
        owners.push(blob.clone());
    });
}

fn ensure_proc() -> Result<(), BlobError> {
    #[cfg(target_os = "linux")]
    {
        if std::path::Path::new("/proc/self/fd").is_dir() {
            return Ok(());
        }
        Err(BlobError::Unsupported(
            "BlobRef requires a mounted /proc filesystem".into(),
        ))
    }
    #[cfg(not(target_os = "linux"))]
    {
        Err(BlobError::Unsupported(
            "BlobRef requires Linux memfd and /proc".into(),
        ))
    }
}

#[cfg(target_os = "linux")]
fn fill_token(token: &mut [u8; 16]) -> Result<(), BlobError> {
    let mut filled = 0;
    while filled < token.len() {
        let count = unsafe {
            libc::getrandom(token[filled..].as_mut_ptr().cast(), token.len() - filled, 0)
        };
        if count > 0 {
            filled += count as usize;
            continue;
        }
        let error = std::io::Error::last_os_error();
        if error.kind() == std::io::ErrorKind::Interrupted {
            continue;
        }
        return Err(BlobError::Io(std::io::Error::new(
            error.kind(),
            format!("cannot generate BlobRef token with getrandom: {error}"),
        )));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn read_file_exact_at(file: &File, data: &mut [u8]) -> Result<(), BlobError> {
    let mut offset = 0;
    while offset < data.len() {
        let count = positional_read(file, &mut data[offset..], offset as u64)?;
        if count == 0 {
            return Err(BlobError::Invalid(
                "source file changed while creating BlobRef".into(),
            ));
        }
        offset += count;
    }
    let mut extra = [0u8; 1];
    if positional_read(file, &mut extra, data.len() as u64)? != 0 {
        return Err(BlobError::Invalid(
            "source file changed while creating BlobRef".into(),
        ));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn positional_read(file: &File, data: &mut [u8], offset: u64) -> std::io::Result<usize> {
    file.read_at(data, offset)
}

fn boot_fingerprint() -> Result<[u8; 32], BlobError> {
    static BOOT: OnceLock<[u8; 32]> = OnceLock::new();
    if let Some(boot) = BOOT.get() {
        return Ok(*boot);
    }
    #[cfg(target_os = "linux")]
    {
        let boot_id = std::fs::read("/proc/sys/kernel/random/boot_id").map_err(|error| {
            BlobError::Unsupported(format!("cannot read Linux boot identity: {error}"))
        })?;
        let digest: [u8; 32] = Sha256::digest(boot_id).into();
        let _ = BOOT.set(digest);
        Ok(digest)
    }
    #[cfg(not(target_os = "linux"))]
    {
        Err(BlobError::Unsupported(
            "BlobRef requires Linux boot identity".into(),
        ))
    }
}

#[cfg(target_os = "linux")]
fn verify_file_header(file: &File, descriptor: &BlobDescriptor) -> Result<(), BlobError> {
    let mut header = [0u8; HEADER_BYTES];
    file.read_exact_at(&mut header, 0)
        .map_err(|error| BlobError::Stale(format!("cannot read BlobRef header: {error}")))?;
    if !header_matches(&header, descriptor) {
        return Err(BlobError::Stale(
            "BlobRef header/token/size verification failed".into(),
        ));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn verify_mapping(mapping: &Mapping, descriptor: &BlobDescriptor) -> Result<(), BlobError> {
    let bytes = mapping.as_slice();
    if bytes.len() != HEADER_BYTES + descriptor.size as usize
        || !header_matches(&bytes[..HEADER_BYTES], descriptor)
    {
        return Err(BlobError::Stale(
            "BlobRef header/token/size verification failed".into(),
        ));
    }

    Ok(())
}

#[cfg(target_os = "linux")]
fn header_matches(header: &[u8], descriptor: &BlobDescriptor) -> bool {
    &header[..8] == HEADER_MAGIC
        && header[8..24] == descriptor.token
        && header[24..32] == descriptor.size.to_be_bytes()
}

#[cfg(target_os = "linux")]
struct Mapping {
    pointer: NonNull<u8>,
    length: usize,
}

#[cfg(target_os = "linux")]
impl Mapping {
    fn map(file: &File, length: usize) -> Result<Self, BlobError> {
        let pointer = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                length,
                libc::PROT_READ,
                libc::MAP_SHARED,
                file.as_raw_fd(),
                0,
            )
        };
        if pointer == libc::MAP_FAILED {
            let error = std::io::Error::last_os_error();
            return Err(match error.kind() {
                std::io::ErrorKind::PermissionDenied => {
                    BlobError::Permission(format!("cannot map BlobRef read-only: {error}"))
                }
                _ => BlobError::Io(error),
            });
        }
        Ok(Self {
            pointer: NonNull::new(pointer.cast())
                .ok_or_else(|| BlobError::Invalid("mmap returned a null pointer".into()))?,
            length,
        })
    }

    fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.pointer.as_ptr(), self.length) }
    }
}

#[cfg(target_os = "linux")]
unsafe impl Send for Mapping {}
#[cfg(target_os = "linux")]
unsafe impl Sync for Mapping {}

#[cfg(target_os = "linux")]
impl Drop for Mapping {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.pointer.as_ptr().cast(), self.length);
        }
    }
}

#[cfg(not(target_os = "linux"))]
struct Mapping;

#[cfg(not(target_os = "linux"))]
impl Mapping {
    fn as_slice(&self) -> &[u8] {
        &[]
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    #[test]
    fn positional_file_read_detects_a_truncated_snapshot() {
        let path = std::env::temp_dir().join(format!(
            "tinyray-truncated-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        std::fs::write(&path, b"12345678").unwrap();
        let file = File::open(&path).unwrap();
        let expected = file.metadata().unwrap().len() as usize;
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(4)
            .unwrap();
        let mut data = vec![0; expected];
        assert!(matches!(
            read_file_exact_at(&file, &mut data),
            Err(BlobError::Invalid(message)) if message.contains("changed")
        ));
        std::fs::write(&path, b"1234").unwrap();
        let file = File::open(&path).unwrap();
        let mut data = vec![0; 4];
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"5")
            .unwrap();
        assert!(matches!(
            read_file_exact_at(&file, &mut data),
            Err(BlobError::Invalid(message)) if message.contains("changed")
        ));
        std::fs::remove_file(path).unwrap();
    }
}

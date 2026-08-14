//! Same-node fast path: hand a result over shared memory instead of a socket.
//!
//! Two actors on one machine copying 10 MB through the loopback interface is
//! pure waste. When producer and consumer share a node, the producer writes the
//! payload into `/dev/shm` and the consumer maps it: no socket, no kernel
//! network stack, no copy.
//!
//! This is not an object store. There is no directory, no eviction policy, and
//! no lifetime beyond the producing actor's: a segment is created for one
//! result and unlinked when that result is released. Everything else about the
//! design -- ownership, references, fetching -- is unchanged.

use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use bytes::Bytes;

/// Directory shared-memory segments live in.
pub const SHM_DIR: &str = "/dev/shm/tinyray";

/// Below this size, a socket round trip beats the syscalls to create, map and
/// unlink a segment.
pub const MIN_SHM_BYTES: usize = 256 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum ShmError {
    #[error("shared memory is unavailable at {path}: {source}")]
    Unavailable {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write shared segment {name}: {source}")]
    Write {
        name: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to read shared segment {name}: {source}")]
    Read {
        name: String,
        #[source]
        source: std::io::Error,
    },
    #[error("shared segment {name} has size {actual}, expected {expected}")]
    SizeMismatch {
        name: String,
        actual: u64,
        expected: u64,
    },
}

/// Where a payload lives in shared memory, plus the frame sizes needed to split
/// it back up.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShmHandle {
    pub name: String,
    pub total_len: u64,
    pub frame_sizes: Vec<u32>,
}

impl ShmHandle {
    pub fn path(&self) -> PathBuf {
        Path::new(SHM_DIR).join(&self.name)
    }
}

/// Whether shared memory can be used at all on this machine.
pub fn is_available() -> bool {
    std::fs::create_dir_all(SHM_DIR).is_ok()
}

/// Whether a payload of this size is worth putting in shared memory.
pub fn is_worthwhile(total_len: usize) -> bool {
    total_len >= MIN_SHM_BYTES
}

/// Write frames into a fresh segment.
pub fn publish(name: &str, frames: &[Bytes]) -> Result<ShmHandle, ShmError> {
    std::fs::create_dir_all(SHM_DIR).map_err(|source| ShmError::Unavailable {
        path: SHM_DIR.to_string(),
        source,
    })?;
    let path = Path::new(SHM_DIR).join(name);

    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)
        .map_err(|source| ShmError::Write {
            name: name.to_string(),
            source,
        })?;

    let mut total_len = 0u64;
    let mut frame_sizes = Vec::with_capacity(frames.len());
    for frame in frames {
        file.write_all(frame).map_err(|source| ShmError::Write {
            name: name.to_string(),
            source,
        })?;
        total_len += frame.len() as u64;
        frame_sizes.push(frame.len() as u32);
    }
    file.flush().map_err(|source| ShmError::Write {
        name: name.to_string(),
        source,
    })?;

    Ok(ShmHandle {
        name: name.to_string(),
        total_len,
        frame_sizes,
    })
}

/// Read a segment back into frames.
///
/// Returns owned `Bytes`; the mapping-based zero-copy version needs `memmap2`,
/// and this keeps the dependency surface small until the benchmark says the
/// copy actually matters.
pub fn consume(handle: &ShmHandle) -> Result<Vec<Bytes>, ShmError> {
    let path = handle.path();
    let mut file = File::open(&path).map_err(|source| ShmError::Read {
        name: handle.name.clone(),
        source,
    })?;
    let actual = file
        .seek(SeekFrom::End(0))
        .map_err(|source| ShmError::Read {
            name: handle.name.clone(),
            source,
        })?;
    if actual != handle.total_len {
        // A truncated or recycled segment must be caught here rather than
        // silently deserialised into nonsense.
        return Err(ShmError::SizeMismatch {
            name: handle.name.clone(),
            actual,
            expected: handle.total_len,
        });
    }
    file.seek(SeekFrom::Start(0))
        .map_err(|source| ShmError::Read {
            name: handle.name.clone(),
            source,
        })?;

    let data = std::fs::read(&path).map_err(|source| ShmError::Read {
        name: handle.name.clone(),
        source,
    })?;
    let data = Bytes::from(data);

    let mut frames = Vec::with_capacity(handle.frame_sizes.len());
    let mut offset = 0usize;
    for size in &handle.frame_sizes {
        let size = *size as usize;
        frames.push(data.slice(offset..offset + size));
        offset += size;
    }
    Ok(frames)
}

/// Delete a segment. Best effort: the owner may already be gone.
pub fn release(name: &str) -> bool {
    std::fs::remove_file(Path::new(SHM_DIR).join(name)).is_ok()
}

/// Remove every segment this process left behind.
///
/// Called at actor shutdown. `/dev/shm` is RAM, so a leak here consumes memory
/// until the machine reboots.
pub fn release_all(prefix: &str) -> usize {
    let Ok(entries) = std::fs::read_dir(SHM_DIR) else {
        return 0;
    };
    let mut removed = 0;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(prefix) && std::fs::remove_file(entry.path()).is_ok() {
            removed += 1;
        }
    }
    removed
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unique(tag: &str) -> String {
        format!("test-{tag}-{}", tinyray_core::TaskId::generate())
    }

    #[test]
    fn publish_and_consume_round_trip() {
        if !is_available() {
            return;
        }
        let name = unique("roundtrip");
        let frames = vec![
            Bytes::from(vec![1u8; 1024]),
            Bytes::from(vec![2u8; 2048]),
            Bytes::from_static(b"tail"),
        ];
        let handle = publish(&name, &frames).expect("publish");
        assert_eq!(handle.total_len, 1024 + 2048 + 4);
        assert_eq!(handle.frame_sizes, vec![1024, 2048, 4]);

        let restored = consume(&handle).expect("consume");
        assert_eq!(restored, frames);
        assert!(release(&name));
    }

    #[test]
    fn empty_frames_survive() {
        if !is_available() {
            return;
        }
        let name = unique("empty");
        let frames = vec![Bytes::new(), Bytes::from_static(b"x"), Bytes::new()];
        let handle = publish(&name, &frames).expect("publish");
        assert_eq!(consume(&handle).expect("consume"), frames);
        release(&name);
    }

    #[test]
    fn publishing_the_same_name_twice_is_refused() {
        // Silently overwriting would corrupt a result another actor is midway
        // through reading.
        if !is_available() {
            return;
        }
        let name = unique("collision");
        publish(&name, &[Bytes::from_static(b"first")]).expect("publish");
        assert!(publish(&name, &[Bytes::from_static(b"second")]).is_err());
        release(&name);
    }

    #[test]
    fn a_truncated_segment_is_detected() {
        if !is_available() {
            return;
        }
        let name = unique("truncated");
        let handle = publish(&name, &[Bytes::from(vec![7u8; 4096])]).expect("publish");
        // Claim it is larger than it is, as a stale handle would.
        let lying = ShmHandle {
            total_len: handle.total_len + 1,
            ..handle.clone()
        };
        assert!(matches!(
            consume(&lying),
            Err(ShmError::SizeMismatch { .. })
        ));
        release(&name);
    }

    #[test]
    fn consuming_a_released_segment_is_an_error() {
        if !is_available() {
            return;
        }
        let name = unique("released");
        let handle = publish(&name, &[Bytes::from_static(b"data")]).expect("publish");
        assert!(release(&name));
        assert!(matches!(consume(&handle), Err(ShmError::Read { .. })));
        assert!(!release(&name), "releasing twice is a no-op");
    }

    #[test]
    fn release_all_cleans_up_a_prefix() {
        // /dev/shm is RAM: a leak here survives the process and eats memory
        // until reboot.
        if !is_available() {
            return;
        }
        let prefix = format!("sweep-{}", tinyray_core::TaskId::generate());
        for index in 0..3 {
            publish(&format!("{prefix}-{index}"), &[Bytes::from_static(b"x")]).expect("publish");
        }
        assert_eq!(release_all(&prefix), 3);
        assert_eq!(release_all(&prefix), 0);
    }

    #[test]
    fn small_payloads_are_not_worth_sharing() {
        // Below the threshold the syscalls cost more than the loopback copy.
        assert!(!is_worthwhile(1024));
        assert!(!is_worthwhile(MIN_SHM_BYTES - 1));
        assert!(is_worthwhile(MIN_SHM_BYTES));
        assert!(is_worthwhile(10 * 1024 * 1024));
    }
}

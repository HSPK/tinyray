//! Fork-safe descriptor tracking without allocations in the at-fork path.

#[cfg(unix)]
use std::sync::atomic::{AtomicI32, Ordering};

#[cfg(unix)]
pub type RawFd = std::os::fd::RawFd;
#[cfg(not(unix))]
pub type RawFd = i64;

#[cfg(unix)]
const TRACKED_FDS: usize = 16_384;

pub struct FdTable {
    #[cfg(unix)]
    slots: Box<[AtomicI32]>,
}

impl Default for FdTable {
    fn default() -> Self {
        Self::new()
    }
}

impl FdTable {
    pub fn new() -> Self {
        Self {
            #[cfg(unix)]
            slots: (0..TRACKED_FDS)
                .map(|_| AtomicI32::new(-1))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }

    #[cfg(unix)]
    pub fn register(&self, fd: RawFd) {
        for slot in &self.slots {
            if slot
                .compare_exchange(-1, fd, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return;
            }
        }
    }

    #[cfg(not(unix))]
    pub fn register(&self, _fd: RawFd) {}

    #[cfg(unix)]
    pub fn unregister(&self, fd: RawFd) {
        for slot in &self.slots {
            if slot
                .compare_exchange(fd, -1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return;
            }
        }
    }

    #[cfg(not(unix))]
    pub fn unregister(&self, _fd: RawFd) {}

    #[cfg(unix)]
    pub fn close_all(&self) {
        for slot in &self.slots {
            let fd = slot.swap(-1, Ordering::AcqRel);
            if fd >= 0 {
                unsafe {
                    libc::close(fd);
                }
            }
        }
    }

    #[cfg(not(unix))]
    pub fn close_all(&self) {}

    #[cfg(unix)]
    pub fn snapshot(&self) -> Vec<i32> {
        self.slots
            .iter()
            .filter_map(|slot| {
                let fd = slot.load(Ordering::Acquire);
                (fd >= 0).then_some(fd)
            })
            .collect()
    }

    #[cfg(not(unix))]
    pub fn snapshot(&self) -> Vec<i32> {
        Vec::new()
    }
}

//! 128-bit identifiers for actors, tasks, callers and nodes.
//!
//! Ids are process-unique by construction: the high half is derived once per
//! process from the pid and wall clock, the low half is a monotonic counter.
//! That is enough for a cluster of the size tinyray targets, and it avoids
//! pulling in a uuid/rand dependency on a hot path.

use std::fmt;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

/// A 128-bit opaque identifier, displayed as 32 lowercase hex characters.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Id {
    pub hi: u64,
    pub lo: u64,
}

impl Id {
    pub const NIL: Id = Id { hi: 0, lo: 0 };

    pub const fn new(hi: u64, lo: u64) -> Id {
        Id { hi, lo }
    }

    /// Allocate a fresh, process-unique identifier.
    pub fn generate() -> Id {
        Id {
            hi: process_seed(),
            lo: COUNTER.fetch_add(1, Ordering::Relaxed),
        }
    }

    pub fn is_nil(&self) -> bool {
        self.hi == 0 && self.lo == 0
    }

    pub fn to_hex(self) -> String {
        format!("{:016x}{:016x}", self.hi, self.lo)
    }
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}{:016x}", self.hi, self.lo)
    }
}

impl fmt::Debug for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Id({self})")
    }
}

/// Error returned when parsing an [`Id`] from its hex representation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseIdError(pub &'static str);

impl fmt::Display for ParseIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid id: {}", self.0)
    }
}

impl std::error::Error for ParseIdError {}

impl FromStr for Id {
    type Err = ParseIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() != 32 {
            return Err(ParseIdError("expected 32 hex characters"));
        }
        // `from_str_radix` accepts a leading `+`, which would make the textual
        // form non-canonical (two spellings for one id). Reject anything that
        // is not a plain lowercase-or-uppercase hex digit up front.
        if !s.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(ParseIdError("expected 32 hex characters"));
        }
        let hi =
            u64::from_str_radix(&s[..16], 16).map_err(|_| ParseIdError("bad hex in high half"))?;
        let lo =
            u64::from_str_radix(&s[16..], 16).map_err(|_| ParseIdError("bad hex in low half"))?;
        Ok(Id { hi, lo })
    }
}

static COUNTER: AtomicU64 = AtomicU64::new(1);

fn process_seed() -> u64 {
    use std::sync::OnceLock;
    static SEED: OnceLock<u64> = OnceLock::new();
    *SEED.get_or_init(|| {
        let pid = std::process::id() as u64;
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        // splitmix64 finalizer: cheap, and good enough to keep concurrently
        // started processes from colliding on adjacent pids/timestamps.
        let mut z = pid.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(nanos);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        let z = z ^ (z >> 31);
        // Never hand out the nil id as a process seed.
        if z == 0 {
            0x5445_5354_5345_4544
        } else {
            z
        }
    })
}

/// Declare a newtype wrapper around [`Id`] with the same ergonomics.
macro_rules! id_newtype {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub Id);

        impl $name {
            pub const NIL: $name = $name(Id::NIL);

            pub fn generate() -> $name {
                $name(Id::generate())
            }

            pub const fn from_parts(hi: u64, lo: u64) -> $name {
                $name(Id::new(hi, lo))
            }

            pub fn is_nil(&self) -> bool {
                self.0.is_nil()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&self.0, f)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!(stringify!($name), "({})"), self.0)
            }
        }

        impl FromStr for $name {
            type Err = ParseIdError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Ok($name(Id::from_str(s)?))
            }
        }
    };
}

id_newtype!(
    /// Identifies an actor instance for its whole lifetime, across restarts.
    ActorId
);
id_newtype!(
    /// Identifies a single method invocation, and therefore its result object.
    TaskId
);
id_newtype!(
    /// Identifies the originator of a call, used for per-caller ordering.
    CallerId
);
id_newtype!(
    /// Identifies a node (one machine) in the cluster.
    NodeId
);

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn hex_roundtrip() {
        let id = Id::new(0x0123_4567_89ab_cdef, 0xfedc_ba98_7654_3210);
        assert_eq!(id.to_string(), "0123456789abcdeffedcba9876543210");
        assert_eq!(Id::from_str(&id.to_string()).unwrap(), id);
    }

    #[test]
    fn nil_formats_as_zeros() {
        assert_eq!(Id::NIL.to_string(), "0".repeat(32));
        assert!(Id::NIL.is_nil());
        assert!(!Id::generate().is_nil());
    }

    #[test]
    fn parse_rejects_bad_input() {
        assert!(Id::from_str("").is_err());
        assert!(Id::from_str("abc").is_err());
        // 31 and 33 characters.
        assert!(Id::from_str(&"a".repeat(31)).is_err());
        assert!(Id::from_str(&"a".repeat(33)).is_err());
        // Right length, wrong alphabet.
        assert!(Id::from_str(&"g".repeat(32)).is_err());
        // A '+' would be accepted by a naive from_str_radix call.
        assert!(Id::from_str(&format!("+{}", "0".repeat(31))).is_err());
        // Same for whitespace and underscores, which some parsers tolerate.
        assert!(Id::from_str(&format!(" {}", "0".repeat(31))).is_err());
        assert!(Id::from_str(&format!("{}_", "0".repeat(31))).is_err());
        // Uppercase hex parses, but Display always emits lowercase.
        let upper = "A".repeat(32);
        assert_eq!(Id::from_str(&upper).unwrap().to_string(), "a".repeat(32));
    }

    #[test]
    fn generated_ids_are_unique() {
        let ids: HashSet<Id> = (0..10_000).map(|_| Id::generate()).collect();
        assert_eq!(ids.len(), 10_000);
    }

    #[test]
    fn generated_ids_share_a_process_seed() {
        let a = Id::generate();
        let b = Id::generate();
        assert_eq!(a.hi, b.hi, "same process must share the high half");
        assert_ne!(a.lo, b.lo);
        assert_ne!(a.hi, 0, "process seed must never be zero");
    }

    #[test]
    fn generated_ids_are_unique_across_threads() {
        let handles: Vec<_> = (0..8)
            .map(|_| std::thread::spawn(|| (0..2_000).map(|_| Id::generate()).collect::<Vec<_>>()))
            .collect();
        let all: HashSet<Id> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();
        assert_eq!(all.len(), 16_000);
    }

    #[test]
    fn newtypes_are_distinct_but_roundtrip() {
        let actor = ActorId::generate();
        let parsed = ActorId::from_str(&actor.to_string()).unwrap();
        assert_eq!(actor, parsed);
        assert_eq!(format!("{actor:?}"), format!("ActorId({actor})"));
    }

    #[test]
    fn newtypes_serialize_transparently() {
        let task = TaskId::from_parts(1, 2);
        let bytes = rmp_serde::to_vec(&task).unwrap();
        let inner = rmp_serde::to_vec(&Id::new(1, 2)).unwrap();
        assert_eq!(bytes, inner, "newtype must not add a wrapper layer");
        let back: TaskId = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(back, task);
    }
}

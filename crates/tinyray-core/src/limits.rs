//! Size limits applied while decoding untrusted byte streams.

/// Guard rails for the framing decoder.
///
/// The decoder allocates buffers based on values read off the wire, so every
/// one of these fields exists to stop a malformed or hostile peer from causing
/// an unbounded allocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Maximum size of the msgpack header.
    pub max_header_len: u64,
    /// Maximum number of out-of-band frames in one message.
    pub max_frames: u64,
    /// Maximum size of a single frame.
    pub max_frame_len: u64,
    /// Maximum size of header plus all frames combined.
    pub max_message_len: u64,
}

impl Limits {
    /// Limits used unless the caller overrides them.
    ///
    /// The defaults are sized for the target workload from the design doc:
    /// ~10 MB payloads, with plenty of headroom for occasional large results.
    pub const DEFAULT: Limits = Limits {
        max_header_len: 1 << 20, // 1 MiB
        max_frames: 4096,
        max_frame_len: 4 << 30,   // 4 GiB
        max_message_len: 8 << 30, // 8 GiB
    };

    /// Small limits, primarily useful in tests.
    pub const fn tiny() -> Limits {
        Limits {
            max_header_len: 1024,
            max_frames: 8,
            max_frame_len: 4096,
            max_message_len: 16 * 1024,
        }
    }
}

impl Default for Limits {
    fn default() -> Self {
        Limits::DEFAULT
    }
}

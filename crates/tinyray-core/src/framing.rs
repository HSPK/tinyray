//! The tinyray wire framing.
//!
//! ```text
//! offset 0        magic         b"TRY1"        (4 bytes)
//! offset 4        header_len    u32 big endian (4 bytes)
//! offset 8        n_frames      u32 big endian (4 bytes)
//! offset 12       frame_sizes   u32 big endian * n_frames
//! offset 12+4n    header        msgpack, header_len bytes
//! then            frames        concatenated, sizes as declared above
//! ```
//!
//! Frame sizes live in the fixed prefix rather than inside the msgpack header
//! so that the framing layer never needs to parse msgpack. That keeps this
//! module purely mechanical, and it means a corrupt header cannot desynchronise
//! the byte stream.
//!
//! The point of the out-of-band frames is zero copy: pickle protocol 5 hands us
//! the large tensor buffers separately from the small pickle body, and they
//! travel all the way to the socket without being concatenated or copied.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::FrameError;
use crate::limits::Limits;

/// Magic bytes at the start of every message.
pub const MAGIC: [u8; 4] = *b"TRY1";

/// Length of the fixed portion of the prefix (magic + header_len + n_frames).
pub const FIXED_PREFIX_LEN: usize = 12;

/// A decoded (or to-be-encoded) message: one header plus N out-of-band frames.
#[derive(Clone, PartialEq, Eq, Default)]
pub struct Message {
    /// The msgpack-encoded protocol header.
    pub header: Bytes,
    /// Out-of-band buffers, e.g. pickle protocol 5 tensor storage.
    pub frames: Vec<Bytes>,
}

impl Message {
    pub fn new(header: impl Into<Bytes>, frames: Vec<Bytes>) -> Message {
        Message {
            header: header.into(),
            frames,
        }
    }

    /// A message with a header and no out-of-band frames.
    pub fn header_only(header: impl Into<Bytes>) -> Message {
        Message {
            header: header.into(),
            frames: Vec::new(),
        }
    }

    /// Total number of bytes this message occupies on the wire.
    pub fn encoded_len(&self) -> usize {
        FIXED_PREFIX_LEN
            + 4 * self.frames.len()
            + self.header.len()
            + self.frames.iter().map(|f| f.len()).sum::<usize>()
    }

    /// Total payload bytes, excluding framing overhead.
    pub fn payload_len(&self) -> usize {
        self.header.len() + self.frames.iter().map(|f| f.len()).sum::<usize>()
    }

    /// Check this message against `limits` without encoding it.
    pub fn validate(&self, limits: &Limits) -> Result<(), FrameError> {
        if self.header.len() as u64 > limits.max_header_len {
            return Err(FrameError::HeaderTooLarge {
                len: self.header.len() as u64,
                max: limits.max_header_len,
            });
        }
        if self.frames.len() as u64 > limits.max_frames {
            return Err(FrameError::TooManyFrames {
                n: self.frames.len() as u64,
                max: limits.max_frames,
            });
        }
        let mut total = self.header.len() as u64;
        for (index, frame) in self.frames.iter().enumerate() {
            let len = frame.len() as u64;
            if len > limits.max_frame_len {
                return Err(FrameError::FrameTooLarge {
                    index,
                    len,
                    max: limits.max_frame_len,
                });
            }
            if len > u32::MAX as u64 {
                return Err(FrameError::FrameTooLarge {
                    index,
                    len,
                    max: u32::MAX as u64,
                });
            }
            total += len;
        }
        if total > limits.max_message_len {
            return Err(FrameError::MessageTooLarge {
                len: total,
                max: limits.max_message_len,
            });
        }
        Ok(())
    }

    /// Encode into a vector of chunks suitable for vectored IO.
    ///
    /// The header and every frame are handed back as-is, so no payload bytes are
    /// copied. Only the small prefix is freshly allocated.
    pub fn encode_chunks(&self, limits: &Limits) -> Result<Vec<Bytes>, FrameError> {
        self.validate(limits)?;
        let mut chunks = Vec::with_capacity(2 + self.frames.len());
        chunks.push(self.encode_prefix());
        chunks.push(self.header.clone());
        chunks.extend(self.frames.iter().cloned());
        Ok(chunks)
    }

    /// Encode into a single contiguous buffer. Convenient, but it copies; use
    /// [`Message::encode_chunks`] on hot paths.
    pub fn encode_to_vec(&self, limits: &Limits) -> Result<Vec<u8>, FrameError> {
        let chunks = self.encode_chunks(limits)?;
        let mut out = Vec::with_capacity(self.encoded_len());
        for chunk in chunks {
            out.extend_from_slice(&chunk);
        }
        Ok(out)
    }

    fn encode_prefix(&self) -> Bytes {
        let mut prefix = BytesMut::with_capacity(FIXED_PREFIX_LEN + 4 * self.frames.len());
        prefix.put_slice(&MAGIC);
        prefix.put_u32(self.header.len() as u32);
        prefix.put_u32(self.frames.len() as u32);
        for frame in &self.frames {
            prefix.put_u32(frame.len() as u32);
        }
        prefix.freeze()
    }
}

impl std::fmt::Debug for Message {
    /// Deliberately prints sizes rather than contents: these payloads are
    /// routinely tens of megabytes.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Message")
            .field("header_len", &self.header.len())
            .field("frames", &self.frames.len())
            .field(
                "frame_sizes",
                &self.frames.iter().map(|f| f.len()).collect::<Vec<_>>(),
            )
            .finish()
    }
}

#[derive(Debug)]
enum Stage {
    Fixed,
    Sizes {
        header_len: u32,
        n_frames: u32,
    },
    Header {
        header_len: u32,
        sizes: Vec<u32>,
    },
    Frames {
        header: Bytes,
        sizes: Vec<u32>,
        next: usize,
        out: Vec<Bytes>,
    },
}

/// Incremental, allocation-aware decoder for the tinyray framing.
///
/// Feed it a [`BytesMut`] that grows as bytes arrive from the socket; it
/// consumes exactly what it needs and yields whole messages. Frames are split
/// off the input buffer, so decoded payloads share the read buffer's allocation
/// instead of being copied.
#[derive(Debug)]
pub struct Decoder {
    limits: Limits,
    stage: Stage,
    poisoned: bool,
}

impl Default for Decoder {
    fn default() -> Self {
        Decoder::new(Limits::DEFAULT)
    }
}

impl Decoder {
    pub fn new(limits: Limits) -> Decoder {
        Decoder {
            limits,
            stage: Stage::Fixed,
            poisoned: false,
        }
    }

    pub fn limits(&self) -> &Limits {
        &self.limits
    }

    /// True once a fatal framing error has been reported. The connection must
    /// be torn down; there is no resynchronisation point in a binary framing.
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// True when no bytes of a message have been consumed yet, i.e. it is safe
    /// to stop reading here.
    pub fn is_at_message_boundary(&self) -> bool {
        matches!(self.stage, Stage::Fixed)
    }

    /// Bytes still required to finish the current stage. Useful for sizing
    /// buffer reservations; not a promise about the whole message.
    pub fn needed(&self) -> usize {
        match &self.stage {
            Stage::Fixed => FIXED_PREFIX_LEN,
            Stage::Sizes { n_frames, .. } => 4 * (*n_frames as usize),
            Stage::Header { header_len, .. } => *header_len as usize,
            Stage::Frames { sizes, next, .. } => sizes[*next] as usize,
        }
    }

    /// Try to decode one message, consuming from `buf`.
    ///
    /// Returns `Ok(None)` when more bytes are needed. Any `Err` is fatal and
    /// poisons the decoder.
    pub fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Message>, FrameError> {
        if self.poisoned {
            return Err(FrameError::Poisoned);
        }
        match self.decode_inner(buf) {
            Err(err) => {
                self.poisoned = true;
                Err(err)
            }
            ok => ok,
        }
    }

    fn decode_inner(&mut self, buf: &mut BytesMut) -> Result<Option<Message>, FrameError> {
        loop {
            match &mut self.stage {
                Stage::Fixed => {
                    if buf.len() < FIXED_PREFIX_LEN {
                        return Ok(None);
                    }
                    let mut prefix = buf.split_to(FIXED_PREFIX_LEN);
                    let mut magic = [0u8; 4];
                    prefix.copy_to_slice(&mut magic);
                    if magic != MAGIC {
                        return Err(FrameError::BadMagic {
                            expected: MAGIC,
                            found: magic,
                        });
                    }
                    let header_len = prefix.get_u32();
                    let n_frames = prefix.get_u32();

                    if header_len as u64 > self.limits.max_header_len {
                        return Err(FrameError::HeaderTooLarge {
                            len: header_len as u64,
                            max: self.limits.max_header_len,
                        });
                    }
                    if n_frames as u64 > self.limits.max_frames {
                        return Err(FrameError::TooManyFrames {
                            n: n_frames as u64,
                            max: self.limits.max_frames,
                        });
                    }
                    self.stage = Stage::Sizes {
                        header_len,
                        n_frames,
                    };
                }

                Stage::Sizes {
                    header_len,
                    n_frames,
                } => {
                    let want = 4 * (*n_frames as usize);
                    if buf.len() < want {
                        return Ok(None);
                    }
                    let header_len = *header_len;
                    let n_frames = *n_frames as usize;
                    let mut raw = buf.split_to(want);
                    let mut sizes = Vec::with_capacity(n_frames);
                    let mut total = header_len as u64;
                    for index in 0..n_frames {
                        let size = raw.get_u32();
                        if size as u64 > self.limits.max_frame_len {
                            return Err(FrameError::FrameTooLarge {
                                index,
                                len: size as u64,
                                max: self.limits.max_frame_len,
                            });
                        }
                        total += size as u64;
                        if total > self.limits.max_message_len {
                            return Err(FrameError::MessageTooLarge {
                                len: total,
                                max: self.limits.max_message_len,
                            });
                        }
                        sizes.push(size);
                    }
                    self.stage = Stage::Header { header_len, sizes };
                }

                Stage::Header { header_len, sizes } => {
                    let want = *header_len as usize;
                    if buf.len() < want {
                        return Ok(None);
                    }
                    let header = buf.split_to(want).freeze();
                    let sizes = std::mem::take(sizes);
                    let out = Vec::with_capacity(sizes.len());
                    self.stage = Stage::Frames {
                        header,
                        sizes,
                        next: 0,
                        out,
                    };
                }

                Stage::Frames {
                    header,
                    sizes,
                    next,
                    out,
                } => {
                    if *next == sizes.len() {
                        let message = Message {
                            header: header.clone(),
                            frames: std::mem::take(out),
                        };
                        self.stage = Stage::Fixed;
                        return Ok(Some(message));
                    }
                    let want = sizes[*next] as usize;
                    if buf.len() < want {
                        return Ok(None);
                    }
                    out.push(buf.split_to(want).freeze());
                    *next += 1;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(header: &[u8], frames: &[&[u8]]) -> Message {
        Message::new(
            Bytes::copy_from_slice(header),
            frames.iter().map(|f| Bytes::copy_from_slice(f)).collect(),
        )
    }

    fn roundtrip(message: &Message, limits: &Limits) -> Message {
        let encoded = message.encode_to_vec(limits).expect("encode");
        let mut buf = BytesMut::from(&encoded[..]);
        let mut decoder = Decoder::new(*limits);
        let decoded = decoder
            .decode(&mut buf)
            .expect("decode")
            .expect("complete message");
        assert!(buf.is_empty(), "decoder must consume the whole message");
        assert!(decoder.is_at_message_boundary());
        decoded
    }

    #[test]
    fn roundtrip_header_only() {
        let m = msg(b"hello", &[]);
        assert_eq!(roundtrip(&m, &Limits::DEFAULT), m);
    }

    #[test]
    fn roundtrip_with_frames() {
        let m = msg(b"hdr", &[b"aaaa", b"bb", b"cccccc"]);
        assert_eq!(roundtrip(&m, &Limits::DEFAULT), m);
    }

    #[test]
    fn roundtrip_empty_header_and_empty_frames() {
        // Both an empty header and zero-length frames are legal and must
        // survive the round trip with their count intact.
        let m = msg(b"", &[b"", b"x", b""]);
        let decoded = roundtrip(&m, &Limits::DEFAULT);
        assert_eq!(decoded, m);
        assert_eq!(decoded.frames.len(), 3);
        assert_eq!(decoded.frames[0].len(), 0);
        assert_eq!(decoded.frames[2].len(), 0);
    }

    #[test]
    fn encoded_len_matches_reality() {
        let m = msg(b"hdr", &[b"aaaa", b"bb"]);
        let encoded = m.encode_to_vec(&Limits::DEFAULT).unwrap();
        assert_eq!(m.encoded_len(), encoded.len());
        assert_eq!(m.payload_len(), 3 + 4 + 2);
    }

    #[test]
    fn wire_layout_is_exactly_as_documented() {
        let m = msg(b"AB", &[b"xyz"]);
        let encoded = m.encode_to_vec(&Limits::DEFAULT).unwrap();
        assert_eq!(&encoded[0..4], b"TRY1");
        assert_eq!(&encoded[4..8], &2u32.to_be_bytes()); // header_len
        assert_eq!(&encoded[8..12], &1u32.to_be_bytes()); // n_frames
        assert_eq!(&encoded[12..16], &3u32.to_be_bytes()); // frame 0 size
        assert_eq!(&encoded[16..18], b"AB");
        assert_eq!(&encoded[18..21], b"xyz");
        assert_eq!(encoded.len(), 21);
    }

    #[test]
    fn encode_chunks_does_not_copy_payload() {
        let header = Bytes::from_static(b"hdr");
        let frame = Bytes::from_static(b"payload");
        let m = Message::new(header.clone(), vec![frame.clone()]);
        let chunks = m.encode_chunks(&Limits::DEFAULT).unwrap();
        assert_eq!(chunks.len(), 3);
        // Same backing allocation, not a copy.
        assert_eq!(chunks[1].as_ptr(), header.as_ptr());
        assert_eq!(chunks[2].as_ptr(), frame.as_ptr());
    }

    #[test]
    fn byte_at_a_time_feeding() {
        // The decoder must never assume a message arrives in one read.
        let m = msg(b"header-bytes", &[b"frame-one", b"frame-two"]);
        let encoded = m.encode_to_vec(&Limits::DEFAULT).unwrap();

        let mut decoder = Decoder::default();
        let mut buf = BytesMut::new();
        for (i, byte) in encoded.iter().enumerate() {
            buf.put_u8(*byte);
            let is_last = i + 1 == encoded.len();
            match decoder.decode(&mut buf).unwrap() {
                Some(decoded) => {
                    assert!(is_last, "message completed early at byte {i}");
                    assert_eq!(decoded, m);
                }
                None => assert!(!is_last, "message did not complete on the final byte"),
            }
        }
    }

    #[test]
    fn several_messages_in_one_buffer() {
        let a = msg(b"first", &[b"1"]);
        let b = msg(b"second", &[]);
        let c = msg(b"third", &[b"xx", b"yyy"]);

        let mut buf = BytesMut::new();
        for m in [&a, &b, &c] {
            buf.extend_from_slice(&m.encode_to_vec(&Limits::DEFAULT).unwrap());
        }

        let mut decoder = Decoder::default();
        let mut decoded = Vec::new();
        while let Some(m) = decoder.decode(&mut buf).unwrap() {
            decoded.push(m);
        }
        assert_eq!(decoded, vec![a, b, c]);
        assert!(buf.is_empty());
    }

    #[test]
    fn truncation_yields_none_not_error() {
        let m = msg(b"header", &[b"frame"]);
        let encoded = m.encode_to_vec(&Limits::DEFAULT).unwrap();
        for cut in 0..encoded.len() {
            let mut decoder = Decoder::default();
            let mut buf = BytesMut::from(&encoded[..cut]);
            assert_eq!(
                decoder.decode(&mut buf).unwrap(),
                None,
                "prefix of {cut} bytes must be incomplete, not an error"
            );
            assert!(!decoder.is_poisoned());
        }
    }

    #[test]
    fn bad_magic_is_fatal_and_poisons() {
        let mut buf = BytesMut::from(&b"XXXX\x00\x00\x00\x01\x00\x00\x00\x00"[..]);
        let mut decoder = Decoder::default();
        let err = decoder.decode(&mut buf).unwrap_err();
        assert!(matches!(err, FrameError::BadMagic { .. }));
        assert!(decoder.is_poisoned());
        // Any subsequent call keeps failing rather than silently resyncing.
        assert_eq!(decoder.decode(&mut buf).unwrap_err(), FrameError::Poisoned);
    }

    #[test]
    fn header_length_limit_is_enforced_before_allocating() {
        let mut buf = BytesMut::new();
        buf.put_slice(&MAGIC);
        buf.put_u32(u32::MAX); // absurd header length
        buf.put_u32(0);
        let mut decoder = Decoder::new(Limits::tiny());
        let err = decoder.decode(&mut buf).unwrap_err();
        assert!(matches!(err, FrameError::HeaderTooLarge { .. }));
    }

    #[test]
    fn frame_count_limit_is_enforced_before_allocating() {
        let mut buf = BytesMut::new();
        buf.put_slice(&MAGIC);
        buf.put_u32(0);
        buf.put_u32(u32::MAX); // absurd frame count
        let mut decoder = Decoder::new(Limits::tiny());
        let err = decoder.decode(&mut buf).unwrap_err();
        assert!(matches!(err, FrameError::TooManyFrames { .. }));
    }

    #[test]
    fn frame_size_limit_is_enforced() {
        let limits = Limits::tiny();
        let mut buf = BytesMut::new();
        buf.put_slice(&MAGIC);
        buf.put_u32(0);
        buf.put_u32(1);
        buf.put_u32(limits.max_frame_len as u32 + 1);
        let mut decoder = Decoder::new(limits);
        let err = decoder.decode(&mut buf).unwrap_err();
        assert!(matches!(err, FrameError::FrameTooLarge { index: 0, .. }));
    }

    #[test]
    fn total_message_size_limit_catches_death_by_a_thousand_frames() {
        // Every individual frame is legal; their sum is not.
        let limits = Limits {
            max_header_len: 1024,
            max_frames: 8,
            max_frame_len: 4096,
            max_message_len: 8192,
        };
        let mut buf = BytesMut::new();
        buf.put_slice(&MAGIC);
        buf.put_u32(0);
        buf.put_u32(8);
        for _ in 0..8 {
            buf.put_u32(4096);
        }
        let mut decoder = Decoder::new(limits);
        let err = decoder.decode(&mut buf).unwrap_err();
        assert!(matches!(err, FrameError::MessageTooLarge { .. }));
    }

    #[test]
    fn encoding_rejects_messages_that_exceed_limits() {
        let limits = Limits::tiny();
        let big = Message::header_only(Bytes::from(vec![0u8; 2048]));
        assert!(matches!(
            big.encode_to_vec(&limits),
            Err(FrameError::HeaderTooLarge { .. })
        ));

        let many = Message::new(Bytes::new(), vec![Bytes::from_static(b"x"); 9]);
        assert!(matches!(
            many.encode_to_vec(&limits),
            Err(FrameError::TooManyFrames { .. })
        ));

        let fat = Message::new(Bytes::new(), vec![Bytes::from(vec![0u8; 5000])]);
        assert!(matches!(
            fat.encode_to_vec(&limits),
            Err(FrameError::FrameTooLarge { index: 0, .. })
        ));
    }

    #[test]
    fn encode_decode_survives_a_realistic_ten_megabyte_payload() {
        // The design targets ~10 MB results; make sure nothing overflows or
        // quietly truncates at that size.
        let frame = Bytes::from(vec![0xAB; 10 * 1024 * 1024]);
        let m = Message::new(Bytes::from_static(b"result-header"), vec![frame.clone()]);
        let decoded = roundtrip(&m, &Limits::DEFAULT);
        assert_eq!(decoded.frames.len(), 1);
        assert_eq!(decoded.frames[0].len(), frame.len());
        assert_eq!(decoded.frames[0], frame);
    }

    #[test]
    fn decoded_frames_share_the_read_buffer_allocation() {
        let m = msg(b"h", &[b"0123456789"]);
        let encoded = m.encode_to_vec(&Limits::DEFAULT).unwrap();
        let mut buf = BytesMut::from(&encoded[..]);
        let base = buf.as_ptr();
        let offset = FIXED_PREFIX_LEN + 4 + 1; // prefix + one size + header
        let mut decoder = Decoder::default();
        let decoded = decoder.decode(&mut buf).unwrap().unwrap();
        // Zero copy: the frame points into the original read buffer.
        assert_eq!(decoded.frames[0].as_ptr(), unsafe { base.add(offset) });
    }

    #[test]
    fn needed_reports_progress_through_the_stages() {
        let m = msg(b"header", &[b"frame"]);
        let encoded = m.encode_to_vec(&Limits::DEFAULT).unwrap();
        let mut decoder = Decoder::default();
        assert_eq!(decoder.needed(), FIXED_PREFIX_LEN);

        let mut buf = BytesMut::from(&encoded[..FIXED_PREFIX_LEN]);
        assert_eq!(decoder.decode(&mut buf).unwrap(), None);
        assert_eq!(decoder.needed(), 4, "one frame size outstanding");
        assert!(!decoder.is_at_message_boundary());

        buf.extend_from_slice(&encoded[FIXED_PREFIX_LEN..FIXED_PREFIX_LEN + 4]);
        assert_eq!(decoder.decode(&mut buf).unwrap(), None);
        assert_eq!(decoder.needed(), 6, "header outstanding");
    }

    #[test]
    fn debug_output_summarises_instead_of_dumping_payload() {
        let m = Message::new(
            Bytes::from_static(b"hdr"),
            vec![Bytes::from(vec![7u8; 4096])],
        );
        let text = format!("{m:?}");
        assert!(text.contains("header_len: 3"));
        assert!(text.contains("frame_sizes: [4096]"));
        assert!(!text.contains("7, 7, 7"), "must not dump payload bytes");
    }
}

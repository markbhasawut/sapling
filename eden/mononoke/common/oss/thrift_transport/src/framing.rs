/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

//! Frames inside a response body.
//!
//! A response body is a sequence of frames: a big-endian `u32` length
//! followed by that many bytes. The first frame is the reply, or a streaming
//! reply's initial response. A streaming reply follows it with stream frames,
//! each tagged with a one-byte kind, and a successful stream ends with an
//! explicit end-of-stream frame.

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;

/// Default limit on a frame's length and on a request's size.
pub const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

const LENGTH_SIZE: usize = 4;

const STREAM_TYPE_SUCCESS: u8 = 0;
const STREAM_TYPE_DECLARED_EX: u8 = 1;
const STREAM_TYPE_APP_EX: u8 = 2;
const STREAM_TYPE_EOS: u8 = 3;

/// A frame that follows the initial response of a streaming reply.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum StreamFrame {
    Success(Bytes),
    DeclaredException(Bytes),
    ApplicationException(Bytes),
    EndOfStream,
}

impl StreamFrame {
    /// The frame's payload: its kind followed by its data. `max` limits its
    /// length.
    pub(crate) fn encode(&self, max: usize) -> Result<Bytes> {
        let (frame_type, payload) = match self {
            Self::Success(payload) => (STREAM_TYPE_SUCCESS, payload.clone()),
            Self::DeclaredException(payload) => (STREAM_TYPE_DECLARED_EX, payload.clone()),
            Self::ApplicationException(payload) => (STREAM_TYPE_APP_EX, payload.clone()),
            Self::EndOfStream => (STREAM_TYPE_EOS, Bytes::new()),
        };
        let frame_len = payload
            .len()
            .checked_add(1)
            .context("stream frame length overflow")?;
        if frame_len > max {
            bail!("stream frame too large: {frame_len} bytes (max {max})");
        }

        let mut frame = BytesMut::with_capacity(frame_len);
        frame.put_u8(frame_type);
        frame.put_slice(&payload);
        Ok(frame.freeze())
    }

    pub(crate) fn decode(frame: Bytes) -> Result<Self> {
        let Some(&frame_type) = frame.first() else {
            bail!("empty stream frame");
        };
        let payload = frame.slice(1..);
        match frame_type {
            STREAM_TYPE_SUCCESS => Ok(Self::Success(payload)),
            STREAM_TYPE_DECLARED_EX => Ok(Self::DeclaredException(payload)),
            STREAM_TYPE_APP_EX => Ok(Self::ApplicationException(payload)),
            STREAM_TYPE_EOS if payload.is_empty() => Ok(Self::EndOfStream),
            STREAM_TYPE_EOS => bail!("end-of-stream frame contains a payload"),
            other => bail!("unknown stream frame type: {other}"),
        }
    }
}

/// Prefix `payload` with its length. `max` limits the length.
pub(crate) fn encode_frame(payload: &[u8], max: usize) -> Result<Bytes> {
    if payload.is_empty() {
        bail!("refusing to write a zero-length frame");
    }
    if payload.len() > max {
        bail!("frame too large: {} bytes (max {max})", payload.len());
    }
    let frame_len = u32::try_from(payload.len()).context("frame length does not fit in u32")?;
    let mut frame = BytesMut::with_capacity(LENGTH_SIZE + payload.len());
    frame.put_u32(frame_len);
    frame.put_slice(payload);
    Ok(frame.freeze())
}

/// Splits a byte stream, which arrives in chunks of any size, into frames.
///
/// Memory grows only with the bytes pushed: a length header is checked
/// against the limit as soon as it is complete, but nothing is allocated for
/// it, so a peer can't make this hold a frame it never sends.
pub(crate) struct FrameDecoder {
    buffer: BytesMut,
    max: usize,
}

impl FrameDecoder {
    pub(crate) fn new(max: usize) -> Self {
        Self {
            buffer: BytesMut::new(),
            max,
        }
    }

    /// Add the next chunk of the byte stream.
    pub(crate) fn push(&mut self, chunk: &[u8]) {
        self.buffer.extend_from_slice(chunk);
    }

    /// The next complete frame, if there is one. Fails if the next frame's
    /// length is zero or over the limit.
    pub(crate) fn next_frame(&mut self) -> Result<Option<Bytes>> {
        if self.buffer.len() < LENGTH_SIZE {
            return Ok(None);
        }
        let header: [u8; LENGTH_SIZE] = self.buffer[..LENGTH_SIZE]
            .try_into()
            .expect("the header slice has the header's length");
        let frame_len = u32::from_be_bytes(header) as usize;
        if frame_len == 0 {
            bail!("zero-length frame");
        }
        if frame_len > self.max {
            bail!("frame too large: {frame_len} bytes (max {})", self.max);
        }
        if self.buffer.len() - LENGTH_SIZE < frame_len {
            return Ok(None);
        }
        self.buffer.advance(LENGTH_SIZE);
        Ok(Some(self.buffer.split_to(frame_len).freeze()))
    }

    /// Fails unless every byte pushed so far belonged to a complete frame,
    /// for when the byte stream has ended.
    pub(crate) fn finish(&self) -> Result<()> {
        if self.buffer.is_empty() {
            Ok(())
        } else {
            bail!(
                "stream ended inside a frame, with {} bytes of it",
                self.buffer.len()
            )
        }
    }

    /// Bytes pushed but not yet returned as frames.
    #[cfg(test)]
    fn buffered(&self) -> usize {
        self.buffer.len()
    }
}

#[cfg(test)]
mod tests {
    use mononoke_macros::mononoke;
    use quickcheck::Arbitrary;
    use quickcheck::Gen;
    use quickcheck::QuickCheck;
    use quickcheck::TestResult;

    use super::*;

    const SMALL_MAX: usize = 64;

    /// Frame payloads, with the points at which to split their encoding.
    #[derive(Clone, Debug)]
    struct Frames {
        payloads: Vec<Vec<u8>>,
        splits: Vec<usize>,
    }

    impl Arbitrary for Frames {
        fn arbitrary(g: &mut Gen) -> Self {
            let count = usize::arbitrary(g) % 8;
            let payloads = (0..count)
                .map(|_| {
                    let len = 1 + usize::arbitrary(g) % SMALL_MAX;
                    (0..len).map(|_| u8::arbitrary(g)).collect()
                })
                .collect();
            Self {
                payloads,
                splits: Vec::arbitrary(g),
            }
        }
    }

    fn encode_all(payloads: &[Vec<u8>]) -> Vec<u8> {
        payloads
            .iter()
            .flat_map(|payload| {
                encode_frame(payload, SMALL_MAX)
                    .expect("the payload is within the limit")
                    .to_vec()
            })
            .collect()
    }

    /// Feed `bytes` to a decoder in chunks that end at `splits`, collecting
    /// the frames, or the first error.
    fn decode_in_chunks(bytes: &[u8], splits: &[usize], max: usize) -> Result<Vec<Bytes>> {
        let mut points: Vec<usize> = splits
            .iter()
            .map(|split| split % (bytes.len() + 1))
            .collect();
        points.push(bytes.len());
        points.sort_unstable();

        let mut decoder = FrameDecoder::new(max);
        let mut frames = Vec::new();
        let mut start = 0;
        for end in points {
            decoder.push(&bytes[start..end]);
            start = end;
            while let Some(frame) = decoder.next_frame()? {
                frames.push(frame);
            }
        }
        decoder.finish()?;
        Ok(frames)
    }

    #[mononoke::test]
    fn frames_survive_any_chunking() {
        fn property(frames: Frames) -> bool {
            let bytes = encode_all(&frames.payloads);
            let decoded =
                decode_in_chunks(&bytes, &frames.splits, SMALL_MAX).expect("valid frames decode");
            decoded
                .iter()
                .map(|frame| frame.to_vec())
                .eq(frames.payloads.iter().cloned())
        }
        QuickCheck::new()
            .tests(1000)
            .quickcheck(property as fn(Frames) -> bool);
    }

    #[mononoke::test]
    fn arbitrary_bytes_never_panic_or_exceed_the_limit() {
        fn property(bytes: Vec<u8>, splits: Vec<usize>) -> bool {
            match decode_in_chunks(&bytes, &splits, SMALL_MAX) {
                Ok(frames) => frames
                    .iter()
                    .all(|frame| !frame.is_empty() && frame.len() <= SMALL_MAX),
                Err(_) => true,
            }
        }
        QuickCheck::new()
            .tests(2000)
            .quickcheck(property as fn(Vec<u8>, Vec<usize>) -> bool);
    }

    #[mononoke::test]
    fn truncation_anywhere_fails_at_the_end() {
        fn property(frames: Frames, cut: usize) -> TestResult {
            let bytes = encode_all(&frames.payloads);
            if bytes.is_empty() {
                return TestResult::discard();
            }
            let cut = cut % bytes.len();
            let truncated = &bytes[..cut];
            // Whole frames before the cut decode; the partial one is an
            // error, unless the cut falls between frames.
            let mut boundary = 0;
            let mut at_boundary = cut == 0;
            for payload in &frames.payloads {
                boundary += LENGTH_SIZE + payload.len();
                at_boundary |= boundary == cut;
            }
            let result = decode_in_chunks(truncated, &frames.splits, SMALL_MAX);
            TestResult::from_bool(result.is_ok() == at_boundary)
        }
        QuickCheck::new()
            .tests(1000)
            .quickcheck(property as fn(Frames, usize) -> TestResult);
    }

    #[mononoke::test]
    fn stream_frames_of_arbitrary_bytes_never_panic() {
        fn property(bytes: Vec<u8>) -> bool {
            match StreamFrame::decode(Bytes::from(bytes.clone())) {
                Ok(frame) => {
                    frame.encode(MAX_FRAME_SIZE).expect("a decoded frame encodes") == bytes
                }
                Err(_) => true,
            }
        }
        QuickCheck::new()
            .tests(2000)
            .quickcheck(property as fn(Vec<u8>) -> bool);
    }

    #[mononoke::test]
    fn an_oversized_length_fails_before_its_body_arrives() {
        let mut decoder = FrameDecoder::new(SMALL_MAX);
        decoder.push(&u32::try_from(SMALL_MAX + 1).unwrap().to_be_bytes());
        assert!(decoder.next_frame().is_err());
        // Nothing was allocated for the declared length.
        assert_eq!(decoder.buffered(), LENGTH_SIZE);

        let mut decoder = FrameDecoder::new(MAX_FRAME_SIZE);
        decoder.push(&u32::MAX.to_be_bytes());
        assert!(decoder.next_frame().is_err());
    }

    #[mononoke::test]
    fn zero_length_frames_are_rejected() {
        let mut decoder = FrameDecoder::new(SMALL_MAX);
        decoder.push(&[0, 0, 0, 0]);
        assert!(decoder.next_frame().is_err());
        assert!(encode_frame(b"", SMALL_MAX).is_err());
    }

    #[mononoke::test]
    fn frames_over_the_limit_are_not_encoded() {
        assert!(encode_frame(&[0; SMALL_MAX + 1], SMALL_MAX).is_err());
        assert!(StreamFrame::Success(Bytes::from(vec![0; SMALL_MAX])).encode(SMALL_MAX).is_err());
    }

    #[mononoke::test]
    fn stream_frame_round_trip() {
        let frames = [
            StreamFrame::Success(Bytes::from_static(b"success")),
            StreamFrame::DeclaredException(Bytes::from_static(b"declared")),
            StreamFrame::ApplicationException(Bytes::from_static(b"application")),
            StreamFrame::EndOfStream,
        ];

        for frame in frames {
            let encoded = frame.encode(MAX_FRAME_SIZE).expect("frame should encode");
            let decoded = StreamFrame::decode(encoded).expect("frame should decode");
            assert_eq!(decoded, frame);
        }
    }

    #[mononoke::test]
    fn stream_frame_rejects_invalid_frames() {
        assert!(StreamFrame::decode(Bytes::new()).is_err());
        assert!(StreamFrame::decode(Bytes::from_static(&[STREAM_TYPE_EOS, 1])).is_err());
        assert!(StreamFrame::decode(Bytes::from_static(&[99])).is_err());
    }
}

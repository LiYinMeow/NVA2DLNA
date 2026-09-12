//! Incremental codec for the binary portion of Bilibili's NVA protocol.
//!
//! The codec deliberately stops at framing and UTF-8 validation. JSON payloads
//! remain strings so the session layer can deserialize them into the type that
//! is appropriate for each command.

use std::error::Error;
use std::fmt;

use bytes::{Buf, BytesMut};

const COMMAND_MARKER: u8 = 0xe0;
const REPLY_MARKER: u8 = 0xc0;
const PING_MARKER: u8 = 0xe4;
const COMMAND_VERSION: u8 = 0x01;
const COMMAND_NAMESPACE: &[u8; 7] = b"Command";

/// Default maximum size of an NVA JSON argument (1 MiB).
///
/// Real command payloads are normally only a few KiB. The limit prevents an
/// untrusted peer from making the decoder retain an arbitrarily large frame.
pub const DEFAULT_MAX_JSON_BYTES: usize = 1024 * 1024;

// The largest non-JSON portion is an E0 command with a 255-byte method name.
const MAX_FRAME_OVERHEAD: usize = 1 + 1 + 4 + 1 + 1 + 7 + 1 + 255 + 4;

/// A decoded NVA binary frame.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Frame {
    Command(CommandFrame),
    Reply(ReplyFrame),
    Ping(PingFrame),
}

/// A command sent by either endpoint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandFrame {
    pub sequence: u32,
    pub method: String,
    pub json: Option<String>,
}

/// A reply to a command. Its sequence matches the command being answered.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplyFrame {
    pub sequence: u32,
    pub json: Option<String>,
}

/// The receiver heartbeat frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PingFrame {
    pub sequence: u32,
}

/// A recoverable problem found while decoding a stream.
///
/// [`Decoder::push`] returns errors inline with frames. After an error the
/// decoder resynchronizes at the next possible frame marker and continues.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DecodeError {
    DiscardedGarbage { bytes: usize },
    InvalidArgumentCount { marker: u8, found: u8 },
    InvalidCommandPreamble,
    JsonTooLarge { declared: usize, maximum: usize },
    InvalidUtf8 { field: Utf8Field },
    BufferLimitExceeded { dropped: usize },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Utf8Field {
    Method,
    Json,
}

impl fmt::Display for DecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DiscardedGarbage { bytes } => {
                write!(
                    formatter,
                    "discarded {bytes} byte(s) before the next NVA frame"
                )
            }
            Self::InvalidArgumentCount { marker, found } => write!(
                formatter,
                "invalid argument count {found} for NVA marker 0x{marker:02x}"
            ),
            Self::InvalidCommandPreamble => formatter.write_str("invalid NVA command preamble"),
            Self::JsonTooLarge { declared, maximum } => write!(
                formatter,
                "NVA JSON argument is {declared} bytes, exceeding the {maximum}-byte limit"
            ),
            Self::InvalidUtf8 { field } => {
                write!(formatter, "NVA {field} is not valid UTF-8")
            }
            Self::BufferLimitExceeded { dropped } => write!(
                formatter,
                "discarded {dropped} byte(s) after the NVA decoder buffer limit was reached"
            ),
        }
    }
}

impl Error for DecodeError {}

impl fmt::Display for Utf8Field {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Method => formatter.write_str("method"),
            Self::Json => formatter.write_str("JSON argument"),
        }
    }
}

/// An error returned while constructing an outbound frame.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EncodeError {
    EmptyMethod,
    MethodTooLong { length: usize, maximum: usize },
    JsonTooLong { length: usize, maximum: usize },
}

impl fmt::Display for EncodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyMethod => formatter.write_str("an NVA command method cannot be empty"),
            Self::MethodTooLong { length, maximum } => write!(
                formatter,
                "NVA command method is {length} bytes, exceeding the {maximum}-byte wire limit"
            ),
            Self::JsonTooLong { length, maximum } => write!(
                formatter,
                "NVA JSON argument is {length} bytes, exceeding the {maximum}-byte wire limit"
            ),
        }
    }
}

impl Error for EncodeError {}

/// Encode an E0 command frame.
pub fn encode_command(
    sequence: u32,
    method: &str,
    json: Option<&str>,
) -> Result<Vec<u8>, EncodeError> {
    let method = method.as_bytes();
    if method.is_empty() {
        return Err(EncodeError::EmptyMethod);
    }
    let method_len = u8::try_from(method.len()).map_err(|_| EncodeError::MethodTooLong {
        length: method.len(),
        maximum: u8::MAX as usize,
    })?;

    let json_len = json
        .map(str::as_bytes)
        .map(|bytes| {
            u32::try_from(bytes.len()).map_err(|_| EncodeError::JsonTooLong {
                length: bytes.len(),
                maximum: u32::MAX as usize,
            })
        })
        .transpose()?;

    let json_bytes = json.map(str::as_bytes);
    let capacity = 16usize
        .saturating_add(method.len())
        .saturating_add(json_bytes.map_or(0, |bytes| 4usize.saturating_add(bytes.len())));
    let mut output = Vec::with_capacity(capacity);
    output.push(COMMAND_MARKER);
    output.push(if json.is_some() { 3 } else { 2 });
    output.extend_from_slice(&sequence.to_be_bytes());
    output.push(COMMAND_VERSION);
    output.push(COMMAND_NAMESPACE.len() as u8);
    output.extend_from_slice(COMMAND_NAMESPACE);
    output.push(method_len);
    output.extend_from_slice(method);
    if let (Some(length), Some(bytes)) = (json_len, json_bytes) {
        output.extend_from_slice(&length.to_be_bytes());
        output.extend_from_slice(bytes);
    }
    Ok(output)
}

/// Encode a C0 reply frame.
pub fn encode_reply(sequence: u32, json: Option<&str>) -> Result<Vec<u8>, EncodeError> {
    let json_len = json
        .map(str::as_bytes)
        .map(|bytes| {
            u32::try_from(bytes.len()).map_err(|_| EncodeError::JsonTooLong {
                length: bytes.len(),
                maximum: u32::MAX as usize,
            })
        })
        .transpose()?;

    let json_bytes = json.map(str::as_bytes);
    let capacity = 6usize.saturating_add(json_bytes.map_or(0, |bytes| 4 + bytes.len()));
    let mut output = Vec::with_capacity(capacity);
    output.push(REPLY_MARKER);
    output.push(u8::from(json.is_some()));
    output.extend_from_slice(&sequence.to_be_bytes());
    if let (Some(length), Some(bytes)) = (json_len, json_bytes) {
        output.extend_from_slice(&length.to_be_bytes());
        output.extend_from_slice(bytes);
    }
    Ok(output)
}

/// Encode an E4 heartbeat frame.
pub fn encode_ping(sequence: u32) -> Vec<u8> {
    let mut output = Vec::with_capacity(6);
    output.push(PING_MARKER);
    output.push(0);
    output.extend_from_slice(&sequence.to_be_bytes());
    output
}

/// Incrementally decodes a TCP byte stream into NVA frames.
#[derive(Debug)]
pub struct Decoder {
    buffer: BytesMut,
    max_json_bytes: usize,
    max_buffer_bytes: usize,
}

impl Default for Decoder {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_JSON_BYTES)
    }
}

impl Decoder {
    /// Create a decoder with a caller-selected JSON argument limit.
    pub fn new(max_json_bytes: usize) -> Self {
        Self {
            buffer: BytesMut::new(),
            max_json_bytes,
            max_buffer_bytes: max_json_bytes.saturating_add(MAX_FRAME_OVERHEAD),
        }
    }

    /// Number of bytes retained for a future, incomplete frame.
    pub fn buffered_len(&self) -> usize {
        self.buffer.len()
    }

    /// Feed another part of the TCP stream into the decoder.
    ///
    /// Empty output means that the bytes form an incomplete frame. Errors are
    /// recoverable: subsequent items in the returned vector may be valid frames.
    pub fn push(&mut self, mut input: &[u8]) -> Vec<Result<Frame, DecodeError>> {
        let mut output = Vec::new();

        while !input.is_empty() {
            if self.buffer.len() == self.max_buffer_bytes {
                let before = self.buffer.len();
                self.decode_available(&mut output);
                if self.buffer.len() == before {
                    self.buffer.advance(1);
                    output.push(Err(DecodeError::BufferLimitExceeded { dropped: 1 }));
                }
            }

            let available = self.max_buffer_bytes.saturating_sub(self.buffer.len());
            // MAX_FRAME_OVERHEAD keeps max_buffer_bytes non-zero, even if the
            // configured JSON limit is zero.
            let take = available.min(input.len());
            self.buffer.extend_from_slice(&input[..take]);
            input = &input[take..];
            self.decode_available(&mut output);
        }

        self.decode_available(&mut output);
        output
    }

    fn decode_available(&mut self, output: &mut Vec<Result<Frame, DecodeError>>) {
        loop {
            if self.buffer.is_empty() {
                return;
            }

            if !is_marker(self.buffer[0]) {
                let garbage_len = self
                    .buffer
                    .iter()
                    .position(|byte| is_marker(*byte))
                    .unwrap_or(self.buffer.len());
                self.buffer.advance(garbage_len);
                output.push(Err(DecodeError::DiscardedGarbage { bytes: garbage_len }));
                continue;
            }

            let parsed = match self.buffer[0] {
                COMMAND_MARKER => self.parse_command(),
                REPLY_MARKER => self.parse_reply(),
                PING_MARKER => self.parse_ping(),
                _ => unreachable!("the marker was checked above"),
            };

            match parsed {
                ParseOutcome::Incomplete => return,
                ParseOutcome::Frame { frame, consumed } => {
                    self.buffer.advance(consumed);
                    output.push(Ok(frame));
                }
                ParseOutcome::Malformed { error, consumed } => {
                    self.buffer.advance(consumed);
                    output.push(Err(error));
                }
            }
        }
    }

    fn parse_command(&self) -> ParseOutcome {
        if self.buffer.len() < 2 {
            return ParseOutcome::Incomplete;
        }
        let argument_count = self.buffer[1];
        if !matches!(argument_count, 2 | 3) {
            return malformed_marker(DecodeError::InvalidArgumentCount {
                marker: COMMAND_MARKER,
                found: argument_count,
            });
        }

        if self.buffer.len() < 8 {
            return ParseOutcome::Incomplete;
        }
        if self.buffer[6] != COMMAND_VERSION || self.buffer[7] != COMMAND_NAMESPACE.len() as u8 {
            return malformed_marker(DecodeError::InvalidCommandPreamble);
        }

        let namespace_end = 8 + COMMAND_NAMESPACE.len();
        if self.buffer.len() < namespace_end {
            return ParseOutcome::Incomplete;
        }
        if &self.buffer[8..namespace_end] != COMMAND_NAMESPACE {
            return malformed_marker(DecodeError::InvalidCommandPreamble);
        }

        if self.buffer.len() <= namespace_end {
            return ParseOutcome::Incomplete;
        }
        let method_len = self.buffer[namespace_end] as usize;
        let method_start = namespace_end + 1;
        let Some(method_end) = method_start.checked_add(method_len) else {
            return malformed_marker(DecodeError::InvalidCommandPreamble);
        };
        if self.buffer.len() < method_end {
            return ParseOutcome::Incomplete;
        }

        let (frame_end, json_range) = if argument_count == 3 {
            let Some(length_end) = method_end.checked_add(4) else {
                return malformed_marker(DecodeError::InvalidCommandPreamble);
            };
            if self.buffer.len() < length_end {
                return ParseOutcome::Incomplete;
            }
            let json_len = read_u32(&self.buffer[method_end..length_end]) as usize;
            if json_len > self.max_json_bytes {
                return malformed_marker(DecodeError::JsonTooLarge {
                    declared: json_len,
                    maximum: self.max_json_bytes,
                });
            }
            let Some(frame_end) = length_end.checked_add(json_len) else {
                return malformed_marker(DecodeError::JsonTooLarge {
                    declared: json_len,
                    maximum: self.max_json_bytes,
                });
            };
            if self.buffer.len() < frame_end {
                return ParseOutcome::Incomplete;
            }
            (frame_end, Some(length_end..frame_end))
        } else {
            (method_end, None)
        };

        let method = match std::str::from_utf8(&self.buffer[method_start..method_end]) {
            Ok(method) if !method.is_empty() => method.to_owned(),
            Ok(_) => {
                return ParseOutcome::Malformed {
                    error: DecodeError::InvalidCommandPreamble,
                    consumed: frame_end,
                };
            }
            Err(_) => {
                return ParseOutcome::Malformed {
                    error: DecodeError::InvalidUtf8 {
                        field: Utf8Field::Method,
                    },
                    consumed: frame_end,
                };
            }
        };

        let json = match json_range {
            Some(range) => match std::str::from_utf8(&self.buffer[range]) {
                Ok(json) => Some(json.to_owned()),
                Err(_) => {
                    return ParseOutcome::Malformed {
                        error: DecodeError::InvalidUtf8 {
                            field: Utf8Field::Json,
                        },
                        consumed: frame_end,
                    };
                }
            },
            None => None,
        };

        ParseOutcome::Frame {
            frame: Frame::Command(CommandFrame {
                sequence: read_u32(&self.buffer[2..6]),
                method,
                json,
            }),
            consumed: frame_end,
        }
    }

    fn parse_reply(&self) -> ParseOutcome {
        if self.buffer.len() < 2 {
            return ParseOutcome::Incomplete;
        }
        let argument_count = self.buffer[1];
        if !matches!(argument_count, 0 | 1) {
            return malformed_marker(DecodeError::InvalidArgumentCount {
                marker: REPLY_MARKER,
                found: argument_count,
            });
        }
        if self.buffer.len() < 6 {
            return ParseOutcome::Incomplete;
        }

        let (frame_end, json_range) = if argument_count == 1 {
            if self.buffer.len() < 10 {
                return ParseOutcome::Incomplete;
            }
            let json_len = read_u32(&self.buffer[6..10]) as usize;
            if json_len > self.max_json_bytes {
                return malformed_marker(DecodeError::JsonTooLarge {
                    declared: json_len,
                    maximum: self.max_json_bytes,
                });
            }
            let Some(frame_end) = 10usize.checked_add(json_len) else {
                return malformed_marker(DecodeError::JsonTooLarge {
                    declared: json_len,
                    maximum: self.max_json_bytes,
                });
            };
            if self.buffer.len() < frame_end {
                return ParseOutcome::Incomplete;
            }
            (frame_end, Some(10..frame_end))
        } else {
            (6, None)
        };

        let json = match json_range {
            Some(range) => match std::str::from_utf8(&self.buffer[range]) {
                Ok(json) => Some(json.to_owned()),
                Err(_) => {
                    return ParseOutcome::Malformed {
                        error: DecodeError::InvalidUtf8 {
                            field: Utf8Field::Json,
                        },
                        consumed: frame_end,
                    };
                }
            },
            None => None,
        };

        ParseOutcome::Frame {
            frame: Frame::Reply(ReplyFrame {
                sequence: read_u32(&self.buffer[2..6]),
                json,
            }),
            consumed: frame_end,
        }
    }

    fn parse_ping(&self) -> ParseOutcome {
        if self.buffer.len() < 2 {
            return ParseOutcome::Incomplete;
        }
        if self.buffer[1] != 0 {
            return malformed_marker(DecodeError::InvalidArgumentCount {
                marker: PING_MARKER,
                found: self.buffer[1],
            });
        }
        if self.buffer.len() < 6 {
            return ParseOutcome::Incomplete;
        }

        ParseOutcome::Frame {
            frame: Frame::Ping(PingFrame {
                sequence: read_u32(&self.buffer[2..6]),
            }),
            consumed: 6,
        }
    }
}

enum ParseOutcome {
    Incomplete,
    Frame { frame: Frame, consumed: usize },
    Malformed { error: DecodeError, consumed: usize },
}

fn malformed_marker(error: DecodeError) -> ParseOutcome {
    // At this point the declared frame boundary is not trustworthy. Dropping
    // only its marker lets the outer loop find the next plausible marker.
    ParseOutcome::Malformed { error, consumed: 1 }
}

fn is_marker(byte: u8) -> bool {
    matches!(byte, COMMAND_MARKER | REPLY_MARKER | PING_MARKER)
}

fn read_u32(bytes: &[u8]) -> u32 {
    u32::from_be_bytes(
        bytes
            .try_into()
            .expect("the caller always passes exactly four bytes"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn one_frame(items: Vec<Result<Frame, DecodeError>>) -> Frame {
        assert_eq!(items.len(), 1, "expected exactly one decoder item");
        items.into_iter().next().unwrap().unwrap()
    }

    #[test]
    fn command_without_json_matches_the_wire_layout() {
        let encoded = encode_command(1, "GetVolume", None).unwrap();
        assert_eq!(
            encoded,
            [
                0xe0, 0x02, 0x00, 0x00, 0x00, 0x01, 0x01, 0x07, b'C', b'o', b'm', b'm', b'a', b'n',
                b'd', 0x09, b'G', b'e', b't', b'V', b'o', b'l', b'u', b'm', b'e',
            ]
        );

        assert_eq!(
            one_frame(Decoder::default().push(&encoded)),
            Frame::Command(CommandFrame {
                sequence: 1,
                method: "GetVolume".into(),
                json: None,
            })
        );
    }

    #[test]
    fn command_round_trips_chinese_json_by_utf8_byte_length() {
        let json = r#"{"content":"你好，哔哩必连","color":16777215}"#;
        let encoded = encode_command(0x0102_0304, "SendDanmaku", Some(json)).unwrap();

        let method_end = 16 + "SendDanmaku".len();
        assert_eq!(
            read_u32(&encoded[method_end..method_end + 4]) as usize,
            json.len()
        );
        assert_eq!(
            one_frame(Decoder::default().push(&encoded)),
            Frame::Command(CommandFrame {
                sequence: 0x0102_0304,
                method: "SendDanmaku".into(),
                json: Some(json.into()),
            })
        );
    }

    #[test]
    fn reply_and_ping_match_the_wire_layout() {
        let reply = encode_reply(1, Some(r#"{"volume":33}"#)).unwrap();
        assert_eq!(
            reply,
            [
                0xc0, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0d, b'{', b'"', b'v', b'o',
                b'l', b'u', b'm', b'e', b'"', b':', b'3', b'3', b'}',
            ]
        );
        assert_eq!(
            one_frame(Decoder::default().push(&reply)),
            Frame::Reply(ReplyFrame {
                sequence: 1,
                json: Some(r#"{"volume":33}"#.into()),
            })
        );

        let empty_reply = encode_reply(2, None).unwrap();
        assert_eq!(empty_reply, [0xc0, 0x00, 0x00, 0x00, 0x00, 0x02]);
        assert_eq!(
            one_frame(Decoder::default().push(&empty_reply)),
            Frame::Reply(ReplyFrame {
                sequence: 2,
                json: None,
            })
        );

        let ping = encode_ping(0x1020_3040);
        assert_eq!(ping, [0xe4, 0x00, 0x10, 0x20, 0x30, 0x40]);
        assert_eq!(
            one_frame(Decoder::default().push(&ping)),
            Frame::Ping(PingFrame {
                sequence: 0x1020_3040,
            })
        );
    }

    #[test]
    fn every_split_point_is_a_valid_partial_delivery() {
        let bytes = encode_command(7, "Play", Some(r#"{"title":"测试视频"}"#)).unwrap();

        for split in 0..=bytes.len() {
            let mut decoder = Decoder::default();
            let first = decoder.push(&bytes[..split]);
            if split < bytes.len() {
                assert!(
                    first.is_empty(),
                    "split {split} decoded too early: {first:?}"
                );
                assert_eq!(decoder.buffered_len(), split);
            }
            let mut items = first;
            items.extend(decoder.push(&bytes[split..]));
            assert_eq!(
                items,
                [Ok(Frame::Command(CommandFrame {
                    sequence: 7,
                    method: "Play".into(),
                    json: Some(r#"{"title":"测试视频"}"#.into()),
                }))],
                "failed at split {split}"
            );
            assert_eq!(decoder.buffered_len(), 0);
        }
    }

    #[test]
    fn sticky_frames_decode_in_order_and_leave_a_partial_tail() {
        let first = encode_ping(1);
        let second = encode_command(2, "Pause", None).unwrap();
        let third = encode_reply(2, Some(r#"{"ok":true}"#)).unwrap();
        let split = third.len() - 2;

        let mut bytes = first;
        bytes.extend_from_slice(&second);
        bytes.extend_from_slice(&third[..split]);

        let mut decoder = Decoder::default();
        assert_eq!(
            decoder.push(&bytes),
            [
                Ok(Frame::Ping(PingFrame { sequence: 1 })),
                Ok(Frame::Command(CommandFrame {
                    sequence: 2,
                    method: "Pause".into(),
                    json: None,
                })),
            ]
        );
        assert_eq!(decoder.buffered_len(), split);
        assert_eq!(
            decoder.push(&third[split..]),
            [Ok(Frame::Reply(ReplyFrame {
                sequence: 2,
                json: Some(r#"{"ok":true}"#.into()),
            }))]
        );
    }

    #[test]
    fn garbage_is_reported_once_and_decoder_resynchronizes() {
        let mut bytes = b"not an nva frame".to_vec();
        bytes.extend_from_slice(&encode_ping(9));
        let mut decoder = Decoder::default();

        assert_eq!(
            decoder.push(&bytes),
            [
                Err(DecodeError::DiscardedGarbage { bytes: 16 }),
                Ok(Frame::Ping(PingFrame { sequence: 9 })),
            ]
        );
    }

    #[test]
    fn oversized_json_header_does_not_block_the_following_frame() {
        let mut bytes = vec![
            0xc0, 0x01, 0x00, 0x00, 0x00, 0x01, // reply header
            0x00, 0x00, 0x00, 0x09, // declared JSON length
        ];
        bytes.extend_from_slice(&encode_ping(10));
        let mut decoder = Decoder::new(8);
        let items = decoder.push(&bytes);

        assert_eq!(
            items.first(),
            Some(&Err(DecodeError::JsonTooLarge {
                declared: 9,
                maximum: 8,
            }))
        );
        assert_eq!(
            items.last(),
            Some(&Ok(Frame::Ping(PingFrame { sequence: 10 })))
        );
        assert_eq!(decoder.buffered_len(), 0);
    }

    #[test]
    fn malformed_headers_and_utf8_recover_to_the_next_frame() {
        let mut bytes = vec![
            0xe0, 0x04, // invalid command argc
            0xe0, 0x02, 0, 0, 0, 1, 0x02, 0x07, // invalid command version
            b'C', b'o', b'm', b'm', b'a', b'n', b'd',
        ];

        let mut invalid_utf8 = encode_reply(2, Some("x")).unwrap();
        *invalid_utf8.last_mut().unwrap() = 0xff;
        bytes.extend_from_slice(&invalid_utf8);
        bytes.extend_from_slice(&encode_ping(3));

        let mut decoder = Decoder::default();
        let items = decoder.push(&bytes);
        assert!(items.contains(&Err(DecodeError::InvalidArgumentCount {
            marker: COMMAND_MARKER,
            found: 4,
        })));
        assert!(items.contains(&Err(DecodeError::InvalidCommandPreamble)));
        assert!(items.contains(&Err(DecodeError::InvalidUtf8 {
            field: Utf8Field::Json,
        })));
        assert_eq!(
            items.last(),
            Some(&Ok(Frame::Ping(PingFrame { sequence: 3 })))
        );
        assert_eq!(decoder.buffered_len(), 0);
    }

    #[test]
    fn invalid_ping_argc_recovers_from_a_fragmented_next_frame() {
        let valid = encode_command(8, "Resume", None).unwrap();
        let mut decoder = Decoder::default();

        assert_eq!(
            decoder.push(&[0xe4, 0x01, 0xaa, 0xbb]),
            [
                Err(DecodeError::InvalidArgumentCount {
                    marker: PING_MARKER,
                    found: 1,
                }),
                Err(DecodeError::DiscardedGarbage { bytes: 3 }),
            ]
        );
        assert!(decoder.push(&valid[..3]).is_empty());
        assert_eq!(
            decoder.push(&valid[3..]),
            [Ok(Frame::Command(CommandFrame {
                sequence: 8,
                method: "Resume".into(),
                json: None,
            }))]
        );
    }

    #[test]
    fn encoder_rejects_methods_that_do_not_fit_the_u8_length() {
        assert_eq!(encode_command(1, "", None), Err(EncodeError::EmptyMethod));
        let long_method = "x".repeat(256);
        assert_eq!(
            encode_command(1, &long_method, None),
            Err(EncodeError::MethodTooLong {
                length: 256,
                maximum: 255,
            })
        );
    }

    #[test]
    fn a_large_garbage_chunk_never_remains_buffered() {
        let mut decoder = Decoder::new(16);
        let items = decoder.push(&vec![0x55; 4096]);
        assert!(!items.is_empty());
        assert!(
            items
                .iter()
                .all(|item| matches!(item, Err(DecodeError::DiscardedGarbage { .. })))
        );
        assert_eq!(decoder.buffered_len(), 0);
    }
}

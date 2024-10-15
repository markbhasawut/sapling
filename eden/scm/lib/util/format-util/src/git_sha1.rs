/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

use std::io;

use anyhow::ensure;
use anyhow::Context as _;
use anyhow::Result;

use crate::ByteCount;

/// Wrap `raw_text` in Git SHA1 format so the returned bytes have the SHA1 that
/// matches the Git object identity.
///
/// kind is "commit", "tree", or "blob".
pub fn git_sha1_serialize(raw_text: &[u8], kind: &str) -> Vec<u8> {
    let mut byte_count = ByteCount::default();
    git_sha1_serialize_write(raw_text, kind, &mut byte_count).unwrap();
    let mut result = Vec::with_capacity(byte_count.into());
    git_sha1_serialize_write(raw_text, kind, &mut result).unwrap();
    result
}

/// A more general purposed `git_sha1_serialize` to avoid copies.
/// The `write` function can write directly to a file, or update a SHA1 digest.
pub fn git_sha1_serialize_write(
    raw_text: &[u8],
    kind: &str,
    out: &mut dyn io::Write,
) -> Result<()> {
    let size = raw_text.len();
    out.write_all(kind.as_bytes())?;
    out.write_all(b" ")?;
    write!(out, "{}", size)?;
    out.write_all(b"\0")?;
    out.write_all(raw_text)?;
    Ok(())
}

/// The reverse of `git_sha1_serialize`.
/// Take `serialized` and return `raw_text` and `kind`.
pub fn git_sha1_deserialize<'a>(serialized: &'a [u8]) -> Result<(&'a [u8], &'a [u8])> {
    let (kind, rest) =
        split_once(serialized, b' ').context("invalid git object - no space separator")?;
    let (size_str, raw_text) =
        split_once(rest, 0).context("invalid git object - no NUL separator")?;
    let size: usize = std::str::from_utf8(size_str)?.parse()?;
    ensure!(size == raw_text.len(), "invalid git object - wrong size");
    Ok((raw_text, kind))
}

// slice::split_once is not yet stable
fn split_once(data: &[u8], sep: u8) -> Option<(&[u8], &[u8])> {
    let index = data.iter().position(|&b| b == sep)?;
    Some((&data[..index], &data[index + 1..]))
}

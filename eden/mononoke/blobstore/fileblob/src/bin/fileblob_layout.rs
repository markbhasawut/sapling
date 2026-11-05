/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

//! Moves a Fileblob's blob files between the flat and the sharded layout.
//!
//! Stop every process that uses the store first. Until the move is done,
//! Mononoke refuses to open the store; running this again finishes an
//! interrupted move.

use std::path::PathBuf;
use std::time::Instant;

use anyhow::Result;
use anyhow::bail;
use fileblob::Layout;

const USAGE: &str = "usage: fileblob_layout <blob directory> --to sharded|flat [--jobs N]

The blob directory is the one that holds the blob-* files, `blobs` under a
storage config's blob_files path.";

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let mut base = None;
    let mut to = None;
    let mut jobs = 8;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--to" => {
                to = Some(match args.next().as_deref() {
                    Some("sharded") => Layout::Sharded,
                    Some("flat") => Layout::Flat,
                    _ => bail!("{USAGE}"),
                })
            }
            "--jobs" => match args.next().and_then(|n| n.parse().ok()) {
                Some(n) if n > 0 => jobs = n,
                _ => bail!("{USAGE}"),
            },
            "-h" | "--help" => {
                println!("{USAGE}");
                return Ok(());
            }
            _ if base.is_none() && !arg.starts_with('-') => base = Some(PathBuf::from(arg)),
            _ => bail!("{USAGE}"),
        }
    }
    let (Some(base), Some(to)) = (base, to) else {
        bail!("{USAGE}");
    };
    // The storage config names the directory above it, and migrating that one
    // would leave the store as it is.
    if base.join("blobs").is_dir() {
        bail!(
            "{} holds a blobs directory: pass {}, which holds the blob-* files",
            base.display(),
            base.join("blobs").display(),
        );
    }

    let started = Instant::now();
    let stats = fileblob::migrate(&base, to, jobs, |stats| {
        eprint!("\r{} blobs moved in {:.0?}", stats.moved, started.elapsed());
    })?;
    eprintln!();
    println!(
        "{}: {to:?} now; moved {} blobs and deleted {} temporary files in {:.1?}",
        base.display(),
        stats.moved,
        stats.removed_temp_files,
        started.elapsed(),
    );
    Ok(())
}

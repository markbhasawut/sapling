/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

use std::collections::HashSet;
use std::ffi::OsString;
use std::fs::create_dir_all;
use std::io::Write;
use std::ops::RangeBounds;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::SystemTime;

use anyhow::Result;
use anyhow::bail;
use anyhow::format_err;
use async_trait::async_trait;
use blobstore::Blobstore;
use blobstore::BlobstoreEnumerationData;
use blobstore::BlobstoreGetData;
use blobstore::BlobstoreIsPresent;
use blobstore::BlobstoreKeyParam;
use blobstore::BlobstoreKeySource;
use blobstore::BlobstoreMetadata;
use blobstore::OverwriteStatus;
use blobstore::PutBehaviour;
use context::CoreContext;
use mononoke_macros::mononoke;
use mononoke_types::BlobstoreBytes;
use percent_encoding::AsciiSet;
use percent_encoding::CONTROLS;
use percent_encoding::percent_encode;
use tempfile::NamedTempFile;
use tempfile::PersistError;
use tokio::fs::File;
use tokio::fs::hard_link;
use tokio::fs::remove_file;
use tokio::io;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use walkdir::WalkDir;

const PREFIX: &str = "blob";
const PREFIX_HYPHEN: &str = "blob-";
// https://url.spec.whatwg.org/#fragment-percent-encode-set
const FRAGMENT: &AsciiSet = &CONTROLS.add(b' ').add(b'"').add(b'<').add(b'>').add(b'`');
// https://url.spec.whatwg.org/#path-percent-encode-set
const PATH: &AsciiSet = &FRAGMENT.add(b'#').add(b'?').add(b'{').add(b'}');

/// The file in a Fileblob's base directory that names its layout. Without it
/// the store is flat.
pub const LAYOUT_FILE: &str = "LAYOUT";
const LAYOUT_SHARDED: &str = "sharded-2x256";
/// Written while `migrate` moves blobs, so that nothing opens the store
/// half-moved.
const LAYOUT_MIGRATING: &str = "migrating";
/// Files `NamedTempFile` creates and an interrupted put leaves behind.
const TEMP_FILE_PREFIX: &str = ".tmp";

/// Where a Fileblob keeps its blob files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Layout {
    /// Every blob directly in the base directory, the default.
    Flat,
    /// Every blob two directory levels down, as `3f/a2/blob-<key>`, picked by
    /// a hash of its file name: 65,536 directories, so that none holds more
    /// than a few thousand files even with tens of millions of blobs. Lookups
    /// and renames in a directory of millions of files are many times slower.
    Sharded,
}

/// The layout of the Fileblob at `base`, from its layout file.
pub fn read_layout(base: &Path) -> Result<Layout> {
    layout_state(base)?.ok_or_else(|| {
        format_err!(
            "Fileblob {base:?} is being moved to another layout: run fileblob_layout again to finish"
        )
    })
}

/// The layout in the layout file at `base`, or None while `migrate` runs.
fn layout_state(base: &Path) -> Result<Option<Layout>> {
    match std::fs::read_to_string(base.join(LAYOUT_FILE)) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Some(Layout::Flat)),
        Err(e) => Err(e.into()),
        Ok(contents) => match contents.trim_end() {
            LAYOUT_SHARDED => Ok(Some(Layout::Sharded)),
            LAYOUT_MIGRATING => Ok(None),
            other => bail!("Fileblob {base:?} has an unknown layout {other:?} in {LAYOUT_FILE}"),
        },
    }
}

/// Replaces the layout file at `base` with `contents`, or removes it.
fn write_layout_file(base: &Path, contents: Option<&str>) -> Result<()> {
    let path = base.join(LAYOUT_FILE);
    match contents {
        None => match std::fs::remove_file(&path) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            removed => removed?,
        },
        Some(contents) => {
            let mut file = tempfile::Builder::new()
                .prefix(".LAYOUT")
                .tempfile_in(base)?;
            writeln!(file, "{contents}")?;
            file.as_file().sync_all()?;
            file.persist(&path)?;
        }
    }
    // Best effort: not every filesystem syncs a directory.
    let _ = std::fs::File::open(base).and_then(|dir| dir.sync_all());
    Ok(())
}

/// What `migrate` did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct MigrationStats {
    /// Blob files moved to the new layout.
    pub moved: u64,
    /// Temporary files of interrupted puts, deleted.
    pub removed_temp_files: u64,
}

#[derive(Default)]
struct Counters {
    moved: AtomicU64,
    removed_temp_files: AtomicU64,
}

impl Counters {
    fn stats(&self) -> MigrationStats {
        MigrationStats {
            moved: self.moved.load(Ordering::Relaxed),
            removed_temp_files: self.removed_temp_files.load(Ordering::Relaxed),
        }
    }
}

/// Names read from a directory before they are handed to the threads.
const MIGRATION_BATCH: usize = 10_000;

/// Moves the blob files of the Fileblob at `base` to layout `to` with `jobs`
/// threads, deleting the temporary files of interrupted puts on the way, and
/// calls `progress` after each batch. Stop every process that has the store
/// open first: from the start of the move to its end `Fileblob::open` refuses
/// the store, and running `migrate` again finishes an interrupted move.
pub fn migrate(
    base: &Path,
    to: Layout,
    jobs: usize,
    progress: impl Fn(&MigrationStats) + Sync,
) -> Result<MigrationStats> {
    if !base.is_dir() {
        bail!("Base {base:?} doesn't exist or is not directory");
    }
    if layout_state(base)? == Some(to) {
        return Ok(MigrationStats::default());
    }
    write_layout_file(base, Some(LAYOUT_MIGRATING))?;

    // Either direction also finishes a move interrupted in the other one.
    let counters = Counters::default();
    match to {
        Layout::Sharded => shard(base, jobs, &counters, &progress)?,
        Layout::Flat => flatten(base, jobs, &counters, &progress)?,
    }

    // The renames reach the disk before the layout file says they are done.
    // SAFETY: sync(2) takes no arguments and cannot fail.
    unsafe { libc::sync() };
    write_layout_file(
        base,
        match to {
            Layout::Sharded => Some(LAYOUT_SHARDED),
            Layout::Flat => None,
        },
    )?;
    Ok(counters.stats())
}

/// Moves the blob files directly in `base` into their shard directories.
fn shard(
    base: &Path,
    jobs: usize,
    counters: &Counters,
    progress: &(impl Fn(&MigrationStats) + Sync),
) -> Result<()> {
    let created = Mutex::new(HashSet::new());
    let move_one = |name: &OsString| -> Result<()> {
        let Some(name) = name.to_str() else {
            return Ok(());
        };
        let path = base.join(name);
        if name.starts_with(PREFIX_HYPHEN) {
            let target = sharded_path(base, name);
            let dir = target.parent().unwrap_or(base);
            if !created.lock().expect("lock poisoned").contains(dir) {
                create_dir_all(dir)?;
                created.lock().expect("lock poisoned").insert(dir.to_owned());
            }
            match std::fs::rename(&path, &target) {
                // Listed twice by a directory that changed while being read.
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                renamed => {
                    renamed?;
                    counters.moved.fetch_add(1, Ordering::Relaxed);
                }
            }
        } else if name.starts_with(TEMP_FILE_PREFIX) {
            remove_temp_file(&path, counters)?;
        }
        Ok(())
    };
    // A directory read while files leave it may skip some, so read it again
    // until a pass finds nothing to move.
    loop {
        let before = counters.stats();
        let mut batch = Vec::with_capacity(MIGRATION_BATCH);
        for entry in std::fs::read_dir(base)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                batch.push(entry.file_name());
            }
            if batch.len() == MIGRATION_BATCH {
                in_parallel(&batch, jobs, &move_one)?;
                batch.clear();
                progress(&counters.stats());
            }
        }
        in_parallel(&batch, jobs, &move_one)?;
        progress(&counters.stats());
        if counters.stats() == before {
            return Ok(());
        }
    }
}

/// Moves the blob files of every shard directory of `base` up into `base`,
/// and removes the directories they leave empty.
fn flatten(
    base: &Path,
    jobs: usize,
    counters: &Counters,
    progress: &(impl Fn(&MigrationStats) + Sync),
) -> Result<()> {
    let tops = shard_dirs(base)?;
    let mut leaves = Vec::new();
    for top in &tops {
        leaves.extend(shard_dirs(top)?);
    }
    let flatten_leaf = |leaf: &PathBuf| -> Result<()> {
        // As in shard, read the directory again until a pass moves nothing.
        loop {
            let mut changed = false;
            for entry in std::fs::read_dir(leaf)? {
                let entry = entry?;
                let name = entry.file_name();
                let Some(name) = name.to_str() else {
                    continue;
                };
                if !entry.file_type()?.is_file() {
                    continue;
                }
                if name.starts_with(PREFIX_HYPHEN) {
                    match std::fs::rename(entry.path(), base.join(name)) {
                        // Listed twice by a directory that changed while being read.
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                        renamed => {
                            renamed?;
                            counters.moved.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    changed = true;
                } else if name.starts_with(TEMP_FILE_PREFIX) {
                    remove_temp_file(&entry.path(), counters)?;
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }
        // Left in place if it still holds files that are not blobs.
        let _ = std::fs::remove_dir(leaf);
        Ok(())
    };
    for batch in leaves.chunks(MIGRATION_BATCH / 100) {
        in_parallel(batch, jobs, &flatten_leaf)?;
        progress(&counters.stats());
    }
    // A flat store cannot see a blob left in a shard directory, so the layout
    // file keeps saying "migrating" until none is.
    for leaf in &leaves {
        let entries = match std::fs::read_dir(leaf) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            entries => entries?,
        };
        for entry in entries {
            let name = entry?.file_name();
            if name.to_str().is_some_and(|name| name.starts_with(PREFIX_HYPHEN)) {
                bail!("{leaf:?} still holds blob {name:?}: run fileblob_layout again to finish");
            }
        }
    }
    for top in &tops {
        let _ = std::fs::remove_dir(top);
    }
    Ok(())
}

/// The subdirectories of `dir` named like shard directories, two hex digits.
fn shard_dirs(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut dirs = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let is_shard_name = entry.file_name().to_str().is_some_and(|name| {
            name.len() == 2 && name.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
        });
        if is_shard_name && entry.file_type()?.is_dir() {
            dirs.push(entry.path());
        }
    }
    Ok(dirs)
}

fn remove_temp_file(path: &Path, counters: &Counters) -> Result<()> {
    match std::fs::remove_file(path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        removed => {
            removed?;
            counters.removed_temp_files.fetch_add(1, Ordering::Relaxed);
        }
    }
    Ok(())
}

/// Runs `f` on every item, spread over `jobs` threads; stops at an error.
fn in_parallel<T: Sync>(items: &[T], jobs: usize, f: &(impl Fn(&T) -> Result<()> + Sync)) -> Result<()> {
    if items.is_empty() {
        return Ok(());
    }
    let chunk = items.len().div_ceil(jobs.max(1));
    std::thread::scope(|scope| {
        let threads: Vec<_> = items
            .chunks(chunk)
            .map(|part| scope.spawn(move || part.iter().try_for_each(f)))
            .collect();
        threads
            .into_iter()
            .try_for_each(|thread| thread.join().expect("a migration thread panicked"))
    })
}

/// The path of the blob file `name` in a sharded store at `base`.
pub fn sharded_path(base: &Path, name: &str) -> PathBuf {
    let hash = fnv1a(name.as_bytes());
    base.join(format!("{:02x}", hash >> 56))
        .join(format!("{:02x}", (hash >> 48) & 0xff))
        .join(name)
}

/// 64-bit FNV-1a. Its result is fixed by its definition, unlike that of std's
/// hashers, so a blob stays in the same directory across builds.
fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

#[derive(Debug, Clone)]
pub struct Fileblob {
    base: PathBuf,
    layout: Layout,
    put_behaviour: PutBehaviour,
}

impl Fileblob {
    pub fn open<P: AsRef<Path>>(base: P, put_behaviour: PutBehaviour) -> Result<Self> {
        let base = base.as_ref();

        if !base.is_dir() {
            bail!("Base {base:?} doesn't exist or is not directory");
        }

        Ok(Self {
            base: base.to_owned(),
            layout: read_layout(base)?,
            put_behaviour,
        })
    }

    pub fn create<P: AsRef<Path>>(base: P, put_behaviour: PutBehaviour) -> Result<Self> {
        let base = base.as_ref();
        create_dir_all(base)?;
        Self::open(base, put_behaviour)
    }

    fn path(&self, key: &str) -> PathBuf {
        let key = percent_encode(key.as_bytes(), PATH);
        let name = format!("{PREFIX}-{key}");
        match self.layout {
            Layout::Flat => self.base.join(name),
            Layout::Sharded => sharded_path(&self.base, &name),
        }
    }

    /// Stripping the prepended prefix (if its exists) before returning
    /// keys back to the caller. Safe to call with or without the prefix.
    fn strip_file_prefix<'a>(&self, key: &'a str) -> &'a str {
        match key.strip_prefix(PREFIX_HYPHEN) {
            // Remove the prefix if present and return the remaining
            // slice.
            Some(key_without_prefix) => key_without_prefix,
            // If not present, return the key as-is.
            None => key,
        }
    }
}

impl std::fmt::Display for Fileblob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Fileblob")
    }
}

async fn ctime(file: &File) -> Option<i64> {
    let meta = file.metadata().await.ok()?;
    let ctime = meta.modified().ok()?;
    let ctime_dur = ctime.duration_since(SystemTime::UNIX_EPOCH).ok()?;
    i64::try_from(ctime_dur.as_secs()).ok()
}

/// Flush a blob's contents to disk before it is renamed into place.
///
/// On Apple platforms `sync_all()` is `fcntl(F_FULLFSYNC)`, which also empties
/// the drive's write cache and costs milliseconds per blob. Use fsync(2) there,
/// which is what `sync_all()` does on Linux.
async fn sync_to_disk(file: File) -> io::Result<()> {
    #[cfg(target_vendor = "apple")]
    {
        use std::os::fd::AsRawFd;

        let file = file.into_std().await;
        mononoke::spawn_blocking(move || {
            // SAFETY: `file` keeps the descriptor open until fsync returns.
            if unsafe { libc::fsync(file.as_raw_fd()) } == 0 {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        })
        .await?
    }
    #[cfg(not(target_vendor = "apple"))]
    {
        file.sync_all().await
    }
}

#[async_trait]
impl Blobstore for Fileblob {
    async fn get<'a>(
        &'a self,
        _ctx: &'a CoreContext,
        key: &'a str,
    ) -> Result<Option<BlobstoreGetData>> {
        let p = self.path(key);

        let ret = match File::open(&p).await {
            Err(ref r) if r.kind() == io::ErrorKind::NotFound => None,
            Err(e) => return Err(e.into()),
            Ok(mut f) => {
                let mut v = Vec::new();
                f.read_to_end(&mut v).await?;

                Some(BlobstoreGetData::new(
                    BlobstoreMetadata::new(ctime(&f).await, None),
                    BlobstoreBytes::from_bytes(v),
                ))
            }
        };
        Ok(ret)
    }

    async fn put_explicit<'a>(
        &'a self,
        _ctx: &'a CoreContext,
        key: String,
        value: BlobstoreBytes,
        put_behaviour: PutBehaviour,
    ) -> Result<OverwriteStatus> {
        let p = self.path(&key);
        // In the blob's own directory, so that a rename never crosses
        // directories, and a sharded store's base directory stays small.
        let dir = p.parent().unwrap_or(&self.base);
        let tempfile = match NamedTempFile::new_in(dir) {
            Err(e) if e.kind() == io::ErrorKind::NotFound && self.layout == Layout::Sharded => {
                create_dir_all(dir)?;
                NamedTempFile::new_in(dir)?
            }
            tempfile => tempfile?,
        };
        let new_file = tempfile.as_file().try_clone()?;
        let mut tokio_file = File::from_std(new_file);
        tokio_file.write_all(value.as_bytes().as_ref()).await?;
        tokio_file.flush().await?;
        sync_to_disk(tokio_file).await?;
        let status = match put_behaviour {
            PutBehaviour::Overwrite => {
                tempfile.persist(&p)?;
                OverwriteStatus::NotChecked
            }
            PutBehaviour::IfAbsent | PutBehaviour::OverwriteAndLog => {
                let temp_path = tempfile.path().to_owned();
                match tempfile.persist_noclobber(&p) {
                    Ok(_) => OverwriteStatus::New,
                    // Key already existed
                    Err(PersistError { file: f, error: e })
                        if f.path() == temp_path
                            && e.kind() == std::io::ErrorKind::AlreadyExists =>
                    {
                        if put_behaviour.should_overwrite() {
                            f.persist(&p)?;
                            OverwriteStatus::Overwrote
                        } else {
                            OverwriteStatus::Prevented
                        }
                    }
                    Err(e) => return Err(e.into()),
                }
            }
        };

        Ok(status)
    }

    async fn put_with_status<'a>(
        &'a self,
        ctx: &'a CoreContext,
        key: String,
        value: BlobstoreBytes,
    ) -> Result<OverwriteStatus> {
        self.put_explicit(ctx, key, value, self.put_behaviour).await
    }

    async fn is_present<'a>(
        &'a self,
        _ctx: &'a CoreContext,
        key: &'a str,
    ) -> Result<BlobstoreIsPresent> {
        let p = self.path(key);

        let present = match File::open(&p).await {
            Err(ref e) if e.kind() == io::ErrorKind::NotFound => false,
            Err(e) => return Err(e.into()),
            Ok(_) => true,
        };
        Ok(if present {
            BlobstoreIsPresent::Present
        } else {
            BlobstoreIsPresent::Absent
        })
    }

    // This uses hardlink semantics as the production blobstores also have hardlink like semantics
    // (i.e. you can't discover a canonical link source when loading by the target)
    async fn copy<'a>(
        &'a self,
        _ctx: &'a CoreContext,
        old_key: &'a str,
        new_key: String,
    ) -> Result<()> {
        // from std::fs::hard_link: The dst path will be a link pointing to the src path
        let src_path = self.path(old_key);
        let dst_path = self.path(&new_key);
        let dst_dir = match self.layout {
            Layout::Flat => None,
            Layout::Sharded => dst_path.parent().map(Path::to_owned),
        };
        // hard_link will fail if dst_path exists. Race it in a task of its own
        Ok(mononoke::spawn_task(async move {
            if let Some(dst_dir) = dst_dir {
                tokio::fs::create_dir_all(dst_dir).await?;
            }
            let _ = remove_file(&dst_path).await;
            hard_link(src_path, dst_path).await
        })
        .await??)
    }

    async fn unlink<'a>(&'a self, _ctx: &'a CoreContext, key: &'a str) -> Result<()> {
        let path = self.path(key);
        Ok(remove_file(path).await?)
    }
}

#[async_trait]
impl BlobstoreKeySource for Fileblob {
    async fn enumerate<'a>(
        &'a self,
        _ctx: &'a CoreContext,
        range: &'a BlobstoreKeyParam,
    ) -> Result<BlobstoreEnumerationData> {
        match range {
            BlobstoreKeyParam::Start(range) => {
                let mut enum_data = BlobstoreEnumerationData {
                    keys: HashSet::new(),
                    next_token: None,
                };
                WalkDir::new(&self.base)
                    .into_iter()
                    .filter_map(|v| v.ok())
                    // Not the directories of a sharded store, or the layout file.
                    .filter(|entry| entry.file_type().is_file() && entry.file_name() != LAYOUT_FILE)
                    .for_each(|entry| {
                        // Need the filename not the directory, since the directory
                        // structure is not exposed to the caller.
                        let entry = entry.file_name().to_str();
                        if let Some(data) = entry {
                            let key = self.strip_file_prefix(data).to_string();
                            if range.contains(&key) {
                                enum_data.keys.insert(key);
                            }
                        }
                    });
                Ok(enum_data)
            }
            _ => Err(format_err!("Fileblob does not support token, only ranges")),
        }
    }
}

#[cfg(test)]
mod test {
    use fbinit::FacebookInit;
    use mononoke_macros::mononoke;

    use super::*;

    #[mononoke::fbinit_test]
    async fn test_persist_error(fb: FacebookInit) -> Result<()> {
        let ctx = CoreContext::test_mock(fb);

        let blob = Fileblob {
            base: PathBuf::from("/mononoke/fileblob/test/path/should/not/exist"),
            layout: Layout::Flat,
            put_behaviour: PutBehaviour::IfAbsent,
        };

        let ret = blob
            .put(&ctx, "key".into(), BlobstoreBytes::from_bytes("value"))
            .await;

        assert!(ret.is_err());

        Ok(())
    }

    /// The files under `dir`, as sorted paths relative to it.
    fn files_under(dir: &Path) -> Vec<String> {
        let mut files: Vec<_> = WalkDir::new(dir)
            .into_iter()
            .map(|entry| entry.unwrap())
            .filter(|entry| entry.file_type().is_file())
            .map(|entry| {
                let path = entry.path().strip_prefix(dir).unwrap();
                path.to_str().unwrap().to_owned()
            })
            .collect();
        files.sort();
        files
    }

    async fn get_string(ctx: &CoreContext, blob: &Fileblob, key: &str) -> Result<Option<String>> {
        Ok(blob.get(ctx, key).await?.map(|data| {
            String::from_utf8(data.as_raw_bytes().to_vec()).unwrap()
        }))
    }

    async fn all_keys(ctx: &CoreContext, blob: &Fileblob) -> Result<Vec<String>> {
        let range = BlobstoreKeyParam::from("".to_owned()..="~".to_owned());
        let mut keys: Vec<_> = blob.enumerate(ctx, &range).await?.keys.into_iter().collect();
        keys.sort();
        Ok(keys)
    }

    #[mononoke::fbinit_test]
    async fn test_sharded_store(fb: FacebookInit) -> Result<()> {
        let ctx = CoreContext::test_mock(fb);
        let dir = tempfile::tempdir()?;
        migrate(dir.path(), Layout::Sharded, 2, |_| {})?;
        assert_eq!(read_layout(dir.path())?, Layout::Sharded);
        let blob = Fileblob::open(dir.path(), PutBehaviour::IfAbsent)?;

        let key = "repo0001.content.blake2.aa";
        let status = blob
            .put_with_status(&ctx, key.to_owned(), BlobstoreBytes::from_bytes("one"))
            .await?;
        assert_eq!(status, OverwriteStatus::New);
        let status = blob
            .put_with_status(&ctx, key.to_owned(), BlobstoreBytes::from_bytes("two"))
            .await?;
        assert_eq!(status, OverwriteStatus::Prevented);

        // Two levels of two hex digits, and no temporary file left behind.
        let path = sharded_path(dir.path(), "blob-repo0001.content.blake2.aa");
        let relative = path.strip_prefix(dir.path())?.to_str().unwrap().to_owned();
        assert_eq!(relative.len(), "3f/a2/".len() + "blob-".len() + key.len());
        let mut expected = vec![LAYOUT_FILE.to_owned(), relative];
        expected.sort();
        assert_eq!(files_under(dir.path()), expected);
        assert_eq!(get_string(&ctx, &blob, key).await?, Some("one".to_owned()));

        blob.copy(&ctx, key, "repo0001.alias.sha1.bb".to_owned()).await?;
        assert_eq!(
            get_string(&ctx, &blob, "repo0001.alias.sha1.bb").await?,
            Some("one".to_owned())
        );
        assert!(matches!(
            blob.is_present(&ctx, "repo0001.alias.sha1.bb").await?,
            BlobstoreIsPresent::Present
        ));
        // Only blob keys: not the shard directories or the layout file.
        assert_eq!(
            all_keys(&ctx, &blob).await?,
            vec!["repo0001.alias.sha1.bb", "repo0001.content.blake2.aa"]
        );

        blob.unlink(&ctx, key).await?;
        assert_eq!(get_string(&ctx, &blob, key).await?, None);
        assert!(matches!(
            blob.is_present(&ctx, key).await?,
            BlobstoreIsPresent::Absent
        ));
        Ok(())
    }

    #[mononoke::fbinit_test]
    async fn test_migrate_both_ways(fb: FacebookInit) -> Result<()> {
        let ctx = CoreContext::test_mock(fb);
        let dir = tempfile::tempdir()?;
        let keys: Vec<_> = (0..1000)
            .map(|i| format!("repo0002.content.blake2.{i:064x}"))
            .collect();
        let flat = Fileblob::create(dir.path(), PutBehaviour::Overwrite)?;
        for key in &keys {
            flat.put(&ctx, key.clone(), BlobstoreBytes::from_bytes(key.clone()))
                .await?;
        }
        // An interrupted put's temporary file.
        std::fs::write(dir.path().join(".tmpAbCdEf"), "partial")?;
        let flat_files = files_under(dir.path());

        let stats = migrate(dir.path(), Layout::Sharded, 4, |_| {})?;
        assert_eq!(
            stats,
            MigrationStats {
                moved: 1000,
                removed_temp_files: 1,
            }
        );
        // Nothing is left directly in the base directory but the layout file.
        let sharded_files = files_under(dir.path());
        assert!(sharded_files.iter().all(|f| f == LAYOUT_FILE || f.len() > 6 && &f[2..3] == "/" && &f[5..6] == "/"));
        let sharded = Fileblob::open(dir.path(), PutBehaviour::Overwrite)?;
        for key in &keys {
            assert_eq!(get_string(&ctx, &sharded, key).await?.as_ref(), Some(key));
        }
        assert_eq!(all_keys(&ctx, &sharded).await?, keys);
        // Already sharded: nothing to do.
        assert_eq!(
            migrate(dir.path(), Layout::Sharded, 4, |_| {})?,
            MigrationStats::default()
        );

        let stats = migrate(dir.path(), Layout::Flat, 4, |_| {})?;
        assert_eq!(stats.moved, 1000);
        let mut expected = flat_files;
        expected.retain(|f| f != ".tmpAbCdEf");
        // Back to the files of a flat store, with every shard directory gone.
        assert_eq!(files_under(dir.path()), expected);
        assert_eq!(std::fs::read_dir(dir.path())?.count(), 1000);
        let flat = Fileblob::open(dir.path(), PutBehaviour::Overwrite)?;
        for key in &keys {
            assert_eq!(get_string(&ctx, &flat, key).await?.as_ref(), Some(key));
        }
        Ok(())
    }

    #[mononoke::fbinit_test]
    async fn test_interrupted_migration(fb: FacebookInit) -> Result<()> {
        let ctx = CoreContext::test_mock(fb);
        let dir = tempfile::tempdir()?;
        let flat = Fileblob::create(dir.path(), PutBehaviour::Overwrite)?;
        for i in 0..10 {
            flat.put(&ctx, format!("key{i}"), BlobstoreBytes::from_bytes("x"))
                .await?;
        }
        // As a move to the sharded layout stopped halfway leaves it.
        write_layout_file(dir.path(), Some(LAYOUT_MIGRATING))?;
        for i in 0..5 {
            let name = format!("blob-key{i}");
            let target = sharded_path(dir.path(), &name);
            create_dir_all(target.parent().unwrap())?;
            std::fs::rename(dir.path().join(&name), target)?;
        }

        let error = Fileblob::open(dir.path(), PutBehaviour::Overwrite).unwrap_err();
        assert!(error.to_string().contains("run fileblob_layout again"), "{error}");

        let stats = migrate(dir.path(), Layout::Sharded, 2, |_| {})?;
        assert_eq!(stats.moved, 5);
        let sharded = Fileblob::open(dir.path(), PutBehaviour::Overwrite)?;
        for i in 0..10 {
            assert_eq!(
                get_string(&ctx, &sharded, &format!("key{i}")).await?,
                Some("x".to_owned())
            );
        }
        Ok(())
    }

    #[test]
    fn test_unknown_layout() -> Result<()> {
        let dir = tempfile::tempdir()?;
        std::fs::write(dir.path().join(LAYOUT_FILE), "sharded-3x16\n")?;
        let error = Fileblob::open(dir.path(), PutBehaviour::Overwrite).unwrap_err();
        assert!(error.to_string().contains("unknown layout"), "{error}");
        assert!(migrate(dir.path(), Layout::Flat, 1, |_| {}).is_err());
        Ok(())
    }

    #[test]
    fn test_shards_are_stable() {
        // A blob must never change directories between builds.
        assert_eq!(
            sharded_path(Path::new("/b"), "blob-repo0000.changeset.blake2.0"),
            Path::new("/b/c3/00/blob-repo0000.changeset.blake2.0")
        );
        // FNV-1a's published test vectors.
        assert_eq!(fnv1a(b""), 0xcbf29ce484222325);
        assert_eq!(fnv1a(b"a"), 0xaf63dc4c8601ec8c);
        assert_eq!(fnv1a(b"foobar"), 0x85944171f73967e8);
    }
}

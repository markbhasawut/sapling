/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

//! The candidate servers of a cluster, with live reload of the hosts file.
//!
//! The candidates come from the hosts file when one is configured and it
//! holds a valid, non-empty list; otherwise from the configured `hosts`, or
//! the legacy `host:port`. The prober calls [`CandidateList::refresh`] at the
//! start of every round: the file is read again when its modification time
//! or size changed, and when it cannot be read or is invalid the last good
//! list is kept, with a rate-limited warning. The file never makes cluster
//! creation fail.
//!
//! Every server listed gets the MySQL password, so a warning is logged, once
//! per process, when users other than the owner can write the file.

use std::fs;
use std::io;
use std::io::Read;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use std::time::SystemTime;

use anyhow::Result;
use tokio::time::Instant;

use super::decide::Candidate;
use super::endpoint::parse_oss_mysql_hosts_file;
use super::secrets::warn_once;

/// Minimum time between two warnings about a hosts file that cannot be used.
pub const HOSTS_FILE_WARNING_INTERVAL: Duration = Duration::from_secs(60);

/// What tells versions of the hosts file apart.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileStamp {
    /// The modification time, when the platform has one.
    pub modified: Option<SystemTime>,
    /// The size in bytes.
    pub len: u64,
}

impl FileStamp {
    pub fn of(metadata: &fs::Metadata) -> Self {
        Self {
            modified: metadata.modified().ok(),
            len: metadata.len(),
        }
    }
}

/// What a refresh did.
#[derive(Debug, PartialEq, Eq)]
pub enum Refresh {
    /// The candidates are the same.
    Unchanged,
    /// The hosts file gave new candidates.
    Changed,
    /// The hosts file cannot be used, so the last good candidates are kept.
    /// `warning` is `None` while warnings are rate-limited.
    KeptLastGood { warning: Option<String> },
}

/// The contents of the hosts file.
struct FileContents {
    text: String,
    /// The permission bits of the file read, when the platform has them.
    mode: Option<u32>,
}

/// Access to the hosts file. Tests replace it.
trait FileAccess {
    fn stamp(&self, path: &Path) -> io::Result<FileStamp>;
    fn read(&self, path: &Path) -> io::Result<FileContents>;
}

struct RealFiles;

impl FileAccess for RealFiles {
    fn stamp(&self, path: &Path) -> io::Result<FileStamp> {
        Ok(FileStamp::of(&fs::metadata(path)?))
    }

    fn read(&self, path: &Path) -> io::Result<FileContents> {
        let mut file = fs::File::open(path)?;
        #[cfg(unix)]
        let mode = {
            use std::os::unix::fs::PermissionsExt;

            Some(file.metadata()?.permissions().mode() & 0o777)
        };
        #[cfg(not(unix))]
        let mode = None;
        let mut text = String::new();
        file.read_to_string(&mut text)?;
        Ok(FileContents { text, mode })
    }
}

/// The current candidates of a cluster.
#[derive(Debug)]
pub struct CandidateList {
    hosts_file: Option<PathBuf>,
    default_port: u16,
    candidates: Vec<Candidate>,
    /// The version of the hosts file `candidates` were read from.
    loaded: Option<FileStamp>,
    /// When the last warning was given.
    last_warning: Option<Instant>,
    /// A warning that users other than the owner can write the hosts file,
    /// from its last read, for `new` and `refresh` to log.
    access_warning: Option<String>,
}

impl CandidateList {
    /// Reads `hosts_file`, if any, and uses `fallback` (the configured
    /// `hosts`, or `host:port`) when there is none or it cannot be used.
    /// Hosts file entries without a port use `default_port`.
    pub fn new(
        hosts_file: Option<PathBuf>,
        default_port: u16,
        fallback: Vec<Candidate>,
        now: Instant,
    ) -> Self {
        let (mut list, warning) =
            Self::new_with(&RealFiles, hosts_file, default_port, fallback, now);
        list.log_access_warning();
        if let Some(warning) = warning {
            tracing::warn!("{warning}");
        }
        list
    }

    /// The candidates, in order.
    pub fn candidates(&self) -> &[Candidate] {
        &self.candidates
    }

    /// The hosts file, if one is configured.
    pub fn hosts_file(&self) -> Option<&Path> {
        self.hosts_file.as_deref()
    }

    /// Reads the hosts file again if it changed. Logs changes and warnings.
    pub fn refresh(&mut self, now: Instant) -> Refresh {
        let refresh = self.refresh_with(&RealFiles, now);
        self.log_access_warning();
        match &refresh {
            Refresh::Changed => tracing::info!(
                "MySQL hosts file {} changed; candidates: {}",
                self.hosts_file
                    .as_deref()
                    .unwrap_or(Path::new(""))
                    .display(),
                list(&self.candidates),
            ),
            Refresh::KeptLastGood {
                warning: Some(warning),
            } => tracing::warn!("{warning}"),
            Refresh::KeptLastGood { warning: None } | Refresh::Unchanged => {}
        }
        refresh
    }

    /// Logs the access warning of the last read, once per process.
    fn log_access_warning(&mut self) {
        if let Some(warning) = self.access_warning.take() {
            warn_once(warning);
        }
    }

    fn new_with(
        files: &impl FileAccess,
        hosts_file: Option<PathBuf>,
        default_port: u16,
        fallback: Vec<Candidate>,
        now: Instant,
    ) -> (Self, Option<String>) {
        let mut list = Self {
            hosts_file,
            default_port,
            candidates: fallback,
            loaded: None,
            last_warning: None,
            access_warning: None,
        };
        let warning = match list.refresh_with(files, now) {
            Refresh::KeptLastGood { warning } => warning,
            Refresh::Changed | Refresh::Unchanged => None,
        };
        (list, warning)
    }

    fn refresh_with(&mut self, files: &impl FileAccess, now: Instant) -> Refresh {
        let Some(path) = self.hosts_file.clone() else {
            return Refresh::Unchanged;
        };
        match self.load(files, &path) {
            Ok(None) => Refresh::Unchanged,
            Ok(Some((stamp, candidates))) => {
                self.loaded = Some(stamp);
                // Warn at once the next time the file goes bad.
                self.last_warning = None;
                if candidates == self.candidates {
                    Refresh::Unchanged
                } else {
                    self.candidates = candidates;
                    Refresh::Changed
                }
            }
            Err(err) => {
                let warn = self.last_warning.is_none_or(|last| {
                    now.saturating_duration_since(last) >= HOSTS_FILE_WARNING_INTERVAL
                });
                let warning = warn.then(|| {
                    self.last_warning = Some(now);
                    format!(
                        "cannot use MySQL hosts file {}: {err:#}; using {}",
                        path.display(),
                        list(&self.candidates),
                    )
                });
                Refresh::KeptLastGood { warning }
            }
        }
    }

    /// The new version of the hosts file and its candidates, or `None` if
    /// the version in use did not change. Sets the access warning of the
    /// version read, even when it is invalid.
    fn load(
        &mut self,
        files: &impl FileAccess,
        path: &Path,
    ) -> Result<Option<(FileStamp, Vec<Candidate>)>> {
        let stamp = files.stamp(path)?;
        if self.loaded == Some(stamp) {
            return Ok(None);
        }
        let contents = files.read(path)?;
        self.access_warning = contents.mode.and_then(|mode| access_warning(path, mode));
        let candidates = parse_oss_mysql_hosts_file(&contents.text, self.default_port)?
            .into_iter()
            .map(Candidate::from)
            .collect();
        Ok(Some((stamp, candidates)))
    }
}

/// A warning when users other than the owner can write the hosts file with
/// permission bits `mode`: whoever can add a line to it can add a server
/// that the prober sends the password to.
fn access_warning(path: &Path, mode: u32) -> Option<String> {
    (mode & 0o022 != 0).then(|| {
        format!(
            "MySQL hosts file {} is writable by other users (mode {mode:03o}), who could \
             add a server that receives the MySQL password; restrict it with chmod go-w",
            path.display()
        )
    })
}

fn list(candidates: &[Candidate]) -> String {
    candidates
        .iter()
        .map(|c| c.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::cell::RefCell;

    use mononoke_macros::mononoke;

    use super::*;

    const PATH: &str = "/etc/mononoke/mysql-hosts";

    /// A hosts file in memory. `None` contents mean the file is missing.
    struct FakeFile {
        contents: RefCell<Option<String>>,
        modified: Cell<u64>,
        mode: Cell<u32>,
        unreadable: Cell<bool>,
        reads: Cell<usize>,
    }

    impl FakeFile {
        fn new(contents: Option<&str>) -> Self {
            Self {
                contents: RefCell::new(contents.map(str::to_string)),
                modified: Cell::new(1),
                mode: Cell::new(0o644),
                unreadable: Cell::new(false),
                reads: Cell::new(0),
            }
        }

        /// Replaces the contents and bumps the modification time.
        fn write(&self, contents: Option<&str>) {
            *self.contents.borrow_mut() = contents.map(str::to_string);
            self.touch();
        }

        fn touch(&self) {
            self.modified.set(self.modified.get() + 1);
        }
    }

    impl FileAccess for FakeFile {
        fn stamp(&self, path: &Path) -> io::Result<FileStamp> {
            assert_eq!(path, Path::new(PATH));
            match &*self.contents.borrow() {
                Some(contents) => Ok(FileStamp {
                    modified: Some(
                        SystemTime::UNIX_EPOCH + Duration::from_secs(self.modified.get()),
                    ),
                    len: contents.len() as u64,
                }),
                None => Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "No such file or directory",
                )),
            }
        }

        fn read(&self, path: &Path) -> io::Result<FileContents> {
            assert_eq!(path, Path::new(PATH));
            self.reads.set(self.reads.get() + 1);
            if self.unreadable.get() {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "Permission denied",
                ));
            }
            let text = self
                .contents
                .borrow()
                .clone()
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "No such file"))?;
            Ok(FileContents {
                text,
                mode: Some(self.mode.get()),
            })
        }
    }

    fn candidates(list: &[(&str, u16)]) -> Vec<Candidate> {
        list.iter()
            .map(|(host, port)| Candidate::new(*host, *port))
            .collect()
    }

    fn fallback() -> Vec<Candidate> {
        candidates(&[("127.0.0.1", 13306)])
    }

    fn create(file: &FakeFile, now: Instant) -> (CandidateList, Option<String>) {
        CandidateList::new_with(file, Some(PathBuf::from(PATH)), 3306, fallback(), now)
    }

    fn warning(refresh: Refresh) -> Option<String> {
        match refresh {
            Refresh::KeptLastGood { warning } => warning,
            other => panic!("expected the last good list to be kept, got {other:?}"),
        }
    }

    #[mononoke::test]
    fn test_no_hosts_file() {
        let file = FakeFile::new(Some("db1\n"));
        let now = Instant::now();
        let (mut list, warning) = CandidateList::new_with(&file, None, 3306, fallback(), now);
        assert_eq!(warning, None);
        assert_eq!(list.candidates(), fallback());
        assert_eq!(list.hosts_file(), None);
        assert_eq!(list.refresh_with(&file, now), Refresh::Unchanged);
        assert_eq!(file.reads.get(), 0);
    }

    #[mononoke::test]
    fn test_hosts_file_takes_precedence() {
        let file = FakeFile::new(Some(
            "# primary first\n127.0.0.1:13307\n127.0.0.1:13306\ndb3\n",
        ));
        let (list, warning) = create(&file, Instant::now());
        assert_eq!(warning, None);
        assert_eq!(
            list.candidates(),
            candidates(&[("127.0.0.1", 13307), ("127.0.0.1", 13306), ("db3", 3306)])
        );
        assert_eq!(list.hosts_file(), Some(Path::new(PATH)));
    }

    #[mononoke::test]
    fn test_unusable_file_at_creation() {
        for (contents, error) in [
            (None, "No such file or directory"),
            (Some(""), "no servers listed"),
            (Some("# nothing yet\n"), "no servers listed"),
            (Some("db1\ndb2:0\n"), "line 2: invalid entry 'db2:0'"),
            (Some("db1\ndb1\n"), "line 2: duplicate entry 'db1'"),
        ] {
            let file = FakeFile::new(contents);
            let (list, warning) = create(&file, Instant::now());
            assert_eq!(list.candidates(), fallback(), "{contents:?}");
            let warning = warning.unwrap_or_else(|| panic!("{contents:?}: no warning"));
            assert!(
                warning.starts_with(&format!("cannot use MySQL hosts file {PATH}: ")),
                "{warning}"
            );
            assert!(warning.contains(error), "{warning}");
            assert!(warning.ends_with("; using 127.0.0.1:13306"), "{warning}");
        }

        let file = FakeFile::new(Some("db1\n"));
        file.unreadable.set(true);
        let (list, warning) = create(&file, Instant::now());
        assert_eq!(list.candidates(), fallback());
        assert!(warning.unwrap().contains("Permission denied"));
    }

    #[mononoke::test]
    fn test_fallback_replaced_once_file_is_valid() {
        let file = FakeFile::new(None);
        let now = Instant::now();
        let (mut list, warning) = create(&file, now);
        assert!(warning.is_some());
        file.write(Some("db1:3307\n"));
        assert_eq!(list.refresh_with(&file, now), Refresh::Changed);
        assert_eq!(list.candidates(), candidates(&[("db1", 3307)]));
    }

    #[mononoke::test]
    fn test_reload_only_on_change() {
        let file = FakeFile::new(Some("db1\ndb2\n"));
        let now = Instant::now();
        let (mut list, _) = create(&file, now);
        assert_eq!(file.reads.get(), 1);
        // Same version: not read again.
        for _ in 0..3 {
            assert_eq!(list.refresh_with(&file, now), Refresh::Unchanged);
        }
        assert_eq!(file.reads.get(), 1);

        // A new modification time.
        file.write(Some("db2\ndb3\n"));
        assert_eq!(list.refresh_with(&file, now), Refresh::Changed);
        assert_eq!(
            list.candidates(),
            candidates(&[("db2", 3306), ("db3", 3306)])
        );
        assert_eq!(file.reads.get(), 2);

        // A new size with the same modification time.
        *file.contents.borrow_mut() = Some("db2\ndb3\ndb4\n".to_string());
        assert_eq!(list.refresh_with(&file, now), Refresh::Changed);
        assert_eq!(
            list.candidates(),
            candidates(&[("db2", 3306), ("db3", 3306), ("db4", 3306)])
        );

        // Touched without a change of list: read, but unchanged.
        file.touch();
        assert_eq!(list.refresh_with(&file, now), Refresh::Unchanged);
        assert_eq!(file.reads.get(), 4);
        assert_eq!(list.refresh_with(&file, now), Refresh::Unchanged);
        assert_eq!(file.reads.get(), 4);
    }

    #[mononoke::test]
    fn test_keep_last_good() {
        let file = FakeFile::new(Some("db1\ndb2\n"));
        let start = Instant::now();
        let good = candidates(&[("db1", 3306), ("db2", 3306)]);
        let (mut list, _) = create(&file, start);
        assert_eq!(list.candidates(), good);

        // Invalid, empty, unreadable or missing: the last good list stays.
        file.write(Some("db1\ndb2:x\n"));
        let first = warning(list.refresh_with(&file, start)).expect("first failure warns");
        assert!(first.contains("line 2: invalid entry 'db2:x'"), "{first}");
        assert!(first.ends_with("; using db1:3306, db2:3306"), "{first}");
        assert_eq!(list.candidates(), good);

        let mut now = start;
        for contents in [Some(""), Some("#\n"), None] {
            now += Duration::from_secs(1);
            file.write(contents);
            // Rate-limited.
            assert_eq!(warning(list.refresh_with(&file, now)), None, "{contents:?}");
            assert_eq!(list.candidates(), good);
        }
        file.write(Some("db3\n"));
        file.unreadable.set(true);
        assert_eq!(warning(list.refresh_with(&file, now)), None);
        assert_eq!(list.candidates(), good);

        // A bad file is read again every round, and warned about again once
        // the interval has passed.
        let reads = file.reads.get();
        assert_eq!(warning(list.refresh_with(&file, now)), None);
        assert_eq!(file.reads.get(), reads + 1);
        let later = start + HOSTS_FILE_WARNING_INTERVAL;
        let again = warning(list.refresh_with(&file, later)).expect("warns again");
        assert!(again.contains("Permission denied"), "{again}");
        assert_eq!(warning(list.refresh_with(&file, later)), None);

        // Fixed: used at once, and the next failure warns at once.
        file.unreadable.set(false);
        assert_eq!(list.refresh_with(&file, later), Refresh::Changed);
        assert_eq!(list.candidates(), candidates(&[("db3", 3306)]));
        file.write(None);
        assert!(warning(list.refresh_with(&file, later)).is_some());
        assert_eq!(list.candidates(), candidates(&[("db3", 3306)]));
    }

    #[mononoke::test]
    fn test_restored_file_is_unchanged() {
        let file = FakeFile::new(Some("db1\n"));
        let now = Instant::now();
        let (mut list, _) = create(&file, now);
        file.write(Some("db1:0\n"));
        assert!(warning(list.refresh_with(&file, now)).is_some());
        file.write(Some("db1\n"));
        assert_eq!(list.refresh_with(&file, now), Refresh::Unchanged);
        assert_eq!(list.candidates(), candidates(&[("db1", 3306)]));
    }

    #[mononoke::test]
    fn test_writable_by_others() {
        let now = Instant::now();
        for (mode, warns) in [
            (0o644, false),
            (0o600, false),
            (0o444, false),
            (0o664, true),
            (0o646, true),
            (0o666, true),
            (0o620, true),
        ] {
            let file = FakeFile::new(Some("db1\n"));
            file.mode.set(mode);
            let (mut list, _) = create(&file, now);
            assert_eq!(list.candidates(), candidates(&[("db1", 3306)]));
            let warning = list.access_warning.take();
            assert_eq!(warning.is_some(), warns, "{mode:o}: {warning:?}");
            if let Some(warning) = warning {
                assert!(
                    warning.starts_with(&format!(
                        "MySQL hosts file {PATH} is writable by other users (mode {mode:03o})"
                    )),
                    "{warning}"
                );
                assert!(warning.ends_with("chmod go-w"), "{warning}");
            }
        }

        // Each new version read is checked, even an invalid one, and a
        // version that is not read again is not checked again.
        let file = FakeFile::new(Some("db1\n"));
        let (mut list, _) = create(&file, now);
        assert_eq!(list.access_warning, None);
        file.mode.set(0o664);
        file.write(Some("db1:x\n"));
        assert!(warning(list.refresh_with(&file, now)).is_some());
        assert!(list.access_warning.take().is_some());
        file.write(Some("db2\n"));
        assert_eq!(list.refresh_with(&file, now), Refresh::Changed);
        assert!(list.access_warning.take().is_some());
        assert_eq!(list.refresh_with(&file, now), Refresh::Unchanged);
        assert_eq!(list.access_warning, None);
        file.mode.set(0o644);
        file.touch();
        assert_eq!(list.refresh_with(&file, now), Refresh::Unchanged);
        assert_eq!(list.access_warning, None);
    }

    #[cfg(unix)]
    #[mononoke::test]
    fn test_real_file_writable_by_others() -> Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir()?;
        let path = dir.path().join("hosts");
        fs::write(&path, "127.0.0.1\n")?;
        for (mode, warns) in [(0o644, false), (0o664, true), (0o600, false)] {
            fs::set_permissions(&path, fs::Permissions::from_mode(mode))?;
            let (mut list, warning) = CandidateList::new_with(
                &RealFiles,
                Some(path.clone()),
                13306,
                fallback(),
                Instant::now(),
            );
            assert_eq!(warning, None);
            assert_eq!(list.candidates(), candidates(&[("127.0.0.1", 13306)]));
            let warning = list.access_warning.take();
            assert_eq!(warning.is_some(), warns, "{mode:o}: {warning:?}");
        }
        Ok(())
    }

    #[mononoke::test]
    fn test_real_file() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("hosts");
        let now = Instant::now();
        let set_modified = |seconds: u64| -> Result<()> {
            fs::File::options()
                .write(true)
                .open(&path)?
                .set_modified(SystemTime::UNIX_EPOCH + Duration::from_secs(seconds))?;
            Ok(())
        };

        // Missing at creation.
        let mut list = CandidateList::new(Some(path.clone()), 13306, fallback(), now);
        assert_eq!(list.candidates(), fallback());

        fs::write(&path, "127.0.0.1\n127.0.0.1:13307\n")?;
        set_modified(1_000)?;
        assert_eq!(list.refresh(now), Refresh::Changed);
        assert_eq!(
            list.candidates(),
            candidates(&[("127.0.0.1", 13306), ("127.0.0.1", 13307)])
        );
        assert_eq!(list.refresh(now), Refresh::Unchanged);

        // Rewritten with the same size: the modification time tells.
        fs::write(&path, "127.0.0.1\n127.0.0.1:13308\n")?;
        set_modified(2_000)?;
        assert_eq!(list.refresh(now), Refresh::Changed);
        assert_eq!(
            list.candidates(),
            candidates(&[("127.0.0.1", 13306), ("127.0.0.1", 13308)])
        );

        // Invalid, then removed: the last good list stays.
        fs::write(&path, "127.0.0.1:99999\n")?;
        assert!(matches!(list.refresh(now), Refresh::KeptLastGood { .. }));
        fs::remove_file(&path)?;
        assert!(matches!(list.refresh(now), Refresh::KeptLastGood { .. }));
        assert_eq!(
            list.candidates(),
            candidates(&[("127.0.0.1", 13306), ("127.0.0.1", 13308)])
        );
        Ok(())
    }
}

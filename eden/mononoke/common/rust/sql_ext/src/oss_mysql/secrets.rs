/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

//! Resolution of the `user_secret` and `password_secret` of an OSS MySQL
//! database.
//!
//! Each is a reference to where the value is kept:
//! - `env:NAME`: the environment variable NAME;
//! - `file:/absolute/path`: the file's contents, without one trailing
//!   newline;
//! - `literal:VALUE`: VALUE itself;
//! - `NAME`: the file NAME in the `--oss-mysql-secrets-dir` directory;
//! - `aws-sm:ID`: reserved for AWS Secrets Manager, not supported yet.
//!
//! Values are never logged, and errors never include them.

use std::collections::HashSet;
use std::env::VarError;
use std::fmt;
use std::fs::File;
use std::io;
use std::io::Read;
use std::path::Path;
use std::sync::LazyLock;
use std::sync::Mutex;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use anyhow::bail;

/// The forms of a secret reference, for error messages.
const FORMS: &str =
    "env:NAME, file:/absolute/path, literal:VALUE, or NAME with --oss-mysql-secrets-dir";

/// Which secret is resolved.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SecretKind {
    /// The user name.
    User,
    /// The password.
    Password,
}

impl fmt::Display for SecretKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            SecretKind::User => "user",
            SecretKind::Password => "password",
        })
    }
}

/// A resolved secret. Its `Debug` output hides the value.
#[derive(Clone, PartialEq, Eq)]
pub struct SecretValue(String);

impl SecretValue {
    /// The value, to pass to the MySQL client. Never log it.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretValue(<redacted>)")
    }
}

/// A resolved secret, with warnings about how it is kept.
#[derive(Debug)]
pub struct ResolvedSecret {
    /// The value.
    pub value: SecretValue,
    /// Warnings to log. They never include the value.
    pub warnings: Vec<String>,
}

/// Resolves `reference` and logs its warnings, each once per process.
/// `secrets_dir` is `--oss-mysql-secrets-dir`, for bare names.
pub fn resolve_secret(
    reference: &str,
    kind: SecretKind,
    secrets_dir: Option<&Path>,
) -> Result<SecretValue> {
    let resolved = resolve_secret_with_warnings(reference, kind, secrets_dir)?;
    for warning in resolved.warnings {
        warn_once(warning);
    }
    Ok(resolved.value)
}

/// The user name and password a cluster connects with.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Credentials {
    /// The user name.
    pub user: SecretValue,
    /// The password.
    pub password: SecretValue,
}

impl Credentials {
    /// Resolves both references, logging their warnings once per process.
    pub fn resolve(
        user_secret: &str,
        password_secret: &str,
        secrets_dir: Option<&Path>,
    ) -> Result<Self> {
        Ok(Self {
            user: resolve_secret(user_secret, SecretKind::User, secrets_dir)?,
            password: resolve_secret(password_secret, SecretKind::Password, secrets_dir)?,
        })
    }
}

/// Resolves `reference`, returning its warnings instead of logging them.
pub fn resolve_secret_with_warnings(
    reference: &str,
    kind: SecretKind,
    secrets_dir: Option<&Path>,
) -> Result<ResolvedSecret> {
    resolve(reference, kind, secrets_dir, |name| std::env::var(name))
}

fn resolve(
    reference: &str,
    kind: SecretKind,
    secrets_dir: Option<&Path>,
    env: impl Fn(&str) -> Result<String, VarError>,
) -> Result<ResolvedSecret> {
    resolve_value(reference, kind, secrets_dir, env)
        .with_context(|| format!("cannot resolve the MySQL {kind} secret"))
}

fn resolve_value(
    reference: &str,
    kind: SecretKind,
    secrets_dir: Option<&Path>,
    env: impl Fn(&str) -> Result<String, VarError>,
) -> Result<ResolvedSecret> {
    let mut warnings = Vec::new();
    let value = if let Some(name) = reference.strip_prefix("env:") {
        if name.is_empty() {
            bail!("env: needs a variable name");
        }
        match env(name) {
            Ok(value) => value,
            Err(VarError::NotPresent) => bail!("environment variable {name} is not set"),
            Err(VarError::NotUnicode(_)) => {
                bail!("environment variable {name} is not valid UTF-8")
            }
        }
    } else if let Some(path) = reference.strip_prefix("file:") {
        let path = Path::new(path);
        if !path.is_absolute() {
            bail!("file: needs an absolute path, not '{}'", path.display());
        }
        read_secret_file(path, &mut warnings)?
    } else if let Some(value) = reference.strip_prefix("literal:") {
        if kind == SecretKind::Password {
            warnings.push(
                "the MySQL password is written in the config with literal:; \
                 use file: or env: instead"
                    .to_string(),
            );
        }
        value.to_string()
    } else if reference.starts_with("aws-sm:") {
        bail!("aws-sm: secrets are not supported yet; use {FORMS}");
    } else if reference.contains(':') {
        // Not echoed: it may be a password that lacks `literal:`.
        bail!("unknown form of secret reference; use {FORMS}");
    } else {
        let dir = secrets_dir.ok_or_else(|| {
            // Not echoed either, for the same reason.
            anyhow!("a bare secret name needs --oss-mysql-secrets-dir; or use {FORMS}")
        })?;
        if reference.is_empty() || reference == "." || reference == ".." || reference.contains('/')
        {
            bail!("a bare secret name must be the name of a file in --oss-mysql-secrets-dir");
        }
        // Neither the name nor the path is echoed, for the same reason: only
        // the directory and the kind of failure.
        let contents = read_file(&dir.join(reference), &mut warnings).map_err(|err| {
            anyhow!(
                "cannot read the file the bare secret name refers to in \
                 --oss-mysql-secrets-dir {}: {}",
                dir.display(),
                err.kind()
            )
        })?;
        strip_newline(contents)
    };
    Ok(ResolvedSecret {
        value: SecretValue(value),
        warnings,
    })
}

/// Reads a secret file, without one trailing newline. Warns when users other
/// than the owner have access to it.
fn read_secret_file(path: &Path, warnings: &mut Vec<String>) -> Result<String> {
    let contents = read_file(path, warnings)
        .with_context(|| format!("cannot read secret file {}", path.display()))?;
    Ok(strip_newline(contents))
}

/// Drops one trailing newline.
fn strip_newline(mut contents: String) -> String {
    if contents.ends_with('\n') {
        contents.pop();
        if contents.ends_with('\r') {
            contents.pop();
        }
    }
    contents
}

fn read_file(path: &Path, warnings: &mut Vec<String>) -> io::Result<String> {
    let mut file = File::open(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mode = file.metadata()?.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            warnings.push(format!(
                "secret file {} is accessible to other users (mode {mode:03o}); \
                 restrict it with chmod 600",
                path.display()
            ));
        }
    }
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    Ok(contents)
}

/// Logs `warning` unless it was logged before in this process.
pub(super) fn warn_once(warning: String) {
    static WARNED: LazyLock<Mutex<HashSet<String>>> = LazyLock::new(Default::default);
    let first = WARNED
        .lock()
        .expect("secret warnings lock poisoned")
        .insert(warning.clone());
    if first {
        tracing::warn!("{warning}");
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::fs;
    use std::path::PathBuf;

    use mononoke_macros::mononoke;

    use super::*;

    fn env(name: &str) -> Result<String, VarError> {
        match name {
            "MONONOKE_MYSQL_PASSWORD" => Ok("s3cret".to_string()),
            "EMPTY" => Ok(String::new()),
            "NOT_UNICODE" => Err(VarError::NotUnicode(OsString::from("x"))),
            _ => Err(VarError::NotPresent),
        }
    }

    fn resolve_with(
        reference: &str,
        kind: SecretKind,
        secrets_dir: Option<&Path>,
    ) -> Result<ResolvedSecret> {
        resolve(reference, kind, secrets_dir, env)
    }

    fn value(reference: &str, secrets_dir: Option<&Path>) -> String {
        let resolved = resolve_with(reference, SecretKind::Password, secrets_dir)
            .unwrap_or_else(|e| panic!("{reference}: {e:#}"));
        resolved.value.expose().to_string()
    }

    fn error(reference: &str, secrets_dir: Option<&Path>) -> String {
        let err = resolve_with(reference, SecretKind::Password, secrets_dir)
            .expect_err("resolution should fail");
        format!("{err:#}")
    }

    /// Writes a secret file with the given mode.
    fn secret_file(dir: &Path, name: &str, contents: &str, mode: u32) -> PathBuf {
        let path = dir.join(name);
        fs::write(&path, contents).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(mode)).unwrap();
        }
        #[cfg(not(unix))]
        let _ = mode;
        path
    }

    #[mononoke::test]
    fn test_env() {
        assert_eq!(value("env:MONONOKE_MYSQL_PASSWORD", None), "s3cret");
        assert_eq!(value("env:EMPTY", None), "");
        let err = error("env:MISSING", None);
        assert!(
            err.contains(
                "cannot resolve the MySQL password secret: environment variable MISSING is not set"
            ),
            "{err}"
        );
        let err = error("env:NOT_UNICODE", None);
        assert!(err.contains("NOT_UNICODE is not valid UTF-8"), "{err}");
        let err = error("env:", None);
        assert!(err.contains("env: needs a variable name"), "{err}");
        // The process environment is read for real.
        let path = std::env::var("PATH").unwrap();
        let resolved = resolve_secret_with_warnings("env:PATH", SecretKind::User, None).unwrap();
        assert_eq!(resolved.value.expose(), path);
        assert!(resolved.warnings.is_empty());
    }

    #[mononoke::test]
    fn test_file() {
        let dir = tempfile::tempdir().unwrap();
        let reference = |path: PathBuf| format!("file:{}", path.display());
        for (contents, expected) in [
            ("s3cret\n", "s3cret"),
            ("s3cret", "s3cret"),
            ("s3cret\r\n", "s3cret"),
            // Only the first trailing newline.
            ("s3cret\n\n", "s3cret\n"),
            (" s3cret \n", " s3cret "),
            ("\n", ""),
            ("", ""),
        ] {
            let path = secret_file(dir.path(), "pw", contents, 0o600);
            let resolved = resolve_with(&reference(path), SecretKind::Password, None).unwrap();
            assert_eq!(resolved.value.expose(), expected, "{contents:?}");
            assert!(resolved.warnings.is_empty(), "{:?}", resolved.warnings);
        }

        let err = error(&reference(dir.path().join("missing")), None);
        assert!(err.contains("cannot read secret file"), "{err}");
        assert!(err.contains("missing"), "{err}");

        let err = error("file:relative/pw", None);
        assert!(
            err.contains("file: needs an absolute path, not 'relative/pw'"),
            "{err}"
        );
    }

    #[cfg(unix)]
    #[mononoke::test]
    fn test_file_permissions() {
        let dir = tempfile::tempdir().unwrap();
        for (mode, warns) in [
            (0o600, false),
            (0o400, false),
            (0o640, true),
            (0o604, true),
            (0o660, true),
            (0o644, true),
        ] {
            let path = secret_file(dir.path(), &format!("pw-{mode:o}"), "s3cret\n", mode);
            let resolved = resolve_with(
                &format!("file:{}", path.display()),
                SecretKind::Password,
                None,
            )
            .unwrap();
            assert_eq!(resolved.value.expose(), "s3cret");
            assert_eq!(!resolved.warnings.is_empty(), warns, "{mode:o}");
            if warns {
                let warning = &resolved.warnings[0];
                assert!(warning.contains(&format!("(mode {mode:03o})")), "{warning}");
                assert!(warning.contains("chmod 600"), "{warning}");
                assert!(!warning.contains("s3cret"), "{warning}");
            }
        }
    }

    #[mononoke::test]
    fn test_literal() {
        let resolved = resolve_with("literal:s3cret", SecretKind::Password, None).unwrap();
        assert_eq!(resolved.value.expose(), "s3cret");
        assert_eq!(resolved.warnings.len(), 1);
        assert!(resolved.warnings[0].contains("literal:"));
        assert!(!resolved.warnings[0].contains("s3cret"));

        // A literal user name is fine.
        let resolved = resolve_with("literal:mononoke", SecretKind::User, None).unwrap();
        assert_eq!(resolved.value.expose(), "mononoke");
        assert!(resolved.warnings.is_empty());

        // Everything after the prefix is the value.
        assert_eq!(value("literal:a:b c", None), "a:b c");
        assert_eq!(value("literal:", None), "");
    }

    #[mononoke::test]
    fn test_bare_name() {
        let dir = tempfile::tempdir().unwrap();
        secret_file(dir.path(), "mysql-password", "s3cret\n", 0o600);
        assert_eq!(value("mysql-password", Some(dir.path())), "s3cret");

        // A name with no file may be a password missing `literal:`: the error
        // names the directory and the failure, not the name or the path.
        let err = error("other-password", Some(dir.path()));
        assert_eq!(
            err,
            format!(
                "cannot resolve the MySQL password secret: cannot read the file the bare \
                 secret name refers to in --oss-mysql-secrets-dir {}: entity not found",
                dir.path().display()
            )
        );
        assert!(!err.contains("other-password"), "{err}");
        // So is a name that cannot be read for another reason.
        fs::create_dir(dir.path().join("hunter2")).unwrap();
        let err = error("hunter2", Some(dir.path()));
        assert!(
            err.contains("cannot read the file the bare secret name refers to"),
            "{err}"
        );
        assert!(!err.contains("hunter2"), "{err}");

        for name in ["", ".", "..", "sub/pw", "../pw"] {
            let err = error(name, Some(dir.path()));
            assert!(
                err.contains("a bare secret name must be the name of a file"),
                "{name}: {err}"
            );
        }

        // Without a directory, the error lists the forms and does not echo
        // the reference, which may be a password missing `literal:`.
        let err = error("hunter2", None);
        assert!(
            err.contains("a bare secret name needs --oss-mysql-secrets-dir"),
            "{err}"
        );
        assert!(
            err.contains("env:NAME, file:/absolute/path, literal:VALUE"),
            "{err}"
        );
        assert!(!err.contains("hunter2"), "{err}");
    }

    #[cfg(unix)]
    #[mononoke::test]
    fn test_bare_name_permissions() {
        let dir = tempfile::tempdir().unwrap();
        secret_file(dir.path(), "pw", "s3cret", 0o644);
        let resolved = resolve_with("pw", SecretKind::Password, Some(dir.path())).unwrap();
        assert_eq!(resolved.value.expose(), "s3cret");
        assert_eq!(resolved.warnings.len(), 1, "{:?}", resolved.warnings);
    }

    #[mononoke::test]
    fn test_aws_secrets_manager() {
        let err = error("aws-sm:prod/mononoke/mysql", None);
        assert!(
            err.contains("aws-sm: secrets are not supported yet"),
            "{err}"
        );
    }

    #[mononoke::test]
    fn test_unknown_form() {
        let err = error("vault:hunter2", None);
        assert!(err.contains("unknown form of secret reference"), "{err}");
        assert!(err.contains("env:NAME"), "{err}");
        assert!(!err.contains("hunter2"), "{err}");
        assert!(!err.contains("vault"), "{err}");
        // Prefixes are case-sensitive.
        let err = error("ENV:MONONOKE_MYSQL_PASSWORD", None);
        assert!(err.contains("unknown form of secret reference"), "{err}");
    }

    #[mononoke::test]
    fn test_value_is_redacted() {
        let resolved = resolve_with("literal:s3cret", SecretKind::Password, None).unwrap();
        assert_eq!(format!("{:?}", resolved.value), "SecretValue(<redacted>)");
        assert!(!format!("{resolved:?}").contains("s3cret"));
        // The logging variant returns the same value.
        let value = resolve_secret("literal:s3cret", SecretKind::Password, None).unwrap();
        assert_eq!(value, resolved.value);
    }
}

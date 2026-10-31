/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

mod args;
#[cfg(fbcode_build)]
pub mod facebook;
mod settings;

use environment::Caching;
#[cfg(fbcode_build)]
use environment::LocalCacheConfig;
#[cfg(fbcode_build)]
use environment::LocalCacheEncoding;
use fbinit::FacebookInit;

#[cfg(fbcode_build)]
use crate::args::CacheEncoding;
pub use crate::args::CacheMode;
pub use crate::args::CachelibArgs;
pub use crate::settings::CachelibSettings;

/// Initializes cachelib with settings that are patched beforehand with args
pub fn init_cachelib(
    fb: FacebookInit,
    settings: &CachelibSettings,
    args: &CachelibArgs,
) -> Caching {
    if args.cache_mode == CacheMode::Disabled {
        return Caching::Disabled;
    }

    // Open-source builds have no cachelib, so any mode that needs it, the
    // default included, runs as --cache-mode=disabled would.
    #[cfg(not(fbcode_build))]
    {
        let _ = (fb, settings);
        let mode = match args.cache_mode {
            CacheMode::Enabled => "enabled",
            CacheMode::LocalOnly => "local-only",
            CacheMode::Disabled => "disabled",
        };
        tracing::warn!(
            "--cache-mode={mode} needs cachelib, which open-source builds lack: caching is disabled"
        );
        Caching::Disabled
    }
    #[cfg(fbcode_build)]
    {
        let mut settings = settings.clone();
        settings.update_from_args(args);

        let enable_cacheadmin = !args.cachelib_disable_cacheadmin;
        facebook::init_cachelib_from_settings(fb, settings, enable_cacheadmin)
            .expect("cachelib initialize should always succeed");

        let encoding = match args.cache_encoding {
            CacheEncoding::Bincode => LocalCacheEncoding::Bincode,
        };

        match args.cache_mode {
            CacheMode::Enabled => Caching::Enabled(LocalCacheConfig {
                blobstore_cache_shards: args.cachelib_shards,
                encoding,
            }),
            CacheMode::LocalOnly => Caching::LocalOnly(LocalCacheConfig {
                blobstore_cache_shards: args.cachelib_shards,
                encoding,
            }),
            CacheMode::Disabled => unreachable!(),
        }
    }
}

#[cfg(all(test, not(fbcode_build)))]
mod test {
    use clap::Args;
    use clap::Command;
    use clap::FromArgMatches;
    use fbinit::FacebookInit;
    use mononoke_macros::mononoke;

    use super::*;

    fn parse(cli: &[&str]) -> CachelibArgs {
        // The app builder gives these their defaults from the settings.
        let required = [
            "--cache-size-gb",
            "1",
            "--cache-file-size-gb",
            "1",
            "--cachelib-rebalancing-interval-secs",
            "300",
        ];
        let matches = CachelibArgs::augment_args(Command::new("test"))
            .try_get_matches_from(
                std::iter::once("test")
                    .chain(required)
                    .chain(cli.iter().copied()),
            )
            .unwrap();
        CachelibArgs::from_arg_matches(&matches).unwrap()
    }

    #[mononoke::fbinit_test]
    async fn test_every_cache_mode_runs_disabled(fb: FacebookInit) {
        for cli in [
            &[][..],
            &["--cache-mode", "enabled"],
            &["--cache-mode", "local-only"],
            &["--cache-mode", "disabled"],
        ] {
            let caching = init_cachelib(fb, &CachelibSettings::default(), &parse(cli));
            assert!(matches!(caching, Caching::Disabled), "{cli:?}");
        }
    }
}

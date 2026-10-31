/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

//! Building the app installs the in-memory stats backend. Its own process, so that no stat is
//! used before the app is built.

#![cfg(not(fbcode_build))]

use fbinit::FacebookInit;
use mononoke_app::MononokeAppBuilder;
use mononoke_macros::mononoke;
use stats::prelude::*;

define_stats! {
    prefix = "mononoke_app_memory_stats_test";
    written: timeseries(Sum),
}

#[mononoke::fbinit_test]
fn the_app_builder_installs_the_memory_stats_backend(fb: FacebookInit) {
    let _builder = MononokeAppBuilder::new(fb);
    assert!(stats::memory_stats::is_installed());

    STATS::written.add_value(42);
    let snapshot = stats::memory_stats::snapshot().expect("the backend is installed");
    let written = snapshot
        .get("mononoke_app_memory_stats_test.written")
        .expect("the stat is tracked");
    assert_eq!(written.total.sum, 42);
}

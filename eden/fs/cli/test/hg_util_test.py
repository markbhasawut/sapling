#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2.

# pyre-strict

import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from .. import hg_util
from ..config import CheckoutConfigCorruptedError, EdenCheckout

_IDENTITY_VARS = ("SL_REPO_IDENTITY", "HGREPO_IDENTITY", "HGIDENTITY", "SL_IDENTITY")


class SniffDotDirTest(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory(prefix="eden_hg_util_test.")
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        env = {k: v for k, v in os.environ.items() if k not in _IDENTITY_VARS}
        patcher = patch.dict(os.environ, env, clear=True)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_existing_dot_dir_wins(self) -> None:
        (self.root / ".sl").mkdir()
        self.assertEqual(hg_util.sniff_dot_dir(self.root, ".hg"), ".sl")

    def test_environment_wins_over_default(self) -> None:
        os.environ["SL_REPO_IDENTITY"] = "hg"
        self.assertEqual(hg_util.sniff_dot_dir(self.root, ".sl"), ".hg")

    def test_default_when_nothing_chooses(self) -> None:
        self.assertEqual(hg_util.sniff_dot_dir(self.root, ".sl"), ".sl")
        self.assertEqual(hg_util.sniff_dot_dir(self.root), ".hg")

    def _checkout_dot_path(self, get_config: object) -> Path:
        mount = self.root / "mount"
        mount.mkdir()
        checkout = SimpleNamespace(path=mount, get_config=get_config)
        # pyre-ignore[16]: cached_property exposes the wrapped function as func.
        return EdenCheckout.hg_dot_path.func(checkout)

    def test_new_checkout_takes_backing_repo_dot_dir(self) -> None:
        backing_repo = self.root / "backing"
        (backing_repo / ".sl").mkdir(parents=True)
        config = SimpleNamespace(backing_repo=backing_repo)
        self.assertEqual(
            self._checkout_dot_path(lambda: config), self.root / "mount" / ".sl"
        )

    def test_checkout_without_config_uses_hg(self) -> None:
        def get_config() -> None:
            raise CheckoutConfigCorruptedError("config.toml does not exist")

        self.assertEqual(
            self._checkout_dot_path(get_config), self.root / "mount" / ".hg"
        )

#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2.

# pyre-strict

import io
import string
import unittest
from pathlib import Path
from typing import BinaryIO

from eden.fs.cli.rage import IOWithRedaction, paste_output, print_log_file


class LogRedactionTest(unittest.TestCase):
    def test_token_across_a_read_boundary(self) -> None:
        token = "ghp_" + (string.digits + string.ascii_lowercase)[:36]
        read_size = 20 * 1024
        body = ("." * (read_size - 10) + token + "." * 100 + "\n").encode()

        def open_log(path: Path, mode: str) -> BinaryIO:
            return io.BytesIO(body)

        sink = io.BytesIO()
        print_log_file(
            Path("edenfs.log"), IOWithRedaction(sink), open_fn=open_log, tail_limit=None
        )

        written = sink.getvalue().decode()
        self.assertNotIn(token, written)
        self.assertIn("<ACCESS_TOKEN_REDACTED>", written)
        self.assertEqual(
            len(written), len(body) - len(token) + len("<ACCESS_TOKEN_REDACTED>")
        )

    def test_token_inside_one_read(self) -> None:
        token = "ghp_" + (string.digits + string.ascii_lowercase)[:36]
        body = ("." * 100 + token + "." * 100 + "\n").encode()

        def open_log(path: Path, mode: str) -> BinaryIO:
            return io.BytesIO(body)

        sink = io.BytesIO()
        print_log_file(
            Path("edenfs.log"), IOWithRedaction(sink), open_fn=open_log, tail_limit=None
        )

        written = sink.getvalue().decode()
        self.assertNotIn(token, written)
        self.assertIn("<ACCESS_TOKEN_REDACTED>", written)


class PasteOutputTest(unittest.TestCase):
    def paste(self, processor: str, text: str) -> tuple[int, str]:
        sink = io.BytesIO()
        result = paste_output(
            lambda out: out.write(text), processor, IOWithRedaction(sink), False
        )
        return result, sink.getvalue().decode()

    def test_without_reporter_the_section_is_inline(self) -> None:
        token = "ghp_" + (string.digits + string.ascii_lowercase)[:36]
        result, written = self.paste("", f"section body {token}\n")
        self.assertEqual(result, 0)
        self.assertEqual(written, "section body <ACCESS_TOKEN_REDACTED>\n")

    def test_without_reporter_a_failing_section_is_reported(self) -> None:
        def fail(out: IOWithRedaction) -> None:
            raise RuntimeError("no such command")

        sink = io.BytesIO()
        result = paste_output(fail, "", IOWithRedaction(sink), False)
        self.assertEqual(result, 1)
        self.assertEqual(
            sink.getvalue().decode(), "Error generating output: no such command\n"
        )

    def test_reporter_output_names_the_paste(self) -> None:
        reporter = (
            "sh -c 'cat > /dev/null; "
            'printf "uploaded\\nP123: https://paste/P123\\n"\''
        )
        result, written = self.paste(reporter, "section body\n")
        self.assertEqual(result, 0)
        self.assertEqual(written, "P123")


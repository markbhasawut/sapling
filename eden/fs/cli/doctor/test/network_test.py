#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2.

# pyre-strict

import subprocess
from pathlib import Path
from typing import List

from eden.fs.cli.doctor.check_network import (
    ConnectivityProblem,
    NetworkChecker,
    parse_server_health,
    ServerHealthProblem,
)
from eden.fs.cli.doctor.problem import ProblemBase, ProblemSeverity
from eden.fs.cli.doctor.test.lib.fake_eden_instance import FakeEdenInstance
from eden.fs.cli.doctor.test.lib.problem_collector import ProblemCollector
from eden.fs.cli.doctor.test.lib.testcase import DoctorTestBase

TREATMENT = (
    "The Mononoke server reports {status} health. The problem is on the server, "
    "not with your network or configuration: retry later, and contact the "
    "server's administrators if it persists."
)

REMEDIATION = (
    "This is a problem on the Mononoke server, not on this machine. "
    "Retry later, and contact the server's administrators if it persists."
)


def network_doctor_stdout(status: str, summary: str) -> str:
    return (
        f"{TREATMENT.format(status=status)}\n"
        "\n"
        f"server reports {status} health: {summary}\n"
    )


class FailingNetworkDoctorChecker(NetworkChecker):
    """'sl debugnetworkdoctor' exits 1 with the given stdout."""

    def __init__(self, stdout: str) -> None:
        self.stdout = stdout
        self.commands: List[List[str]] = []

    def run_command(
        self, args: List[str], cwd: Path
    ) -> subprocess.CompletedProcess[str]:
        self.commands.append(args)
        if args[1:] == ["debugnetworkdoctor"]:
            raise subprocess.CalledProcessError(1, args, output=self.stdout, stderr="")
        raise AssertionError(f"unexpected command {args}")


# The tail of 'sl debugnetwork --speed' for a fast network.
FAST_SPEED_OUTPUT = (
    "Latency: 10.00 ms (average of 5 round-trips)\n"
    "Speed: (round 1) downloaded 10.00 MB in 100.0 ms (800.00 Mbit/s, 100.00 MiB/s)\n"
    "Speed: (round 2) downloaded 10.00 MB in 100.0 ms (800.00 Mbit/s, 100.00 MiB/s)\n"
    "Speed: (round 1) uploaded 10.00 MB in 100.0 ms (800.00 Mbit/s, 100.00 MiB/s)\n"
    "Speed: (round 2) uploaded 10.00 MB in 100.0 ms (800.00 Mbit/s, 100.00 MiB/s)\n"
)


class PassingNetworkDoctorChecker(NetworkChecker):
    """'sl debugnetworkdoctor' exits 0 with the given stdout; every later
    'sl debugnetwork' step passes."""

    def __init__(self, stdout: str) -> None:
        self.stdout = stdout
        self.commands: List[List[str]] = []

    def run_command(
        self, args: List[str], cwd: Path
    ) -> subprocess.CompletedProcess[str]:
        self.commands.append(args)
        if args[1:] == ["debugnetworkdoctor"]:
            stdout = self.stdout
        elif "--speed" in args:
            stdout = FAST_SPEED_OUTPUT
        else:
            stdout = ""
        return subprocess.CompletedProcess(args, 0, stdout=stdout, stderr="")


class NetworkCheckTest(DoctorTestBase):
    def check_network(self, stdout: str) -> List[ProblemBase]:
        instance = FakeEdenInstance(self.make_temporary_directory())
        tracker = ProblemCollector(instance)
        checker = FailingNetworkDoctorChecker(stdout)
        checker.check_network(tracker, Path(self.make_temporary_directory()), set())
        # The later 'sl debugnetwork' steps do not run after a failure.
        self.assertEqual(len(checker.commands), 1)
        return tracker.problems

    def test_server_degraded(self) -> None:
        problems = self.check_network(
            network_doctor_stdout(
                "degraded",
                "metadata_db: cluster 1 (identity): a new primary awaits confirmation",
            )
        )
        self.assertEqual(len(problems), 1)
        problem = problems[0]
        self.assertIsInstance(problem, ServerHealthProblem)
        self.assertEqual(
            problem.description(),
            "Mononoke server reports degraded health: metadata_db: cluster 1 "
            "(identity): a new primary awaits confirmation",
        )
        self.assertEqual(problem.get_manual_remediation_message(), REMEDIATION)
        self.assertEqual(problem.severity(), ProblemSeverity.POTENTIALLY_SERIOUS)

    def test_server_down(self) -> None:
        problems = self.check_network(
            network_doctor_stdout("down", "server: the server is shutting down")
        )
        self.assertEqual(len(problems), 1)
        problem = problems[0]
        self.assertIsInstance(problem, ServerHealthProblem)
        self.assertEqual(
            problem.description(),
            "Mononoke server reports down health: server: the server is shutting down",
        )
        self.assertEqual(problem.get_manual_remediation_message(), REMEDIATION)
        self.assertEqual(problem.severity(), ProblemSeverity.ERROR)

    def test_network_problem(self) -> None:
        stdout = (
            "Please check your VPN or proxy (internet okay, but can't reach server).\n"
            "\n"
            "no server connectivity: TCP error: test\n"
        )
        problems = self.check_network(stdout)
        self.assertEqual(len(problems), 1)
        problem = problems[0]
        self.assertIsInstance(problem, ConnectivityProblem)
        self.assertIn(stdout, problem.description())
        self.assertEqual(problem.severity(), ProblemSeverity.ERROR)

    def test_parse_server_health(self) -> None:
        problem = parse_server_health(
            network_doctor_stdout("down", "sql_queries: 50.0% failed").encode()
        )
        assert problem is not None
        self.assertEqual(
            problem.description(),
            "Mononoke server reports down health: sql_queries: 50.0% failed",
        )
        self.assertIsNone(parse_server_health(None))
        self.assertIsNone(parse_server_health(""))
        self.assertIsNone(parse_server_health(b"No network problems detected.\n"))
        # Only a whole line is a server health diagnosis.
        self.assertIsNone(
            parse_server_health(
                "http connection problem: server reports down health: x"
            )
        )

    def check_passing_network(self, stdout: str) -> PassingNetworkDoctorChecker:
        instance = FakeEdenInstance(self.make_temporary_directory())
        tracker = ProblemCollector(instance)
        checker = PassingNetworkDoctorChecker(stdout)
        checker.check_network(tracker, Path(self.make_temporary_directory()), set())
        self.assertEqual(tracker.problems, [])
        return checker

    def test_repo_without_server(self) -> None:
        # Nothing is left to check: 'sl debugnetwork' would only try to
        # reach "default" on this machine.
        checker = self.check_passing_network(
            "No server configured for this repo: there is no network to check.\n"
        )
        self.assertEqual(
            [args[1:] for args in checker.commands], [["debugnetworkdoctor"]]
        )

    def test_repo_with_server(self) -> None:
        checker = self.check_passing_network("No network problems detected.\n")
        self.assertEqual(
            [args[1] for args in checker.commands],
            ["debugnetworkdoctor", "debugnetwork", "debugnetwork"],
        )

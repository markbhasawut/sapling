#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2.

# pyre-strict

import os
import shlex
import sys
from datetime import timedelta
from pathlib import Path
from textwrap import dedent
from typing import Dict, List, Optional, Set

from eden.fs.cli import (
    config as config_mod,
    daemon,
    daemon_util,
    filesystem,
    mtab,
    prjfs,
    proc_utils as proc_utils_mod,
    ui,
    version,
)
from eden.fs.cli.config import EdenInstance
from eden.fs.cli.doctor.util import (
    CheckoutInfo,
    format_approx_duration,
    get_dependent_repos,
    hg_doctor_in_backing_repo,
)

from facebook.eden.constants import STATS_MOUNTS_STATS

from facebook.eden.ttypes import GetStatInfoParams, MountState
from fb303_core.ttypes import fb303_status

from . import (
    check_filesystems,
    check_hg,
    check_network,
    check_os,
    check_recent_writes,
    check_redirections,
    check_rogue_edenfs,
    check_stale_mounts,
    check_watchman,
)
from .problem import (
    DryRunFixer,
    FixableProblem,
    Problem,
    ProblemFixer,
    ProblemSeverity,
    ProblemTracker,
)

try:
    from .facebook import check_facebook
except ImportError:
    from typing import Any

    def check_facebook(*_args: Any, **_kwargs: Any) -> None:
        pass


try:
    from .facebook.check_vscode_extensions import VSCodeExtensionsChecker
except ImportError:
    from typing import Any

    class VSCodeExtensionsChecker:
        def check_problematic_vscode_extensions(
            self, *_args: Any, **_kwargs: Any
        ) -> None:
            pass


try:
    from .facebook.internal_consts import (
        get_doctor_link,
        get_local_commit_recovery_link,
    )
except ImportError:

    def get_doctor_link() -> str:
        return ""

    def get_local_commit_recovery_link() -> str:
        return ""


# working_directory_was_stale may be set to True by the CLI main module
# if the original working directory referred to a stale eden mount point.
working_directory_was_stale = False


def get_reclone_msg(checkout_path: str) -> str:
    reclone_msg = """To recover, you will need to remove and reclone the repo.
Your local commits will be uneffected, but reclones will lose uncommitted work or shelves.
However, the local changes are manually recoverable before the reclone."""

    if get_local_commit_recovery_link():
        reclone_msg += f"\nIf you have local changes you would like to save before reclone, see {get_local_commit_recovery_link()}, or reachout to the EdenFS team."

    if get_doctor_link():
        reclone_msg += (
            "\nTo reclone the corrupted repo, run: `fbclone $REPO --reclone --eden`"
        )
        reclone_msg += f"\nFor additional info see the wiki at {get_doctor_link()}"
    else:
        reclone_msg += f"\nTo remove the corrupted repo, run: `eden rm {checkout_path}`"
    return reclone_msg


def cure_what_ails_you(
    instance: EdenInstance,
    dry_run: bool,
    *,
    debug: bool = False,
    fast: bool = False,
    min_severity_to_report: ProblemSeverity = ProblemSeverity.ADVICE,
    mount_table: Optional[mtab.MountTable] = None,
    fs_util: Optional[filesystem.FsUtil] = None,
    proc_utils: Optional[proc_utils_mod.ProcUtils] = None,
    vscode_extensions_checker: Optional[VSCodeExtensionsChecker] = None,
    out: Optional[ui.Output] = None,
) -> int:
    return EdenDoctor(
        instance,
        dry_run,
        debug,
        fast,
        min_severity_to_report,
        mount_table,
        fs_util,
        proc_utils,
        vscode_extensions_checker,
        out,
    ).cure_what_ails_you()


class UnexpectedPrivHelperProblem(Problem):
    def __init__(self, ex: Exception) -> None:
        super().__init__("Unexpected error while checking PrivHelper", exception=ex)


class UnexpectedMountProblem(Problem):
    def __init__(self, mount: Path, ex: Exception) -> None:
        super().__init__(f"unexpected error while checking {mount}", exception=ex)


class UnknownElevationProblem(Problem):
    def __init__(self, pid: Optional[int], ex: Exception) -> None:
        super().__init__(
            description=f"Unable to determine elevation of process {pid}: {ex}",
        )


class RunningElevatedProblem(Problem):
    def __init__(self, pid: Optional[int]) -> None:
        super().__init__(
            description=f"EdenFS is running as elevated process {pid}, which isn't supported",
            remediation="Run `edenfsctl restart` from a non-elevated command prompt",
        )


class EdenDoctorChecker:
    """EdenDoctorChecker is a base class for EdenDoctor, and only supports
    running checks, without reporting or fixing problems.
    """

    instance: EdenInstance
    mount_table: mtab.MountTable
    fs_util: filesystem.FsUtil
    proc_utils: proc_utils_mod.ProcUtils
    vscode_extensions_checker: VSCodeExtensionsChecker
    tracker: ProblemTracker
    out: ui.Output
    # Setting run_system_wide_checks to False causes EdenDoctor to skip checks that
    # try to detect system-wide problems (e.g., stale mount points, old OS/kernel
    # version, low disk space, etc.).  It is often desirable to disable this during
    # integration tests, since tests normally only want to report issues with their
    # specific EdenFS instance under test.
    run_system_wide_checks: bool = True

    def __init__(
        self,
        instance: EdenInstance,
        tracker: ProblemTracker,
        debug: bool,
        fast: bool,
        mount_table: Optional[mtab.MountTable] = None,
        fs_util: Optional[filesystem.FsUtil] = None,
        proc_utils: Optional[proc_utils_mod.ProcUtils] = None,
        vscode_extensions_checker: Optional[VSCodeExtensionsChecker] = None,
        out: Optional[ui.Output] = None,
    ) -> None:
        self.instance = instance
        self.tracker = tracker
        self.debug = debug
        self.fast = fast
        self.mount_table = mount_table if mount_table is not None else mtab.new()
        self.fs_util = fs_util if fs_util is not None else filesystem.new()
        self.proc_utils = proc_utils if proc_utils is not None else proc_utils_mod.new()
        self.vscode_extensions_checker = (
            vscode_extensions_checker
            if vscode_extensions_checker is not None
            else VSCodeExtensionsChecker()
        )
        self.out = out if out is not None else ui.get_output()

    def run_checks(self) -> None:
        self.check_working_directory()

        if self.run_system_wide_checks:
            # check OS type, kernel version etc.
            check_os.run_operating_system_checks(self.tracker, self.instance, self.out)

            # check multiple edenfs running with some rogue stale PIDs
            check_rogue_edenfs.check_many_edenfs_are_running(
                self.tracker, self.proc_utils
            )

        status = self.instance.check_health()
        if status.status == fb303_status.ALIVE:
            self.run_normal_checks()
        elif status.status == fb303_status.STARTING:
            self.tracker.add_problem(EdenfsStarting())
        elif status.status == fb303_status.STOPPING:
            self.tracker.add_problem(EdenfsStopping())
        elif (
            status.status == fb303_status.DEAD or status.status == fb303_status.STOPPED
        ):
            self.run_edenfs_not_healthy_checks()
        else:
            self.tracker.add_problem(EdenfsUnexpectedStatus(status))

    def check_working_directory(self) -> None:
        problem = check_for_working_directory_problem()
        if problem:
            self.tracker.add_problem(problem)

    def run_edenfs_not_healthy_checks(self) -> None:
        configured_mounts = self.instance.get_mount_paths()
        if configured_mounts:
            self.tracker.add_problem(EdenfsNotHealthy(self.instance, self.out))
        else:
            self.tracker.using_edenfs = False
            return

        if self.run_system_wide_checks:
            check_stale_mounts.check_for_stale_mounts(self.tracker, self.mount_table)
            check_filesystems.check_disk_usage(
                self.tracker,
                list(configured_mounts),
                self.instance,
                fs_util=self.fs_util,
            )

    def _get_checkouts_info(self) -> Dict[Path, CheckoutInfo]:
        checkouts: Dict[Path, CheckoutInfo] = {}
        # Get information about the checkouts currently known to the running
        # edenfs process
        with self.instance.get_thrift_client_legacy() as client:
            internal_stats = client.getStatInfo(
                GetStatInfoParams(statsMask=STATS_MOUNTS_STATS)
            )
            mount_point_info = internal_stats.mountPointInfo or {}

            for mount in client.listMounts():
                # Old versions of edenfs did not return a mount state field.
                # These versions only listed running mounts, so treat the mount state
                # as running in this case.
                mount_state = (
                    mount.state if mount.state is not None else MountState.RUNNING
                )
                path = Path(os.fsdecode(mount.mountPoint))
                checkout = CheckoutInfo(
                    self.instance,
                    path,
                    backing_repo=(
                        Path(os.fsdecode(mount.backingRepoPath))
                        if mount.backingRepoPath is not None
                        else None
                    ),
                    running_state_dir=Path(os.fsdecode(mount.edenClientPath)),
                    state=mount_state,
                    mount_inode_info=mount_point_info.get(mount.mountPoint),
                )
                checkouts[path] = checkout

        # Get information about the checkouts listed in the config file
        missing_checkouts = []
        for configured_checkout in self.instance.get_checkouts():
            checkout_info = checkouts.get(configured_checkout.path, None)
            if checkout_info is None:
                checkout_info = CheckoutInfo(self.instance, configured_checkout.path)
                checkout_info.configured_state_dir = configured_checkout.state_dir
                checkouts[checkout_info.path] = checkout_info

            if checkout_info.backing_repo is None:
                try:
                    checkout_info.backing_repo = (
                        configured_checkout.get_config().backing_repo
                    )
                except Exception as ex:
                    # Config file is missing or invalid. The string here is formatted to make sense with the remediation message for EdenCheckoutInfosCorruption
                    missing_checkouts.append(
                        f"{configured_checkout.path} (error: {ex})"
                    )
                    continue

            checkout_info.configured_state_dir = configured_checkout.state_dir
        if missing_checkouts:
            errmsg = "\n".join(missing_checkouts)
            raise RuntimeError(errmsg)

        return checkouts

    def check_privhelper(self) -> None:
        try:
            connected = self.instance.check_privhelper_connection()
            if not connected:
                self.tracker.add_problem(EdenfsPrivHelperNotHealthy())
        except Exception as ex:
            # This check is only reached after we've determined that the EdenFS
            # daemon is healthy, so any error thrown while calling into Thrift
            # would be unexpected.
            self.tracker.add_problem(UnexpectedPrivHelperProblem(ex))

    def check_running_elevated(self) -> None:
        if sys.platform != "win32":
            return

        # proc_utils_win's ctypes dependences can't be imported on non-Windows.
        from eden.fs.cli import proc_utils_win

        health_status = self.instance.check_health()
        if health_status.pid is None:
            return

        try:
            process_handle = proc_utils_win.open_process(health_status.pid)
            token_handle = proc_utils_win.open_process_token(process_handle)
            elevated = proc_utils_win.is_token_elevated(token_handle)
        except Exception as ex:
            self.tracker.add_problem(UnknownElevationProblem(health_status.pid, ex))
            return

        if elevated:
            self.tracker.add_problem(RunningElevatedProblem(health_status.pid))

    def run_normal_checks(self) -> None:
        check_edenfs_version(self.tracker, self.instance)
        try:
            checkouts = self._get_checkouts_info()
        except RuntimeError as ex:
            self.tracker.add_problem(EdenCheckoutInfosCorruption(ex))
            return
        checked_backing_repos = set()

        if sys.platform == "win32":
            self.check_running_elevated()
        else:
            self.check_privhelper()

        if self.run_system_wide_checks:
            check_filesystems.check_eden_directory(self.tracker, self.instance)
            check_stale_mounts.check_for_stale_mounts(self.tracker, self.mount_table)
            check_filesystems.check_disk_usage(
                self.tracker,
                list(self.instance.get_mount_paths()),
                self.instance,
                fs_util=self.fs_util,
            )
            check_slow_hg_import(self.tracker, self.instance)
            check_facebook(
                self.tracker,
                list(checkouts.values()),
                checked_backing_repos,
                vscode_extensions_checker=self.vscode_extensions_checker,
                eden_instance=self.instance,
            )

        watchman_info = check_watchman.pre_check()

        for path, checkout in sorted(checkouts.items()):
            self.out.writeln(f"Checking {path}")
            try:
                check_mount(
                    self.out,
                    self.tracker,
                    self.instance,
                    checkout,
                    self.mount_table,
                    watchman_info,
                    list(checkouts.values()),
                    checked_backing_repos,
                    self.debug,
                    self.fast,
                )
            except Exception as ex:
                self.tracker.add_problem(UnexpectedMountProblem(path, ex))


class EdenDoctor(EdenDoctorChecker):
    fixer: ProblemFixer
    dry_run: bool
    min_severity_to_report: ProblemSeverity

    def __init__(
        self,
        instance: EdenInstance,
        dry_run: bool,
        debug: bool,
        fast: bool,
        min_severity_to_report: ProblemSeverity,
        mount_table: Optional[mtab.MountTable] = None,
        fs_util: Optional[filesystem.FsUtil] = None,
        proc_utils: Optional[proc_utils_mod.ProcUtils] = None,
        vscode_extensions_checker: Optional[VSCodeExtensionsChecker] = None,
        out: Optional[ui.Output] = None,
    ) -> None:
        self.dry_run = dry_run
        self.min_severity_to_report = min_severity_to_report
        out = out if out is not None else ui.get_output()
        if dry_run:
            self.fixer = DryRunFixer(
                instance,
                out,
                debug,
                min_severity_to_report,
            )
        else:
            self.fixer = ProblemFixer(
                instance,
                out,
                debug,
                min_severity_to_report,
            )

        super().__init__(
            instance,
            tracker=self.fixer,
            debug=debug,
            fast=fast,
            mount_table=mount_table,
            fs_util=fs_util,
            proc_utils=proc_utils,
            vscode_extensions_checker=vscode_extensions_checker,
            out=out,
        )

    def cure_what_ails_you(self) -> int:
        self.run_checks()
        return self._report_problems()

    def _report_problems(self) -> int:
        fixer = self.fixer
        out = self.out

        if not self.dry_run:
            self.instance.log_sample(
                "eden_doctor",
                num_problems=fixer.num_problems,
                problems=fixer.problem_types.union(fixer.ignored_problem_types),
                problem_description=fixer.problem_description,
                num_fixed_problems=fixer.num_fixed_problems,
                num_failed_fixes=fixer.num_failed_fixes,
                num_manual_fixes=fixer.num_manual_fixes,
                num_no_fixes=fixer.num_no_fixes,
                num_advisory_fixes=fixer.num_advisory_fixes,
                problem_failed_fixes=fixer.problem_failed_fixes,
                problem_successful_fixes=fixer.problem_successful_fixes,
                problem_manual_fixes=fixer.problem_manual_fixes,
                problem_no_fixes=fixer.problem_no_fixes,
                problem_advisory_fixes=fixer.problem_advisory_fixes,
                exception=fixer.problem_failed_fixes_exceptions,
            )
        elif sys.platform == "win32":
            # dry run doesn't run fixes so we count the number of fixable problems rather
            # than the number of failed fixes
            self.instance.log_sample(
                "eden_doctor_dry_run",
                num_problems=fixer.num_problems,
                problems=fixer.problem_types.union(fixer.ignored_problem_types),
                problem_description=fixer.problem_description,
                num_fixable=fixer.num_fixable,
                num_manual_fixes=fixer.num_manual_fixes,
                num_no_fixes=fixer.num_no_fixes,
                num_advisory_fixes=fixer.num_advisory_fixes,
                problem_fixable=fixer.problem_fixable,
                problem_manual_fixes=fixer.problem_manual_fixes,
                problem_no_fixes=fixer.problem_no_fixes,
                problem_advisory_fixes=fixer.problem_advisory_fixes,
            )

        if fixer.num_problems == 0:
            if not fixer.using_edenfs:
                out.writeln("EdenFS is not in use.")
            else:
                out.writeln("No issues detected.", fg=out.GREEN)
            return 0

        def problem_count(num: int) -> str:
            if num == 1:
                return "1 problem"
            return f"{num} problems"

        if self.dry_run:
            out.writeln(
                f"Discovered {problem_count(fixer.num_problems)} during --dry-run",
                fg=out.YELLOW,
            )
            return 1

        if fixer.num_fixed_problems:
            out.writeln(
                f"Successfully fixed {problem_count(fixer.num_fixed_problems)}.",
                fg=out.YELLOW,
            )
        if fixer.num_failed_fixes:
            out.writeln(
                f"Failed to fix {problem_count(fixer.num_failed_fixes)}.", fg=out.RED
            )

        if fixer.num_advisory_fixes:
            out.writeln(
                f"{fixer.num_advisory_fixes} issue{'' if fixer.num_advisory_fixes==1 else 's'} with recommended fixes.",
                fg=out.YELLOW,
            )

        if fixer.num_manual_fixes:
            if fixer.num_manual_fixes == 1:
                msg = "1 issue requires manual attention."
            else:
                msg = f"{fixer.num_manual_fixes} issues require manual attention."
            out.writeln(msg, fg=out.YELLOW)

        if fixer.num_no_fixes:
            if fixer.num_no_fixes == 1:
                msg = "No standard fix for 1 issue."
            else:
                msg = f"No standard fix for {fixer.num_no_fixes} issues."
            out.writeln(msg, fg=out.RED)

        if fixer.num_fixed_problems == fixer.num_problems:
            return 0

        if sys.platform == "darwin":
            help_url = "https://fb.workplace.com/groups/edenfsmacos"
            help_group = "EdenFS macOS"
        elif sys.platform == "win32":
            help_url = "https://fb.workplace.com/groups/edenfswindows"
            help_group = "EdenFS Windows"
        else:
            help_url = "https://fb.workplace.com/groups/eden.users"
            help_group = "EdenFS"

        out.write(
            f"Collect an 'eden rage' and ask in the {help_group} Users group if you need help fixing issues with EdenFS:\n"
            f"{help_url}\n"
        )
        return 1


class EdenfsNotHealthy(FixableProblem):

    def __init__(
        self,
        instance: EdenInstance,
        out: ui.Output,
    ) -> None:
        self._instance = instance
        self._out = out

    def description(self) -> str:
        return "EdenFS is not running"

    def dry_run_msg(self) -> str:
        return "Would run `eden start` to start EdenFS"

    def start_msg(self) -> str:
        return "Running `eden start` to start EdenFS..."

    def perform_fix(self) -> None:
        """Try to start EdenFS. If Eden is running, an exception will be thrown (and ignored)."""
        try:
            daemon.start_edenfs_service(self._instance, None, None)
        except Exception:
            # Eden start failed, or Eden is already running/starting. Either way,
            # check_fix will determine if the fix worked.
            pass

    def check_fix(self) -> bool:
        health = self._instance.check_health()
        if health.is_starting():
            self._out.writeln(
                "EdenFS still starting, use `eden status --wait` to watch progress and ensure it starts",
                fg=self._out.YELLOW,
            )
            return False
        elif health.is_healthy():
            return True
        else:
            return False


class EdenfsPrivHelperNotHealthy(Problem):
    def __init__(self) -> None:
        super().__init__(
            "The PrivHelper process is not accessible.",
            remediation="To restore the connection to the PrivHelper, run `eden restart`",
        )


class EdenfsStarting(Problem):
    def __init__(self) -> None:
        remediation = '''\
Please wait for edenfs to finish starting. You can watch its progress with
`eden status --wait`.

If EdenFS seems to be taking too long to start you can try restarting it
with "eden restart --force"'''
        super().__init__(
            "EdenFS is currently still starting.",
            remediation=remediation,
            severity=ProblemSeverity.ADVICE,
        )


class EdenfsStopping(Problem):
    def __init__(self) -> None:
        remediation = """\
Either wait for edenfs to exit, or to forcibly kill EdenFS, run:

    eden stop --kill"""
        super().__init__("EdenFS is currently shutting down.", remediation=remediation)


class EdenfsUnexpectedStatus(Problem):
    def __init__(self, status: config_mod.HealthStatus) -> None:
        msg = f"Unexpected health status reported by edenfs: {status}"
        remediation = 'You can try restarting EdenFS by running "eden restart"'
        super().__init__(msg, remediation=remediation)


class CheckoutIsStartingUp(Problem):
    def __init__(self, checkout: CheckoutInfo) -> None:
        super().__init__(
            f"Checkout {checkout.path} is currently starting up.",
            "If this checkout does not successfully finish starting soon, "
            'try running "eden restart"',
            severity=ProblemSeverity.ADVICE,
        )


class CheckoutIsShuttingDown(Problem):
    def __init__(self, checkout: CheckoutInfo) -> None:
        super().__init__(
            f"Checkout {checkout.path} is currently shutting down.",
            "If this checkout does not successfully finish shutting down soon, "
            'try running "eden restart"',
        )


class CheckoutFailedDuetoFuseError(Problem):
    def __init__(self, checkout: CheckoutInfo) -> None:
        super().__init__(
            f"Checkout {checkout.path} encountered a FUSE error while mounting"
        )


class CheckoutInUnknownState(Problem):
    def __init__(self, checkout: CheckoutInfo) -> None:
        super().__init__(
            f"edenfs reports that checkout {checkout.path} is in "
            "unknown state {checkout.state}"
        )


class NestedCheckout(Problem):
    def __init__(
        self, checkout: CheckoutInfo, existing_checkout: config_mod.EdenCheckout
    ) -> None:
        super().__init__(
            f"""\
edenfs reports that checkout {checkout.path} is nested within an existing checkout {existing_checkout.path}
Nested checkouts are usually not intended and can cause spurious behavior.""",
            f"Consider running `eden rm {checkout.path}` to remove misplaced repo(s)\n",
            severity=ProblemSeverity.ADVICE,
        )


class CheckoutInsideBackingRepo(Problem):
    def __init__(
        self, checkout: CheckoutInfo, existing_checkout: config_mod.EdenCheckout
    ) -> None:
        super().__init__(
            f"""\
edenfs reports that checkout {checkout.path} is created within backing repo of an existing checkout {existing_checkout.path} (backing repo: {existing_checkout.get_backing_repo_path()})
Checkouts inside backing repo are usually not intended and can cause spurious behavior.""",
            f"Consider running `eden rm {checkout.path}` to remove misplaced repo(s)\n",
            severity=ProblemSeverity.ADVICE,
        )


class EdenCheckoutCorruption(Problem):
    def __init__(self, checkout: CheckoutInfo, ex: Exception) -> None:
        remediation = get_reclone_msg(str(checkout.path))

        super().__init__(
            f"Eden's checkout state for {checkout.path} has been corrupted: {ex}",
            remediation=remediation,
        )


class EdenCheckoutInfosCorruption(Problem):
    def __init__(self, ex: Exception) -> None:
        remediation = get_reclone_msg("$CHECKOUT_PATH")

        super().__init__(
            f"Encountered errors reading Eden's checkout info for the following checkouts:\n{ex}",
            remediation=remediation,
        )


def check_mount(
    out: ui.Output,
    tracker: ProblemTracker,
    instance: EdenInstance,
    checkout: CheckoutInfo,
    mount_table: mtab.MountTable,
    watchman_info: check_watchman.WatchmanCheckInfo,
    all_checkouts: List[CheckoutInfo],
    checked_backing_repos: Set[str],
    debug: bool,
    fast: bool,
) -> None:
    if checkout.state is None:
        # This checkout is configured but not currently running.
        tracker.add_problem(
            CheckoutNotMounted(out, checkout, all_checkouts, checked_backing_repos)
        )
    elif checkout.state == MountState.RUNNING:
        try:
            check_running_mount(
                tracker,
                instance,
                checkout,
                mount_table,
                watchman_info,
                debug,
                fast,
            )
        except Exception as ex:
            raise RuntimeError("Failed to check running mount") from ex
    elif checkout.state in (
        MountState.UNINITIALIZED,
        MountState.INITIALIZING,
        MountState.INITIALIZED,
        MountState.STARTING,
    ):
        try:
            check_starting_mount(
                tracker,
                instance,
                checkout,
                mount_table,
                watchman_info,
                debug,
                fast,
            )
        except Exception as ex:
            raise RuntimeError("Failed to check initializing/starting mount") from ex
    elif checkout.state in (
        MountState.SHUTTING_DOWN,
        MountState.SHUT_DOWN,
        MountState.DESTROYING,
    ):
        tracker.add_problem(CheckoutIsShuttingDown(checkout))
    elif checkout.state == MountState.FUSE_ERROR:
        # TODO: We could potentially try automatically unmounting and remounting.
        # In general mounts shouldn't remain in this state for long, so we probably
        # don't need to worry too much about this case.
        tracker.add_problem(CheckoutFailedDuetoFuseError(checkout))
    else:
        tracker.add_problem(CheckoutInUnknownState(checkout))

    try:
        # Check if this checkout is nested inside another one
        problem_type, existing_checkout = config_mod.detect_checkout_path_problem(
            checkout.path,
            instance,
        )

        if problem_type is not None and existing_checkout is not None:
            if problem_type == config_mod.CheckoutPathProblemType.NESTED_CHECKOUT:
                tracker.add_problem(NestedCheckout(checkout, existing_checkout))
            if problem_type == config_mod.CheckoutPathProblemType.INSIDE_BACKING_REPO:
                tracker.add_problem(
                    CheckoutInsideBackingRepo(checkout, existing_checkout)
                )
    except Exception as ex:
        raise RuntimeError("Failed to detect nested checkout") from ex


def check_starting_mount(
    tracker: ProblemTracker,
    instance: EdenInstance,
    checkout_info: CheckoutInfo,
    mount_table: mtab.MountTable,
    watchman_info: check_watchman.WatchmanCheckInfo,
    debug: bool,
    fast: bool,
) -> None:
    checkout = checkout_info.get_checkout()
    try:
        checkout.get_config()
        checkout.get_snapshot()
    except Exception as ex:
        # Config file is missing or invalid
        tracker.add_problem(
            EdenCheckoutCorruption(
                checkout_info,
                ex,
            )
        )
        return
    # Return this if there are no other problems
    tracker.add_problem(CheckoutIsStartingUp(checkout_info))


def check_running_mount(
    tracker: ProblemTracker,
    instance: EdenInstance,
    checkout_info: CheckoutInfo,
    mount_table: mtab.MountTable,
    watchman_info: check_watchman.WatchmanCheckInfo,
    debug: bool,
    fast: bool,
) -> None:
    if checkout_info.configured_state_dir is None:
        tracker.add_problem(CheckoutNotConfigured(checkout_info))
        return
    elif checkout_info.configured_state_dir != checkout_info.running_state_dir:
        tracker.add_problem(CheckoutConfigurationMismatch(checkout_info))
        return

    checkout = checkout_info.get_checkout()
    try:
        config = checkout.get_config()
    except Exception as ex:
        # Config file is missing or invalid
        tracker.add_problem(
            EdenCheckoutCorruption(
                checkout_info,
                ex,
            )
        )
        # Just skip the remaining checks.
        # Most of them rely on values from the configuration.
        return

    try:
        check_filesystems.check_inode_counts(tracker, instance, checkout_info)
    except Exception as ex:
        raise RuntimeError("Failed to check inode counts for mount") from ex

    try:
        check_filesystems.check_using_nfs_path(tracker, checkout.path)
    except Exception as ex:
        raise RuntimeError("Failed to check if backing store is on NFS") from ex

    try:
        check_watchman.check_active_mount(tracker, str(checkout.path), watchman_info)
    except Exception as ex:
        raise RuntimeError("Failed to check watchman status for mount") from ex

    try:
        check_redirections.check_redirections(tracker, instance, checkout, mount_table)
    except Exception as ex:
        raise RuntimeError("Failed to check redirections for mount") from ex

    try:
        check_recent_writes.check_recent_writes(tracker, instance, debug)
    except Exception as ex:
        raise RuntimeError("Failed to check recent writes counts for mount") from ex

    if sys.platform == "win32":
        try:
            if not fast:
                check_filesystems.check_materialized_are_accessible(
                    tracker, instance, checkout, lambda p: os.lstat(p).st_mode
                )
        except Exception as ex:
            raise RuntimeError(
                "Failed to check if materialized files are accessible"
            ) from ex

        try:
            if not fast:
                check_filesystems.check_loaded_content(
                    tracker, instance, checkout, prjfs.PrjGetOnDiskFileState
                )
        except Exception as ex:
            raise RuntimeError("Failed to check loaded content integrity") from ex

    if config.scm_type in ["hg", "filteredhg"]:
        try:
            check_hg.check_hg(tracker, checkout)
        except RuntimeError as ex:
            tracker.add_problem(EdenCheckoutCorruption(checkout_info, ex))
            # Exit here but don't reraise since we're already reporting a problem.
            return
        except Exception as ex:
            raise RuntimeError("Failed to check Mercurial status") from ex

        try:
            check_filesystems.check_hg_status_match_hg_diff(tracker, instance, checkout)
        except Exception as ex:
            raise RuntimeError("Failed to compare `hg status` with `hg diff`") from ex

        try:
            if not fast:
                check_network.check_network(tracker, checkout)
        except Exception as ex:
            raise RuntimeError("Failed to check network for mount") from ex


class CheckoutNotConfigured(Problem):
    def __init__(self, checkout_info: CheckoutInfo) -> None:
        msg = (
            f"Checkout {checkout_info.path} is running but not "
            "listed in Eden's configuration file."
        )
        # TODO: Maybe we could use some better suggestions here, depending on
        # common cases that might lead to this situation.  (At the moment I believe this
        # can occur if `eden clone` fails to set up the .hg directory after mounting.)
        quoted_path = shlex.quote(str(checkout_info.path))
        remediation = (
            f'Running "eden unmount {quoted_path}" will unmount this checkout.'
        )
        super().__init__(msg, remediation)


class CheckoutConfigurationMismatch(Problem):
    def __init__(self, checkout_info: CheckoutInfo) -> None:
        msg = f"""\
The running configuration for {checkout_info.path} is different than "
the on-disk state in Eden's configuration file:
- Running state directory:    {checkout_info.running_state_dir}
- Configured state directory: {checkout_info.configured_state_dir}"""
        remediation = """\
Running `eden restart` will cause EdenFS to restart and use the data from the
on-disk configuration."""
        super().__init__(msg, remediation)


class CheckoutNotMounted(FixableProblem):
    _out: ui.Output
    _instance: EdenInstance
    _mount_path: Path
    _backing_repo: Path
    _all_checkouts: List[CheckoutInfo]
    _checked_backing_repos: Set[str]

    def __init__(
        self,
        out: ui.Output,
        checkout_info: CheckoutInfo,
        all_checkouts: List[CheckoutInfo],
        checked_backing_repos: Set[str],
    ) -> None:
        self._out = out
        self._instance = checkout_info.instance
        self._mount_path = checkout_info.path
        self._backing_repo = checkout_info.get_backing_repo()
        self._all_checkouts = all_checkouts
        self._checked_backing_repos = checked_backing_repos

    def description(self) -> str:
        return f"{self._mount_path} is not currently mounted"

    def dry_run_msg(self) -> str:
        return f"Would remount {self._mount_path}"

    def start_msg(self) -> str:
        return f"Remounting {self._mount_path}"

    def perform_fix(self) -> None:
        try:
            self._instance.mount(str(self._mount_path), False)
            return
        except Exception as ex:
            # eden corruption
            if "is too short for header" in str(ex):
                raise Exception(
                    f"""\
{ex}

{self._mount_path} appears to have been corrupted.
This can happen if your machine was hard-rebooted.
{get_reclone_msg(str(self._mount_path))}"""
                )

            # if it is not this ^ eden corruption then it could be hg
            # corruption that hg doctor could fix. Let's try hg doctor and then
            # retry the mount for this case.

        self._out.write(
            "\nMount failed. Running `hg doctor` in the backing repo and then "
            "will retry the mount.\n",
            flush=True,
        )
        result = hg_doctor_in_backing_repo(
            self._backing_repo,
            get_dependent_repos(self._backing_repo, self._all_checkouts),
            self._checked_backing_repos,
        )

        try:
            self._instance.mount(str(self._mount_path), False)
        except Exception as ex:
            # If the result is non None then hg tried to fix something. It must
            # not have succeeded all the way because the mount still failed.
            if result is not None:
                raise Exception(
                    f"""\
Failed to remount this mount with error:

{ex}

{result}"""
                )
            else:
                raise

    def check_fix(self) -> bool:
        """
        Check that mount return value is 1(already mounted) when rerunning command
        """
        try:
            mount_value = self._instance.mount(str(self._mount_path), False)
        except Exception as ex:
            """
            This should only happen if the mount becomes corrupted
            between the time of the remount and the time of the check.
            """
            self._out.write(
                f"\nAttempt to fix missing mount failed: {ex}.\n",
                flush=True,
            )
            return False
        if mount_value == 1:
            return True
        elif mount_value == 0:
            """
            This should only happen if the mount somehow gets unmounted
            between the time of the remount and the time of the check.
            """
            return True
        return False


class StaleWorkingDirectory(Problem):
    def __init__(self, msg: str) -> None:
        remediation = f"""\
Run "cd / && cd -" to update your shell's working directory."""
        super().__init__(msg, remediation)


def check_for_working_directory_problem() -> Optional[Problem]:
    # Report an issue if the working directory points to a stale mount point
    if working_directory_was_stale:
        msg = "Your current working directory appears to be a stale EdenFS mount point"
        return StaleWorkingDirectory(msg)

    # If the $PWD environment variable is set, confirm that it points our current
    # working directory.
    #
    # This helps catch problems where the current working directory has been replaced
    # with a new mount point but the user hasn't cd'ed into the new mount yet.  For
    # instance this can happen if the user cd'ed into a checkout directory before Eden
    # was running, and then started EdenFS.  The user will still need to cd again to see
    # the EdenFS checkout contents.
    pwd = os.environ.get("PWD")
    if pwd is None:
        # If $PWD isn't set we can't check anything else
        return None

    try:
        pwd_stat = os.stat(pwd)
        cwd_stat = os.stat(".")
    except Exception:
        # If we fail to stat either directory just ignore the error and don't report a
        # problem for now.  If we ever see a real issue in practice hit this scenario we
        # can add an appropriate error message at that point in time.
        #
        # We've already handled stale mounts above, and `os.stat()` checks normally
        # succeed in this case anyway.
        #
        # Users can get into situations where they don't have permissions to
        # stat the current working directory in some cases, but this should be very
        # rare and they will probably be able to identify the issue themselves in this
        # case.
        return None

    if (pwd_stat.st_dev, pwd_stat.st_ino) == (cwd_stat.st_dev, cwd_stat.st_ino):
        # Everything looks okay
        return None

    msg = """\
Your current working directory is out-of-date.
This can happen if you have (re)started EdenFS but your shell is still pointing to
the old directory from before the EdenFS checkouts were mounted.
"""
    return StaleWorkingDirectory(msg)


class BadEdenFsVersion(Problem):
    def __init__(self, running_version: str, reasons: List[str]) -> None:
        reasons_string = "\n    ".join(reasons)
        help_string = f"""\
The version of EdenFS that is running on your machine is:
    {running_version}
This version is known to have issue:
    {reasons_string}
"""

        remediation_string = 'Run `edenfsctl restart{"" if sys.platform == "win32" else " --graceful"}` to migrate to the newer version to avoid these issues.'
        super().__init__(
            dedent(help_string), remediation_string, severity=ProblemSeverity.ADVICE
        )


class OutOfDateVersion(Problem):
    def __init__(self, installed_version: str, running_version: str) -> None:
        help_string = f"""\
The version of EdenFS that is installed on your machine is:
    {installed_version}
but the version of EdenFS that is currently running is:
    {running_version}
"""

        remediation_string = """Consider running `edenfsctl restart --graceful` to migrate to the newer version,
which may have important bug fixes or performance improvements.
"""
        super().__init__(
            dedent(help_string),
            dedent(remediation_string),
            severity=ProblemSeverity.ADVICE,
        )


def check_edenfs_version(tracker: ProblemTracker, instance: EdenInstance) -> None:
    # get released version parts
    rver, release = instance.get_running_version_parts()
    if not rver or not release:
        # This could be a dev build that returns the empty
        # string for both of these values.
        return

    # check for bad eden fs running version
    bad_version_reasons_map = instance.get_known_bad_edenfs_versions()
    running_version = version.format_eden_version((rver, release))
    running_version_str = (
        f"fb.eden {running_version}"
        if sys.platform == "win32"
        else f"fb-eden-{running_version}.x86_64"
    )
    if running_version in bad_version_reasons_map:
        reasons = bad_version_reasons_map[running_version]
        tracker.add_problem(BadEdenFsVersion(running_version_str, reasons))
        # if bad version, don't check for out of date version
        return

    # get installed version parts
    iversion, irelease = version.get_current_version_parts()
    if not iversion or not irelease:
        # dev build of eden client returns empty strings here
        return

    # check if the running version is more than two weeks old
    iversion_date = version.date_from_version(iversion)
    rversion_date = version.date_from_version(rver)
    if not iversion_date or not rversion_date:
        return
    daysgap = iversion_date - rversion_date
    if daysgap.days < 14:
        return

    installed_version = version.format_eden_version((iversion, irelease))
    installed_version_str = (
        f"fb.eden {installed_version}"
        if sys.platform == "win32"
        else f"fb-eden-{installed_version}.x86_64"
    )
    tracker.add_problem(OutOfDateVersion(installed_version_str, running_version_str))


class SlowHgImportProblem(Problem):
    def __init__(self, max_fetch_duration: timedelta) -> None:
        super().__init__(
            description=f"Slow file download taking up to {format_approx_duration(max_fetch_duration)} observed",
            remediation="""\
Try:
- Running `hg debugnetwork`.
- Checking your network connection's performance.
- Running `eden top` to check whether downloads are making progress.""",
            severity=ProblemSeverity.ADVICE,
        )


def check_slow_hg_import(tracker: ProblemTracker, instance: EdenInstance) -> None:
    threshold_s = instance.get_config_int(
        "doctor.slow-hg-import-problem-threshold-seconds", 60
    )
    threshold = timedelta(seconds=threshold_s)

    with instance.get_thrift_client_legacy() as client:
        max_duration_us = client.getCounter("store.sapling.live_import.max_duration_us")

    max_duration = timedelta(microseconds=max_duration_us)
    if max_duration > threshold:
        tracker.add_problem(SlowHgImportProblem(max_duration))

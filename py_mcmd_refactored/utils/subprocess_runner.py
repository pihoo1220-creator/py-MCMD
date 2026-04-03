# # py_mcmd_refactored/utils/subprocess_runner.py

# """
# Subprocess execution adapter.

# Why this exists
# --------------
# Legacy code used:
#   - subprocess.Popen(..., shell=True)
#   - os.wait4(pid, ...)
#   - stdout redirection via '> out.dat'

# Refactor needs:
#   1) mockable interface (unit tests),
#   2) explicit cwd (no 'cd ... &&'),
#   3) explicit stdout redirection without shell.
# """

# from __future__ import annotations

# from dataclasses import dataclass
# from datetime import datetime
# from pathlib import Path
# from typing import Optional
# import subprocess


# @dataclass(frozen=True)
# class Command:
#     """A command to run without invoking a shell."""
#     argv: list[str]
#     cwd: Path
#     stdout_path: Path


# @dataclass
# class ProcessHandle:
#     """A handle to a started process."""
#     pid: int
#     command: Command
#     started_at: datetime
#     popen: Optional[subprocess.Popen] = None


# class SubprocessRunner:
#     """Runs commands and waits for completion."""

#     def __init__(self, *, dry_run: bool = False):
#         self.dry_run = bool(dry_run)

#     def start(self, cmd: Command) -> ProcessHandle:
#         """Start a subprocess.

#         In dry-run mode, creates stdout file and returns a dummy handle.
#         """
#         cmd.cwd.mkdir(parents=True, exist_ok=True)
#         cmd.stdout_path.parent.mkdir(parents=True, exist_ok=True)

#         if self.dry_run:
#             if not cmd.stdout_path.exists():
#                 cmd.stdout_path.write_text("[dry_run] subprocess not executed\n")
#             return ProcessHandle(pid=0, command=cmd, started_at=datetime.now(), popen=None)

#         out_fh = cmd.stdout_path.open("w", encoding="utf-8")
#         p = subprocess.Popen(
#             cmd.argv,
#             cwd=str(cmd.cwd),
#             stdout=out_fh,
#             stderr=subprocess.STDOUT,
#             text=True,
#         )
#         # keep handle alive until wait() closes it
#         p._py_mcmd_out_fh = out_fh  # type: ignore[attr-defined]
#         return ProcessHandle(pid=p.pid, command=cmd, started_at=datetime.now(), popen=p)

#     def wait(self, handle: ProcessHandle) -> int:
#         """Wait for completion and return exit code."""
#         if handle.popen is None:
#             return 0

#         rc = int(handle.popen.wait())
#         out_fh = getattr(handle.popen, "_py_mcmd_out_fh", None)
#         if out_fh is not None:
#             try:
#                 out_fh.close()
#             except Exception:
#                 pass
#         return rc

#     def run_and_wait(self, cmd: Command) -> int:
#         """Convenience helper for start+wait."""
#         h = self.start(cmd)
#         return self.wait(h)


# class DryRunSubprocessRunner(SubprocessRunner):
#     def __init__(self):
#         super().__init__(dry_run=True)



from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional
import os
import subprocess
import threading


@dataclass(frozen=True)
class Command:
    """A command to run without invoking a shell.

    Backward-compatible behavior:
    - if only `stdout_path` is provided, stdout is written directly to that file.
    - if `stdout_fifo_path` is provided, stdout is streamed to the FIFO and
      optionally mirrored to `stdout_disk_path`.
    """
    argv: list[str]
    cwd: Path
    stdout_path: Optional[Path] = None
    stdout_fifo_path: Optional[Path] = None
    stdout_disk_path: Optional[Path] = None

    def resolved_stdout_disk_path(self) -> Optional[Path]:
        return self.stdout_disk_path or self.stdout_path


@dataclass
class ProcessHandle:
    """A handle to a started process."""
    pid: int
    command: Command
    started_at: datetime
    popen: Optional[subprocess.Popen] = None


class SubprocessRunner:
    """Runs commands and waits for completion."""

    def __init__(self, *, dry_run: bool = False):
        self.dry_run = bool(dry_run)

    def _pump_stdout(
        self,
        pipe,
        *,
        fifo_path: Optional[Path],
        disk_path: Optional[Path],
    ) -> None:
        fifo_fh = None
        disk_fh = None
        try:
            if fifo_path is not None:
                if not fifo_path.exists():
                    raise FileNotFoundError(f"FIFO path does not exist: {fifo_path}")
                # O_RDWR avoids blocking if no reader is attached yet.
                fifo_fd = os.open(str(fifo_path), os.O_RDWR | os.O_NONBLOCK)
                fifo_fh = os.fdopen(fifo_fd, "wb", buffering=0)

            if disk_path is not None:
                disk_path.parent.mkdir(parents=True, exist_ok=True)
                disk_fh = disk_path.open("wb")

            while True:
                chunk = pipe.read(65536)
                if not chunk:
                    break
                if fifo_fh is not None:
                    fifo_fh.write(chunk)
                if disk_fh is not None:
                    disk_fh.write(chunk)

            if disk_fh is not None:
                disk_fh.flush()
        finally:
            try:
                pipe.close()
            except Exception:
                pass
            if fifo_fh is not None:
                try:
                    fifo_fh.close()
                except Exception:
                    pass
            if disk_fh is not None:
                try:
                    disk_fh.close()
                except Exception:
                    pass

    def start(self, cmd: Command) -> ProcessHandle:
        """Start a subprocess.

        Modes:
        - file redirect only: direct stdout -> file
        - FIFO mode: subprocess stdout -> PIPE -> tee(FIFO, optional disk)
        - dry-run: create the disk mirror if configured
        """
        cmd.cwd.mkdir(parents=True, exist_ok=True)

        disk_path = cmd.resolved_stdout_disk_path()
        if disk_path is not None:
            disk_path.parent.mkdir(parents=True, exist_ok=True)

        if cmd.stdout_fifo_path is not None:
            cmd.stdout_fifo_path.parent.mkdir(parents=True, exist_ok=True)

        if self.dry_run:
            if disk_path is not None and not disk_path.exists():
                disk_path.write_text("[dry_run] subprocess not executed\n", encoding="utf-8")
            return ProcessHandle(pid=0, command=cmd, started_at=datetime.now(), popen=None)

        # Backward-compatible direct-file mode
        if cmd.stdout_fifo_path is None:
            if disk_path is None:
                raise ValueError("Command must define either stdout_path or stdout_fifo_path")
            out_fh = disk_path.open("w", encoding="utf-8")
            p = subprocess.Popen(
                cmd.argv,
                cwd=str(cmd.cwd),
                stdout=out_fh,
                stderr=subprocess.STDOUT,
                text=True,
            )
            p._py_mcmd_out_fh = out_fh  # type: ignore[attr-defined]
            return ProcessHandle(pid=p.pid, command=cmd, started_at=datetime.now(), popen=p)

        # FIFO tee mode
        p = subprocess.Popen(
            cmd.argv,
            cwd=str(cmd.cwd),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=False,
            bufsize=0,
        )
        pump_thread = threading.Thread(
            target=self._pump_stdout,
            kwargs={
                "pipe": p.stdout,
                "fifo_path": cmd.stdout_fifo_path,
                "disk_path": disk_path,
            },
            daemon=True,
        )
        pump_thread.start()
        p._py_mcmd_pump_thread = pump_thread  # type: ignore[attr-defined]
        return ProcessHandle(pid=p.pid, command=cmd, started_at=datetime.now(), popen=p)

    def wait(self, handle: ProcessHandle) -> int:
        """Wait for completion and return exit code."""
        if handle.popen is None:
            return 0

        rc = int(handle.popen.wait())

        pump_thread = getattr(handle.popen, "_py_mcmd_pump_thread", None)
        if pump_thread is not None:
            pump_thread.join()

        out_fh = getattr(handle.popen, "_py_mcmd_out_fh", None)
        if out_fh is not None:
            try:
                out_fh.close()
            except Exception:
                pass
        return rc

    def run_and_wait(self, cmd: Command) -> int:
        h = self.start(cmd)
        return self.wait(h)


class DryRunSubprocessRunner(SubprocessRunner):
    def __init__(self):
        super().__init__(dry_run=True)
"""Operations pane: launch DB-writing commands with progress + cancel.

Commands run in background worker threads that shell out to the same
CLI subcommands the user would invoke manually. Each running command
gets a card with the command line, a live status line, and a cancel
binding.
"""

from __future__ import annotations

import contextlib
import shlex
import subprocess
from dataclasses import dataclass, field
from datetime import UTC, datetime

from textual.app import ComposeResult
from textual.containers import Vertical, VerticalScroll
from textual.screen import Screen
from textual.widgets import Input, Static

from ..widgets import PageHeader


@dataclass
class OpsJob:
    cmd: list[str]
    proc: subprocess.Popen | None = None
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    finished_at: datetime | None = None
    returncode: int | None = None
    last_line: str = ""


class OpsScreen(Screen):
    """Launchpad + live status for long-running adsbtrack commands."""

    BINDINGS = [("escape", "back", "Back")]

    SUGGESTED_COMMANDS = [
        "adsbtrack fetch --hex <HEX> --source all --start 2026-01-01",
        "adsbtrack extract --hex <HEX> --reprocess",
        "adsbtrack enrich hex --hex <HEX>",
        "adsbtrack registry update",
        "adsbtrack acars --hex <HEX> --start 2026-01-01",
    ]

    def __init__(self) -> None:
        super().__init__()
        self._header = PageHeader("operations", crumb="fetch / extract / enrich / acars / registry")
        self._input = Input(placeholder="adsbtrack <command> ...   (enter to launch)", id="ops-input")
        self._jobs_panel = VerticalScroll(id="ops-jobs")
        self._suggestions = Static(
            "[#6b7885]examples:[/]\n  " + "\n  ".join(self.SUGGESTED_COMMANDS),
            id="ops-suggestions",
        )
        self._jobs: list[OpsJob] = []
        self._job_widgets: dict[int, Static] = {}

    def compose(self) -> ComposeResult:
        with Vertical():
            yield self._header
            yield self._input
            yield self._suggestions
            yield self._jobs_panel

    def on_mount(self) -> None:
        self.set_interval(0.5, self._poll)

    def on_input_submitted(self, event: Input.Submitted) -> None:  # type: ignore[override]
        line = event.value.strip()
        if not line:
            return
        self._launch(line)
        self._input.value = ""

    def action_back(self) -> None:
        self.app.pop_screen()

    # --- job bookkeeping ---

    def _launch(self, line: str) -> None:
        try:
            cmd = shlex.split(line)
        except ValueError as e:
            self._flash_error(f"bad command: {e}")
            return
        if not cmd:
            return
        # Prefix with `uv run` when the user invokes `adsbtrack` so the project
        # venv is used; otherwise run the command as-is.
        if cmd[0] == "adsbtrack":
            cmd = ["uv", "run", "-m", "adsbtrack.cli", *cmd[1:]]
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                cwd=str(self.app.project_root),
                text=True,
            )
        except FileNotFoundError as e:
            self._flash_error(f"launch failed: {e}")
            return
        job = OpsJob(cmd=cmd, proc=proc)
        self._jobs.append(job)
        idx = len(self._jobs) - 1
        widget = Static(self._render_job(idx, job), id=f"ops-job-{idx}")
        self._job_widgets[idx] = widget
        self._jobs_panel.mount(widget)

    def _flash_error(self, text: str) -> None:
        widget = Static(f"[#e0433a]{text}[/]")
        self._jobs_panel.mount(widget)

    def _poll(self) -> None:
        for idx, job in enumerate(self._jobs):
            if job.proc is None:
                continue
            if job.returncode is not None:
                continue
            rc = job.proc.poll()
            if job.proc.stdout is not None:
                with contextlib.suppress(Exception):
                    job.proc.stdout.flush()
                while True:
                    line = job.proc.stdout.readline()
                    if not line:
                        break
                    job.last_line = line.rstrip()
            if rc is not None:
                job.returncode = rc
                job.finished_at = datetime.now(UTC)
            if idx in self._job_widgets:
                self._job_widgets[idx].update(self._render_job(idx, job))

    def _render_job(self, idx: int, job: OpsJob) -> str:
        cmd = " ".join(job.cmd)
        if job.returncode is None:
            status = "[#f2b136]running[/]"
        elif job.returncode == 0:
            status = "[#4ec07a]done[/]"
        else:
            status = f"[#e0433a]exit {job.returncode}[/]"
        elapsed = (datetime.now(UTC) - job.started_at).total_seconds()
        last = job.last_line or "(no output yet)"
        return f"[#4fb8e0]#{idx:02d}[/]  [b]{cmd}[/b]  {status}  [#6b7885]{elapsed:.0f}s[/]\n     [#6b7885]{last}[/]"

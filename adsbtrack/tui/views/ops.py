"""Operations pane: launch DB-writing adsbtrack commands with live status.

Renders each active / completed job as a ``Card`` inside a 2-column
``Grid`` to mirror the concept's ``#ops`` layout. The input row sits
at the top; quick suggestions render below the input.
"""

from __future__ import annotations

import contextlib
import re
import shlex
import subprocess
from dataclasses import dataclass, field
from datetime import UTC, datetime

from rich.text import Text
from textual.app import ComposeResult
from textual.containers import Grid, Vertical
from textual.widgets import Input, Static

from ..widgets import (
    ACCENT_AMBER,
    ACCENT_CYAN,
    ACCENT_OK,
    ACCENT_RED,
    Card,
    DOT,
    FG_0,
    FG_1,
    FG_2,
    PageHeader,
)


@dataclass
class OpsJob:
    cmd: list[str]
    proc: subprocess.Popen | None = None
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    finished_at: datetime | None = None
    returncode: int | None = None
    last_line: str = ""
    progress: float | None = None  # 0..1 parsed from "N / M" output if present


_PROGRESS_RE = re.compile(r"(\d[\d,]*)\s*/\s*(\d[\d,]*)")


def _parse_progress(line: str) -> float | None:
    m = _PROGRESS_RE.search(line)
    if not m:
        return None
    try:
        done = int(m.group(1).replace(",", ""))
        total = int(m.group(2).replace(",", ""))
    except ValueError:
        return None
    if total <= 0:
        return None
    return max(0.0, min(1.0, done / total))


class OpsView(Vertical):
    """Launch + monitor long-running adsbtrack commands."""

    SUGGESTED_COMMANDS = [
        "adsbtrack fetch --hex <HEX> --source all --start 2026-01-01",
        "adsbtrack extract --hex <HEX> --reprocess",
        "adsbtrack enrich hex --hex <HEX>",
        "adsbtrack registry update",
        "adsbtrack acars --hex <HEX> --start 2026-01-01",
    ]

    def __init__(self) -> None:
        super().__init__(id="view-ops")
        self._header = PageHeader(
            "operations",
            crumb="fetch / extract / enrich / acars / registry",
            widget_id="ops-header",
        )
        self._input = Input(placeholder="adsbtrack <command> ...   (enter to launch)", id="ops-input")
        self._suggestions = Static(
            Text.from_markup(
                f"[{FG_2}]examples:[/]  "
                + f"  [{FG_2}]{DOT}[/]  ".join(f"[{FG_1}]{cmd}[/]" for cmd in self.SUGGESTED_COMMANDS[:3])
            ),
            id="ops-suggestions",
        )
        self._grid = Grid(id="ops-grid")
        self._jobs: list[OpsJob] = []
        self._job_cards: dict[int, Card] = {}

    def compose(self) -> ComposeResult:
        yield self._header
        yield self._input
        yield self._suggestions
        yield self._grid

    def on_mount(self) -> None:
        self.set_interval(0.5, self._poll)

    def on_input_submitted(self, event: Input.Submitted) -> None:
        line = event.value.strip()
        if not line:
            return
        self._launch(line)
        self._input.value = ""

    def focus_filter(self) -> None:
        self._input.focus()

    # --- job bookkeeping ---

    def _launch(self, line: str) -> None:
        try:
            cmd = shlex.split(line)
        except ValueError as e:
            self._flash_error(f"bad command: {e}")
            return
        if not cmd:
            return
        if cmd[0] == "adsbtrack":
            cmd = ["uv", "run", "python", "-m", "adsbtrack.cli", *cmd[1:]]
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
        card = Card(self._render_job(idx, job), id=f"ops-job-{idx}")
        self._job_cards[idx] = card
        self._grid.mount(card)

    def _flash_error(self, text: str) -> None:
        card = Card(Text.from_markup(f"[{ACCENT_RED}]{text}[/]"))
        self._grid.mount(card)

    def _poll(self) -> None:
        for idx, job in enumerate(self._jobs):
            if job.proc is None or job.returncode is not None:
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
                    parsed = _parse_progress(job.last_line)
                    if parsed is not None:
                        job.progress = parsed
            if rc is not None:
                job.returncode = rc
                job.finished_at = datetime.now(UTC)
            if idx in self._job_cards:
                self._job_cards[idx].update(self._render_job(idx, job))

    def _render_job(self, idx: int, job: OpsJob) -> Text:
        cmd_tail = " ".join(job.cmd[4:] if job.cmd[:4] == ["uv", "run", "python", "-m"] else job.cmd)
        heading = cmd_tail.split(" ", 1)[0] if cmd_tail else "adsbtrack"
        args = cmd_tail[len(heading) :].strip()
        if job.returncode is None:
            status_colour = ACCENT_AMBER
            status_text = f"running  {DOT} {job.progress * 100:.0f}%" if job.progress is not None else "running"
            value_colour = ACCENT_AMBER
            value_text = status_text if job.progress is None else f"{int(job.progress * 100)}%"
        elif job.returncode == 0:
            status_colour = ACCENT_OK
            status_text = "done"
            value_colour = ACCENT_OK
            value_text = "done"
        else:
            status_colour = ACCENT_RED
            status_text = f"exit {job.returncode}"
            value_colour = ACCENT_RED
            value_text = status_text
        elapsed = (datetime.now(UTC) - job.started_at).total_seconds()
        last = job.last_line or "(no output yet)"
        bar = self._progress_bar(job, status_colour)
        hint = f"[{FG_2}][[/][{ACCENT_CYAN}]c[/][{FG_2}]] cancel  [/][{FG_2}][[/][{ACCENT_CYAN}]b[/][{FG_2}]] background  [/][{FG_2}][[/][{ACCENT_CYAN}]l[/][{FG_2}]] live log[/]"
        return Text.from_markup(
            f"[{FG_2}]#{idx:02d}  {heading.upper()}[/]\n"
            f"[b {value_colour}]{value_text}[/]\n"
            f"[{FG_2}]{args or '--'}  {DOT}  {elapsed:.0f}s  {DOT}  [/]"
            f"[{status_colour}]{status_text}[/]\n"
            f"{bar}\n"
            f"[{FG_2}]> [/][{FG_1}]{last[:200]}[/]\n"
            f"{hint}"
        )

    @staticmethod
    def _progress_bar(job: OpsJob, colour: str) -> str:
        width = 40
        progress = job.progress if job.progress is not None else (1.0 if job.returncode == 0 else 0.0)
        fill = max(0, min(width, int(round(progress * width))))
        return f"[{colour}]{'█' * fill}[/][{FG_2}]{'░' * (width - fill)}[/]"

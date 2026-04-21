"""Reusable Textual widgets for the adsbtrack TUI."""

from __future__ import annotations

from datetime import UTC, datetime

from textual.containers import Horizontal
from textual.widget import Widget
from textual.widgets import Input, Label, Static


class StatusStrip(Static):
    """Top-of-screen status strip with DB path, counts, active job, UTC clock."""

    DEFAULT_CSS = ""

    def __init__(self, *, db_path: str, flights: int, aircraft: int) -> None:
        super().__init__(id="status-strip")
        self._db_path = db_path
        self._flights = flights
        self._aircraft = aircraft
        self._job: str | None = None
        self._clock = datetime.now(UTC).strftime("%H:%M:%SZ")

    def on_mount(self) -> None:
        self._render()
        self.set_interval(1.0, self._tick)

    def set_job(self, text: str | None) -> None:
        self._job = text
        self._render()

    def set_counts(self, flights: int, aircraft: int) -> None:
        self._flights = flights
        self._aircraft = aircraft
        self._render()

    def _tick(self) -> None:
        self._clock = datetime.now(UTC).strftime("%H:%M:%SZ")
        self._render()

    def _render(self) -> None:
        parts = [
            "[b]adsbtrack[/b]",
            f"[#6b7885]{self._db_path}[/]",
            f"[#6b7885]flights {self._flights:,}[/]",
            f"[#6b7885]aircraft {self._aircraft:,}[/]",
        ]
        if self._job:
            parts.append(f"[#f2b136]{self._job}[/]")
        parts.append(f"[#4fb8e0]{self._clock}[/]")
        self.update("  ".join(parts))


class PageHeader(Static):
    """Per-screen header: title, breadcrumb, trailing dim detail."""

    def __init__(self, title: str, crumb: str = "", trailing: str = "", *, widget_id: str | None = None) -> None:
        super().__init__(id=widget_id)
        self._title = title
        self._crumb = crumb
        self._trailing = trailing

    def on_mount(self) -> None:
        self._render()

    def set_title(self, title: str) -> None:
        self._title = title
        self._render()

    def set_crumb(self, crumb: str) -> None:
        self._crumb = crumb
        self._render()

    def set_trailing(self, trailing: str) -> None:
        self._trailing = trailing
        self._render()

    def _render(self) -> None:
        parts = [f"[b]{self._title}[/b]"]
        if self._crumb:
            parts.append(f"[#6b7885]> {self._crumb}[/]")
        line = "  ".join(parts)
        if self._trailing:
            line = f"{line}   [#6b7885]{self._trailing}[/]"
        self.update(line)


class FilterBar(Widget):
    """fzf-style filter bar: `>` prompt + Input + count label."""

    DEFAULT_CSS = """
    FilterBar {
        layout: horizontal;
        height: 1;
    }
    FilterBar Input {
        border: none;
        padding: 0 1;
        height: 1;
    }
    """

    def __init__(self, placeholder: str = "filter (fzf)", *, widget_id: str | None = None) -> None:
        super().__init__(id=widget_id)
        self._placeholder = placeholder
        self._total = 0
        self._matched = 0

    def compose(self):  # type: ignore[override]
        yield Label("> ", classes="prompt")
        yield Input(placeholder=self._placeholder, id=f"{self.id or 'filter'}-input")
        yield Label("0 / 0", classes="count", id=f"{self.id or 'filter'}-count")

    def set_counts(self, matched: int, total: int) -> None:
        self._matched = matched
        self._total = total
        label = self.query_one(f"#{self.id or 'filter'}-count", Label)
        label.update(f"[#6b7885]{matched:,} / {total:,}[/]")

    @property
    def input_widget(self) -> Input:
        return self.query_one(f"#{self.id or 'filter'}-input", Input)


class HorizontalGroup(Horizontal):
    """Thin alias so screens can opt into a horizontal container without
    importing the textual.containers module directly."""

"""Reusable Textual widgets for the adsbtrack TUI.

Every widget here is a Static or a Widget subclass that renders via
Rich markup. The design tokens are mirrored from
``design/colors_and_type.css`` into hex literals in the markup so the
terminal output stays aligned with the GUI export and the preview
cards. When a token changes, update it here and in
``adsbtrack/tui/styles/app.tcss``.
"""

from __future__ import annotations

from datetime import UTC, datetime

from rich.text import Text
from textual.app import ComposeResult
from textual.containers import Horizontal
from textual.reactive import reactive
from textual.widget import Widget
from textual.widgets import Input, Label

# ---- colour tokens (mirror of design/colors_and_type.css) ----

FG_0 = "#e4ecf3"
FG_1 = "#aab6c2"
FG_2 = "#6b7885"
BD_0 = "#222c37"

ACCENT_CYAN = "#4fb8e0"
ACCENT_OK = "#4ec07a"
ACCENT_AMBER = "#f2b136"
ACCENT_RED = "#e0433a"
ACCENT_VIOLET = "#c24bd6"


def pill_markup(label: str, colour: str) -> str:
    """Return Rich markup for a single inline pill: coloured text padded with spaces.

    Terminals render this as a solid coloured rectangle carrying the
    label in the accent colour's foreground (inverted by Rich's ``on``
    + foreground combination). Spaces pad the label so the pill reads
    as a block, not a typographic glyph.
    """
    return f"[{FG_0} on {colour}] {label} [/]"


def pill_outline(label: str, colour: str) -> str:
    """Return Rich markup for an outlined pill: coloured text with accent foreground.

    When the accent itself is the brand signal (e.g. SPF / MIL on the
    aircraft list), an outlined pill reads as a badge without the
    solid-block weight of ``pill_markup``. Rendered as ``[colour]``
    wrapping the label with square brackets that mimic the design's
    1px borders.
    """
    return f"[{colour}]\\[{label}][/]"


# ---------------------------------------------------------------------------
# Status strip: single top bar with brand, DB, counts, active job, UTC clock.
# ---------------------------------------------------------------------------


class StatusStrip(Widget):
    """Top-of-screen status strip with DB path, counts, active job, UTC clock."""

    DEFAULT_CSS = """
    StatusStrip {
        height: 1;
        dock: top;
        content-align: left middle;
    }
    """

    _flights = reactive(0)
    _aircraft = reactive(0)
    _job: reactive[str | None] = reactive(None)
    _clock = reactive("--:--:--Z")

    def __init__(self, *, db_path: str, flights: int, aircraft: int) -> None:
        super().__init__(id="status-strip")
        self._db_path = db_path
        self._flights = flights
        self._aircraft = aircraft
        self._clock = datetime.now(UTC).strftime("%H:%M:%SZ")

    def on_mount(self) -> None:
        self.set_interval(1.0, self._tick)

    def set_job(self, text: str | None) -> None:
        self._job = text

    def set_counts(self, flights: int, aircraft: int) -> None:
        self._flights = flights
        self._aircraft = aircraft

    def _tick(self) -> None:
        self._clock = datetime.now(UTC).strftime("%H:%M:%SZ")

    def render(self) -> Text:
        left = (
            f"[b {FG_0}]adsbtrack[/]   "
            f"[{FG_2}]{self._db_path}[/]   "
            f"[{FG_2}]flights {self._flights:,}[/]   "
            f"[{FG_2}]aircraft {self._aircraft:,}[/]"
        )
        right_parts: list[str] = []
        if self._job:
            right_parts.append(f"[{ACCENT_AMBER}]{self._job}[/]")
        right_parts.append(f"[{ACCENT_CYAN}]{self._clock}[/]")
        return Text.from_markup(f"{left}{' ' * 20}{'   '.join(right_parts)}")


# ---------------------------------------------------------------------------
# Page header: per-view title + breadcrumb + trailing dim detail.
# ---------------------------------------------------------------------------


class PageHeader(Widget):
    """Per-screen header: title, breadcrumb, trailing dim detail."""

    DEFAULT_CSS = """
    PageHeader {
        height: 1;
        content-align: left middle;
    }
    """

    _title = reactive("")
    _crumb = reactive("")
    _trailing = reactive("")

    def __init__(self, title: str, crumb: str = "", trailing: str = "", *, widget_id: str | None = None) -> None:
        super().__init__(id=widget_id, classes="page-header")
        self._title = title
        self._crumb = crumb
        self._trailing = trailing

    def set_title(self, title: str) -> None:
        self._title = title

    def set_crumb(self, crumb: str) -> None:
        self._crumb = crumb

    def set_trailing(self, trailing: str) -> None:
        self._trailing = trailing

    def render(self) -> Text:
        parts = [f"[b {FG_0}]{self._title}[/]"]
        if self._crumb:
            parts.append(f"[{FG_2}]> {self._crumb}[/]")
        line = "   ".join(parts)
        if self._trailing:
            line = f"{line}      [{FG_2}]{self._trailing}[/]"
        return Text.from_markup(line)


# ---------------------------------------------------------------------------
# Filter bar: fzf-style ``>`` prompt + Input + count label.
# ---------------------------------------------------------------------------


class FilterBar(Widget):
    """Filter bar with a cyan ``>`` prompt, an Input, and a count label."""

    def __init__(self, placeholder: str = "filter (fzf)", *, widget_id: str | None = None) -> None:
        super().__init__(id=widget_id, classes="filter-bar")
        self._placeholder = placeholder

    def compose(self) -> ComposeResult:
        yield Label(">", classes="filter-prompt")
        yield Input(placeholder=self._placeholder, id=f"{self.id or 'filter'}-input")
        yield Label("0 / 0", classes="filter-count", id=f"{self.id or 'filter'}-count")

    def set_counts(self, matched: int, total: int) -> None:
        label = self.query_one(f"#{self.id or 'filter'}-count", Label)
        label.update(f"[{FG_2}]{matched:,} / {total:,}[/]")

    @property
    def input_widget(self) -> Input:
        return self.query_one(f"#{self.id or 'filter'}-input", Input)


# ---------------------------------------------------------------------------
# Sidebar: persistent nav. Views / Operations / Session groups with shortcut
# pills on the right.
# ---------------------------------------------------------------------------

_VIEWS: list[tuple[str, str, str]] = [
    ("aircraft", "Aircraft list", "1"),
    ("flights", "Flight timeline", "2"),
    ("events", "Event feed", "3"),
    ("spoof", "Spoofed broadcasts", "4"),
    ("map", "Map", "5"),
    ("status", "Status dashboard", "6"),
]

_OPS: list[tuple[str, str, str]] = [
    ("ops", "fetch", "f"),
    ("ops", "extract", ""),
    ("ops", "enrich", ""),
    ("ops", "acars", ""),
    ("ops", "registry", ""),
]

_SESSION: list[tuple[str, str, str]] = [
    ("", "Jump to hex", ":"),
    ("", "Help", "?"),
    ("", "Quit", "q"),
]


class Sidebar(Widget):
    """Left-hand navigation: 3 groups (Views / Operations / Session)."""

    DEFAULT_CSS = """
    Sidebar {
        width: 24;
        height: 1fr;
        dock: left;
    }
    """

    _active = reactive("aircraft")

    def __init__(self) -> None:
        super().__init__(id="sidebar")

    def set_active(self, view_id: str) -> None:
        self._active = view_id

    def render(self) -> Text:
        lines: list[str] = []
        lines.append(f"[{FG_2}]VIEWS[/]")
        lines.extend(self._render_items(_VIEWS, highlight=self._active))
        lines.append("")
        lines.append(f"[{FG_2}]OPERATIONS[/]")
        lines.extend(self._render_items(_OPS, highlight=self._active))
        lines.append("")
        lines.append(f"[{FG_2}]SESSION[/]")
        lines.extend(self._render_items(_SESSION, highlight=None))
        return Text.from_markup("\n".join(lines))

    @staticmethod
    def _render_items(items: list[tuple[str, str, str]], *, highlight: str | None) -> list[str]:
        out: list[str] = []
        for view_id, label, shortcut in items:
            is_active = highlight is not None and view_id == highlight
            label_colour = FG_0 if is_active else FG_1
            marker = f"[{ACCENT_CYAN}]|[/] " if is_active else "  "
            shortcut_cell = f" [{FG_2}]{shortcut}[/]" if shortcut else ""
            out.append(f"{marker}[{label_colour}]{label:<20}[/]{shortcut_cell}")
        return out


# ---------------------------------------------------------------------------
# HorizontalGroup alias (kept for external imports).
# ---------------------------------------------------------------------------


class HorizontalGroup(Horizontal):
    """Thin alias so screens/views can opt into a horizontal container without
    importing the textual.containers module directly."""


# ---------------------------------------------------------------------------
# DataTable cell helpers - produce styled Rich Text ready to drop into
# DataTable.add_row(). Every cell is a rich.text.Text so DataTable does
# not re-escape embedded markup.
# ---------------------------------------------------------------------------


def cell(text: str, *, style: str | None = None) -> Text:
    """Return a Rich Text cell with an optional style string."""
    return Text(text, style=style or "")


def num_cell(text: str, *, style: str | None = None) -> Text:
    """Right-aligned numeric cell with tabular numerics."""
    return Text(text, style=style or "", justify="right")


def dash() -> Text:
    """Placeholder cell for missing values."""
    return Text("--", style=FG_2)

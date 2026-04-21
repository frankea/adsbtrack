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


_PILL_BG = {
    # Tinted backgrounds mirroring the CSS --accent-*-bg swatches
    # (rgba at ~12% opacity). Terminal paints these as the named
    # accent hue dimmed; the outlined effect comes from setting
    # foreground = full accent and background = dimmed accent.
    "#e0433a": "#2a1413",  # red
    "#f2b136": "#2d2210",  # amber
    "#c24bd6": "#291433",  # violet
    "#4ec07a": "#142c1d",  # ok
    "#4fb8e0": "#102b37",  # cyan
}


def pill_markup(label: str, colour: str) -> str:
    """Return Rich markup for an outlined pill.

    Border + tint is simulated in the terminal by setting the foreground
    to the accent colour and the background to a dimmed version of the
    same accent. Matches the design's outlined-with-tinted-bg pill style.
    """
    bg = _PILL_BG.get(colour, "#1c242e")
    return f"[{colour} on {bg}] {label} [/]"


def pill_solid(label: str, colour: str) -> str:
    """Return Rich markup for a solid pill (accent as background, white fg).

    Used where the pill itself is the meaning (e.g. the SEV column in the
    event feed) rather than a badge attached to something else.
    """
    return f"[{FG_0} on {colour}] {label} [/]"


# ---------------------------------------------------------------------------
# Status strip: single top bar with brand, DB, counts, active job, UTC clock.
# ---------------------------------------------------------------------------


class StatusStrip(Label):
    """Top-of-screen status strip with DB path, counts, active job, UTC clock.

    Implemented as a ``Label`` subclass (not a bare ``Widget``) because
    ``Label`` sets its content via the base-class renderable protocol,
    which means Textual's compositor computes a content height of 1
    from our single-line markup. A ``Widget`` with ``render()`` returning
    a ``Text`` renders but reports a content height of 0, which hides it.
    """

    DEFAULT_CSS = """
    StatusStrip {
        height: 1;
        width: 1fr;
        background: #141b23;
        color: #aab6c2;
    }
    """

    def __init__(self, *, db_path: str, flights: int, aircraft: int) -> None:
        self._db_path = db_path
        self._flights = flights
        self._aircraft = aircraft
        self._job: str | None = None
        self._clock = datetime.now(UTC).strftime("%H:%M:%SZ")
        super().__init__(self._build(), id="status-strip")

    def on_mount(self) -> None:
        self.set_interval(1.0, self._tick)

    def set_job(self, text: str | None) -> None:
        self._job = text
        self.update(self._build())

    def set_counts(self, flights: int, aircraft: int) -> None:
        self._flights = flights
        self._aircraft = aircraft
        self.update(self._build())

    def _tick(self) -> None:
        self._clock = datetime.now(UTC).strftime("%H:%M:%SZ")
        self.update(self._build())

    def _build(self) -> Text:
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


class PageHeader(Label):
    """Per-screen header: title, breadcrumb, trailing dim detail.

    Label subclass for the same compositor-height reason StatusStrip
    documents above.
    """

    DEFAULT_CSS = """
    PageHeader {
        height: 1;
        width: 1fr;
        background: #0b0f14;
        color: #e4ecf3;
        padding: 0 1;
    }
    """

    def __init__(self, title: str, crumb: str = "", trailing: str = "", *, widget_id: str | None = None) -> None:
        self._title = title
        self._crumb = crumb
        self._trailing = trailing
        super().__init__(self._build(), id=widget_id, classes="page-header")

    def set_title(self, title: str) -> None:
        self._title = title
        self.update(self._build())

    def set_crumb(self, crumb: str) -> None:
        self._crumb = crumb
        self.update(self._build())

    def set_trailing(self, trailing: str) -> None:
        self._trailing = trailing
        self.update(self._build())

    def _build(self) -> Text:
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

    DEFAULT_CSS = """
    FilterBar {
        layout: horizontal;
        height: 1;
        width: 1fr;
        background: #0b0f14;
    }
    FilterBar > Label.filter-prompt {
        width: 3;
        padding: 0 1;
        color: #4fb8e0;
        text-style: bold;
    }
    FilterBar > Input {
        background: #0b0f14;
        color: #e4ecf3;
        border: none;
        padding: 0 1;
        height: 1;
        width: 1fr;
    }
    FilterBar > Label.filter-count {
        width: auto;
        padding: 0 1;
        color: #6b7885;
    }
    """

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


class Sidebar(Label):
    """Left-hand navigation: 3 groups (Views / Operations / Session).

    Label subclass for compositor-height reasons documented on StatusStrip.
    """

    DEFAULT_CSS = """
    Sidebar {
        width: 24;
        height: 1fr;
        background: #141b23;
        color: #aab6c2;
        border-right: solid #222c37;
        padding: 1 1;
    }
    """

    def __init__(self) -> None:
        self._active = "aircraft"
        super().__init__(self._build(), id="sidebar")

    def set_active(self, view_id: str) -> None:
        self._active = view_id
        self.update(self._build())

    def _build(self) -> Text:
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

    # Sidebar width is 24 cols, padding 1 on each side -> 22 usable.
    # Layout per row: marker (2) + label + shortcut-box (3) = 22.
    _LABEL_WIDTH = 17
    _ROW_WIDTH = 22

    @classmethod
    def _render_items(cls, items: list[tuple[str, str, str]], *, highlight: str | None) -> list[str]:
        out: list[str] = []
        for view_id, label, shortcut in items:
            is_active = highlight is not None and view_id == highlight
            label_colour = FG_0 if is_active else FG_1
            marker = f"[{ACCENT_CYAN}]│[/] " if is_active else "  "
            # Truncate to available width if a label is unexpectedly long.
            label_text = label[: cls._LABEL_WIDTH]
            shortcut_cell = (
                f"[{FG_2} on #1c242e] {shortcut} [/]"
                if shortcut
                else "   "  # Same width as the pill so all rows line up.
            )
            # Pad label out so the shortcut lands at the right edge.
            padded = f"{label_text:<{cls._LABEL_WIDTH}}"
            out.append(f"{marker}[{label_colour}]{padded}[/]{shortcut_cell}")
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

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
from textual.css.query import NoMatches
from textual.widgets import Input, Label, Static

# ---- colour tokens (mirror of design/colors_and_type.css) ----

BG_0 = "#0b0f14"
BG_1 = "#141b23"
BG_2 = "#1c242e"
BG_3 = "#232d39"

FG_0 = "#e4ecf3"
FG_1 = "#aab6c2"
FG_2 = "#6b7885"
FG_3 = "#4a5561"

BD_0 = "#222c37"
BD_1 = "#2d3946"

ACCENT_CYAN = "#4fb8e0"
ACCENT_OK = "#4ec07a"
ACCENT_AMBER = "#f2b136"
ACCENT_RED = "#e0433a"
ACCENT_VIOLET = "#c24bd6"
ACCENT_MAGENTA = "#d47bd4"

# Approx --overlay-selected on bg-0 after cyan rgba blend: a cold-teal tint.
OVERLAY_SELECTED = "#102b37"
OVERLAY_HOVER = "#141a22"
OVERLAY_STRIPE = "#0e131a"

# Middle-dot glyph used across all page chrome per the design spec.
DOT = "·"


_PILL_BG = {
    # Tinted backgrounds mirroring the CSS --accent-*-bg swatches
    # (rgba at ~12% opacity). Terminal paints these as the named
    # accent hue dimmed; the outlined effect comes from setting
    # foreground = full accent and background = dimmed accent.
    ACCENT_RED: "#2a1413",
    ACCENT_AMBER: "#2d2210",
    ACCENT_VIOLET: "#291433",
    ACCENT_OK: "#142c1d",
    ACCENT_CYAN: "#102b37",
    FG_2: BG_2,
}


def pill_markup(label: str, colour: str) -> str:
    """Return Rich markup for a tinted-background pill: accent-hued text
    on a dimmed version of the same accent. No terminal border is drawn;
    the separation from surrounding text comes from the background tint.
    Pair with ``pill_solid`` when the pill itself carries the meaning.
    """
    bg = _PILL_BG.get(colour, BG_2)
    return f"[{colour} on {bg}] {label} [/]"


def pill_solid(label: str, colour: str) -> str:
    """Return Rich markup for a solid pill (accent as background, white fg).

    Used where the pill itself is the meaning (e.g. the SEV column in the
    event feed) rather than a badge attached to something else.
    """
    return f"[{FG_0} on {colour}] {label} [/]"


def _widget_width(widget, fallback: int = 120) -> int:
    """Return a sensible current width for layout math.

    Accessing ``widget.size`` before the widget is mounted raises
    because Textual resolves it through ``self.screen.find_widget()``.
    We want ``_build()`` to run safely during ``__init__`` (before
    mount) and again on ``on_resize`` (after mount), so trap any
    lookup error and fall back to a wide default that'll be corrected
    on the first resize callback.
    """
    try:
        w = widget.size.width
    except (RuntimeError, NoMatches):
        return fallback
    return w if w else fallback


def fmt_bytes(n: int) -> str:
    """Format a byte count like the concept: ``3.4 GB``, ``412 MB``."""
    if n <= 0:
        return "0 B"
    for unit, threshold in (("GB", 1_000_000_000), ("MB", 1_000_000), ("KB", 1_000)):
        if n >= threshold:
            return f"{n / threshold:.1f} {unit}"
    return f"{n} B"


# ---------------------------------------------------------------------------
# Status strip: single top bar with brand, DB, counts, traces, job, UTC clock.
# ---------------------------------------------------------------------------


class StatusStrip(Label):
    """Top-of-screen status strip matching ``ui_kits/tui/index.html`` topbar.

    Implemented as a ``Label`` subclass (not a bare ``Widget``) because
    ``Label`` sets its content via the base-class renderable protocol,
    which means Textual's compositor computes a content height of 1
    from our single-line markup.
    """

    DEFAULT_CSS = f"""
    StatusStrip {{
        height: 1;
        width: 1fr;
        background: {BG_1};
        color: {FG_1};
        padding: 0 1;
    }}
    """

    def __init__(self, *, db_path: str, flights: int, aircraft: int, traces: int = 0) -> None:
        self._db_path = db_path
        self._flights = flights
        self._aircraft = aircraft
        self._traces = traces
        self._job: str | None = None
        self._clock = datetime.now(UTC).strftime("%H:%M:%SZ")
        super().__init__(self._build(), id="status-strip")

    def on_mount(self) -> None:
        self.set_interval(1.0, self._tick)

    def on_resize(self) -> None:
        self.update(self._build())

    def set_job(self, text: str | None) -> None:
        self._job = text
        self.update(self._build())

    def set_counts(self, flights: int, aircraft: int, traces: int | None = None) -> None:
        self._flights = flights
        self._aircraft = aircraft
        if traces is not None:
            self._traces = traces
        self.update(self._build())

    def set_traces(self, traces: int) -> None:
        self._traces = traces
        self.update(self._build())

    def _tick(self) -> None:
        self._clock = datetime.now(UTC).strftime("%H:%M:%SZ")
        self.update(self._build())

    def _build(self) -> Text:
        sep = f" [{FG_2}]{DOT}[/] "
        left = sep.join(
            [
                f"[b {FG_0}]adsbtrack[/]",
                f"[{FG_2}]{self._db_path}[/]",
                f"[{FG_2}]flights {self._flights:,}[/]",
                f"[{FG_2}]aircraft {self._aircraft:,}[/]",
                f"[{FG_2}]traces {fmt_bytes(self._traces)}[/]",
            ]
        )
        right_parts: list[str] = []
        if self._job:
            right_parts.append(f"[{ACCENT_AMBER}]{self._job}[/]")
        right_parts.append(f"[{ACCENT_CYAN}]{self._clock}[/]")
        # Right-alignment is handled by the Label expanding to 1fr width
        # and Rich's Text justification. We build the trailing pieces into
        # a Text so padding falls on the space between left and right.
        txt = Text.from_markup(left)
        right = Text.from_markup(sep.join(right_parts))
        total_width = max(1, _widget_width(self) - 2)
        gap = max(1, total_width - txt.cell_len - right.cell_len)
        txt.append(" " * gap)
        txt.append(right)
        return txt


# ---------------------------------------------------------------------------
# Page header: per-view title + crumb + trailing dim detail.
# Dot-separated, right-aligned trailing info.
# ---------------------------------------------------------------------------


class PageHeader(Label):
    """Per-screen header: title, crumb, trailing dim detail.

    Label subclass for the same compositor-height reason StatusStrip
    documents above.
    """

    DEFAULT_CSS = f"""
    PageHeader {{
        height: 1;
        width: 1fr;
        background: {BG_0};
        color: {FG_0};
        padding: 0 1;
    }}
    """

    def __init__(
        self,
        title: str,
        crumb: str = "",
        trailing: str | Text = "",
        *,
        widget_id: str | None = None,
    ) -> None:
        self._title = title
        self._crumb = crumb
        self._trailing: str | Text = trailing
        super().__init__(self._build(), id=widget_id, classes="page-header")

    def on_resize(self) -> None:
        self.update(self._build())

    def set_title(self, title: str) -> None:
        self._title = title
        self.update(self._build())

    def set_crumb(self, crumb: str) -> None:
        self._crumb = crumb
        self.update(self._build())

    def set_trailing(self, trailing: str | Text) -> None:
        self._trailing = trailing
        self.update(self._build())

    def _build(self) -> Text:
        left_parts: list[str] = [f"[b {FG_0}]{self._title}[/]"]
        if self._crumb:
            left_parts.append(f"[{FG_2}]{DOT} {self._crumb}[/]")
        left = Text.from_markup("  ".join(left_parts))
        if isinstance(self._trailing, Text):
            right = self._trailing
        else:
            right = Text.from_markup(f"[{FG_2}]{self._trailing}[/]") if self._trailing else Text("")
        total_width = max(1, _widget_width(self) - 2)
        gap = max(2, total_width - left.cell_len - right.cell_len)
        out = Text()
        out.append(left)
        out.append(" " * gap)
        out.append(right)
        return out


# ---------------------------------------------------------------------------
# Filter bar: fzf-style ``>`` prompt + Input + count label.
# ---------------------------------------------------------------------------


class FilterBar:
    """Factory + handle for an fzf-style filter bar.

    NOT a widget subclass. Subclassing ``Horizontal`` (or wrapping a
    ``Horizontal`` in a ``Widget``) in this project broke Textual's
    ``Input`` paint path: the Input would accept keystrokes and update
    its value but render blank. The bare ``Horizontal`` returned by
    :meth:`build` side-steps that. ``set_counts`` and ``input_widget``
    remain available on this handle for screens to poke.
    """

    def __init__(self, placeholder: str = "filter (fzf)", *, widget_id: str | None = None) -> None:
        self._placeholder = placeholder
        self._widget_id = widget_id or "filter"
        self._input = Input(placeholder=placeholder, id=f"{self._widget_id}-input")
        self._count = Label("", classes="filter-count", id=f"{self._widget_id}-count")
        self._count.update(Text.from_markup(f"[{FG_2}]0 / 0[/]"))

    def build(self) -> Horizontal:
        return Horizontal(
            Label("›", classes="filter-prompt"),
            self._input,
            self._count,
            id=self._widget_id,
            classes="filter-bar",
        )

    def set_counts(self, matched: int, total: int) -> None:
        self._count.update(Text.from_markup(f"[{FG_2}]{matched:,} / {total:,}[/]"))

    @property
    def input_widget(self) -> Input:
        return self._input


# App-level CSS for the filter bar styling (applied by id/class because we no
# longer have a widget class to hang DEFAULT_CSS off of).
FILTER_BAR_CSS = f"""
Horizontal.filter-bar {{
    height: 1;
    width: 1fr;
    background: {BG_0};
}}
Horizontal.filter-bar Label.filter-prompt {{
    width: 3;
    padding: 0 1;
    color: {ACCENT_CYAN};
    text-style: bold;
}}
Horizontal.filter-bar Input {{
    background: {BG_0};
    color: {FG_0};
    border: none;
    padding: 0 1;
    width: 1fr;
}}
Horizontal.filter-bar Label.filter-count {{
    width: auto;
    min-width: 12;
    padding: 0 1;
    content-align: right middle;
    color: {FG_2};
}}
"""


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
    ("jump", "Jump to hex", ":"),
    ("help", "Help", "?"),
    ("quit", "Quit", "q"),
]


class Sidebar(Label):
    """Left-hand navigation: 3 groups (Views / Operations / Session).

    Label subclass for compositor-height reasons documented on StatusStrip.
    """

    DEFAULT_CSS = f"""
    Sidebar {{
        width: 24;
        height: 1fr;
        background: {BG_1};
        color: {FG_1};
        padding: 1 0;
    }}
    """

    def __init__(self) -> None:
        self._active = "aircraft"
        super().__init__(self._build(), id="sidebar")

    def set_active(self, view_id: str) -> None:
        self._active = view_id
        self.update(self._build())

    def _build(self) -> Text:
        lines: list[str] = []
        lines.append(f" [{FG_2}]VIEWS[/]")
        lines.extend(self._render_items(_VIEWS, highlight=self._active))
        lines.append("")
        lines.append(f" [{FG_2}]OPERATIONS[/]")
        lines.extend(self._render_items(_OPS, highlight=self._active))
        lines.append("")
        lines.append(f" [{FG_2}]SESSION[/]")
        lines.extend(self._render_items(_SESSION, highlight=None))
        return Text.from_markup("\n".join(lines))

    _LABEL_WIDTH = 17

    @classmethod
    def _render_items(cls, items: list[tuple[str, str, str]], *, highlight: str | None) -> list[str]:
        out: list[str] = []
        for view_id, label, shortcut in items:
            is_active = highlight is not None and view_id == highlight
            label_colour = FG_0 if is_active else FG_1
            # Left marker: a block glyph in accent cyan for the active row,
            # blank otherwise. One cell wide, matches the design's 2px cyan
            # left border in TUI proportions.
            marker = f"[{ACCENT_CYAN}]▌[/]" if is_active else " "
            label_text = label[: cls._LABEL_WIDTH]
            shortcut_cell = f"[{FG_2} on {BG_0}] {shortcut} [/]" if shortcut else "   "
            padded = f"{label_text:<{cls._LABEL_WIDTH}}"
            if is_active:
                row = f"{marker} [{FG_0} on {OVERLAY_SELECTED}]{padded}[/]{shortcut_cell}"
            else:
                row = f"{marker} [{label_colour}]{padded}[/]{shortcut_cell}"
            out.append(row)
        return out


# ---------------------------------------------------------------------------
# Action bar: bottom-of-screen kbd hint strip + mode indicator.
# Replaces Textual's default Footer to match the concept's `.actionbar`.
# ---------------------------------------------------------------------------


_ACTION_HINTS: list[tuple[str, str]] = [
    ("/", "search"),
    ("f", "filter"),
    (":", "jump"),
    ("j/k", "move"),
    ("enter", "open"),
    ("e", "events"),
    ("m", "map"),
    ("?", "help"),
    ("q", "quit"),
]


class ActionBar(Label):
    """Bottom single-row kbd hint strip with a trailing mode indicator."""

    DEFAULT_CSS = f"""
    ActionBar {{
        height: 1;
        width: 1fr;
        background: {BG_1};
        color: {FG_1};
        padding: 0 1;
    }}
    """

    def __init__(self) -> None:
        self._mode = "aircraft list"
        super().__init__(self._build(), id="action-bar")

    def on_resize(self) -> None:
        self.update(self._build())

    def set_mode(self, mode: str) -> None:
        self._mode = mode
        self.update(self._build())

    def _build(self) -> Text:
        chunks: list[str] = []
        for key, label in _ACTION_HINTS:
            chunks.append(f"[{FG_2} on {BG_0}] {key} [/][{FG_1}] {label}[/]")
        left = Text.from_markup("  ".join(chunks))
        right = Text.from_markup(f"[{FG_2}]{self._mode}[/] [{FG_2}]{DOT}[/] [{ACCENT_CYAN}]normal[/]")
        total_width = max(1, _widget_width(self) - 2)
        gap = max(2, total_width - left.cell_len - right.cell_len)
        out = Text()
        out.append(left)
        out.append(" " * gap)
        out.append(right)
        return out


# ---------------------------------------------------------------------------
# Card: bordered panel for dashboard grids (status view / ops view).
# ---------------------------------------------------------------------------


class Card(Static):
    """Bordered panel matching the concept's ``.card`` block.

    Use ``.wide`` to span 2 grid columns and ``.tall`` for 2 rows. Pass
    the header/value/sub lines as Rich markup via ``content``; the caller
    controls exact layout.
    """

    DEFAULT_CSS = f"""
    Card {{
        background: {BG_1};
        border: round {BD_0};
        padding: 0 1;
        height: auto;
        color: {FG_0};
    }}
    Card.wide {{
        column-span: 2;
    }}
    Card.tall {{
        row-span: 2;
    }}
    """

    def __init__(self, content: str | Text = "", *, classes: str | None = None, id: str | None = None) -> None:
        super().__init__(content, classes=classes, id=id)


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


__all__ = [
    "ACCENT_AMBER",
    "ACCENT_CYAN",
    "ACCENT_MAGENTA",
    "ACCENT_OK",
    "ACCENT_RED",
    "ACCENT_VIOLET",
    "ActionBar",
    "BD_0",
    "BD_1",
    "BG_0",
    "BG_1",
    "BG_2",
    "BG_3",
    "Card",
    "DOT",
    "FG_0",
    "FG_1",
    "FG_2",
    "FG_3",
    "FILTER_BAR_CSS",
    "FilterBar",
    "HorizontalGroup",
    "OVERLAY_HOVER",
    "OVERLAY_SELECTED",
    "OVERLAY_STRIPE",
    "PageHeader",
    "Sidebar",
    "StatusStrip",
    "cell",
    "dash",
    "fmt_bytes",
    "num_cell",
    "pill_markup",
    "pill_solid",
]


# ---------------------------------------------------------------------------
# Compose helper for views that want a standard header + filter bar pair.
# ---------------------------------------------------------------------------


def compose_header_filterbar(header: PageHeader, filter_bar: FilterBar) -> ComposeResult:
    """Yield the standard page-header + filter-bar pair for a view."""
    yield header
    yield filter_bar.build()

"""Braille-based text-mode map for the TUI.

Renders the trace as a connected path on a braille dot grid, with a
graph-paper grid backdrop, overlay panels (LAYERS / TRACE info) and
endpoint labels so users can see both the shape of the flight and
WHERE in the world it happened. Real cartography (street map,
satellite tiles) lives in the GUI export via Leaflet; this surface is
the TUI's at-a-glance view.

The compositor is a simple two-pass painter:

1. A ``BrailleCanvas`` rasterises the trace (2x4 dots per terminal cell)
   so a 156x37 pane becomes a 312x148 dot grid. Consecutive points are
   connected with Bresenham segments, coloured by the starting point's
   readsb source. Segments straddling a gap in time are drawn as dashed
   amber.
2. A ``_Grid`` records per-cell character + colour (grid dots, braille
   glyphs, panel borders, labels). Panels and labels paint AFTER the
   trace so they cover the trace where they overlap; grid backdrop
   paints only where the trace hasn't already touched. The grid is
   flattened to a single ``rich.text.Text`` by coalescing runs of
   same-coloured characters, rather than emitting one markup tag per
   character.
"""

from __future__ import annotations

import sqlite3
from collections import Counter
from dataclasses import dataclass, field

from rich.text import Text
from textual import work
from textual.app import ComposeResult
from textual.containers import Vertical
from textual.widget import Widget
from textual.worker import Worker, WorkerState

from ...airports import find_nearest_airport
from ...db import Database
from ..braille import BrailleCanvas
from ..queries import TracePoint, distinct_dates_for_icao, list_flights, load_trace_points
from ..widgets import (
    ACCENT_AMBER,
    ACCENT_CYAN,
    ACCENT_OK,
    ACCENT_VIOLET,
    BD_0,
    DOT,
    FG_0,
    FG_1,
    FG_2,
    PageHeader,
)

# Reused (not reimplemented) so a near-match-only endpoint renders as the
# same ~ICAO marker here as it does in the flights table (issue #18).
from .flights import _display_destination, _display_origin

_SOURCE_COLOUR = {
    "adsb_icao": ACCENT_OK,
    "adsb_other": ACCENT_OK,
    "mlat": FG_2,
    "tisb_icao": ACCENT_AMBER,
    "tisb_other": ACCENT_AMBER,
    "adsr_icao": ACCENT_CYAN,
    "adsc": ACCENT_VIOLET,
    "other": FG_2,
    "mode_s": FG_2,
}

_SOURCE_LEGEND: list[tuple[str, str, list[str]]] = [
    ("ADS-B", ACCENT_OK, ["adsb_icao", "adsb_other"]),
    ("MLAT", FG_2, ["mlat"]),
    ("TIS-B", ACCENT_AMBER, ["tisb_icao", "tisb_other"]),
    ("ADS-R", ACCENT_CYAN, ["adsr_icao"]),
    ("ADS-C", ACCENT_VIOLET, ["adsc"]),
]

# Footer legend annotations for entries that double up as another map
# element's colour (gap dashes reuse TIS-B's amber, the end marker
# reuses ADS-R's cyan). Keyed by the _SOURCE_LEGEND label.
_FOOTER_SUFFIX = {"TIS-B": "gap", "ADS-R": "end"}

# Grid backdrop: one dot every N cols / M rows.
_GRID_COL_STEP = 6
_GRID_ROW_STEP = 3

# Overlay panel geometry. The pane needs room for a 1-col margin, both
# panels side by side, and a matching margin on the right before the
# LAYERS/TRACE panels are worth drawing (see the gate in ``_compose``).
_PANEL_MARGIN = 1
_LAYERS_PANEL_WIDTH = 22
_INFO_PANEL_WIDTH = 32
_MIN_PANEL_COLS = _PANEL_MARGIN + _LAYERS_PANEL_WIDTH + _INFO_PANEL_WIDTH + _PANEL_MARGIN


@dataclass(frozen=True)
class _MapCtx:
    """Everything the renderer needs in one immutable bundle.

    ``lat_min``/``lat_max``/``lon_min``/``lon_max``, the altitude range,
    and ``source_counts`` are reductions over ``points`` computed once
    (in ``_build_ctx``, on data load) rather than recomputed on every
    ``MapCanvas.render()`` call -- a terminal resize alone would
    otherwise re-walk the whole point list several times per frame.
    """

    points: list[TracePoint]
    date: str | None
    start_label: str
    end_label: str
    gap_secs: float
    lat_min: float
    lat_max: float
    lon_min: float
    lon_max: float
    alt_min: int | None
    alt_max: int | None
    source_counts: Counter[str]


@dataclass(frozen=True)
class _MapLoadResult:
    """Everything the worker fetched, applied to the UI on the event loop."""

    icao: str
    date: str | None
    ctx: _MapCtx | None
    crumb: str
    trailing: str


def _build_ctx(
    points: list[TracePoint],
    date: str | None,
    start_label: str,
    end_label: str,
    gap_secs: float,
) -> _MapCtx:
    """Build a ``_MapCtx`` from a nonempty point list, computing the bbox /
    altitude range / source counts exactly once (F7)."""
    lats = [p.lat for p in points]
    lons = [p.lon for p in points]
    alts = [p.alt_ft for p in points if p.alt_ft is not None]
    return _MapCtx(
        points=points,
        date=date,
        start_label=start_label,
        end_label=end_label,
        gap_secs=gap_secs,
        lat_min=min(lats),
        lat_max=max(lats),
        lon_min=min(lons),
        lon_max=max(lons),
        alt_min=min(alts) if alts else None,
        alt_max=max(alts) if alts else None,
        source_counts=Counter(p.source for p in points),
    )


# ---------------------------------------------------------------------------
# Projection + trace rasterisation
# ---------------------------------------------------------------------------


def _usable_dot_span(dot_w: int, dot_h: int) -> tuple[int, int, int, int]:
    """Return ``(inset_x, inset_y, usable_w, usable_h)`` dot spans.

    The projection insets by ~10% so the trace doesn't hug the pane
    edges where endpoint labels want to live. Shared by the projector
    and the legend's degrees-per-cell scale so both agree on what "the
    map" actually spans.
    """
    inset_x = max(1, dot_w // 10)
    inset_y = max(1, dot_h // 10)
    usable_w = max(1, dot_w - 2 * inset_x - 1)
    usable_h = max(1, dot_h - 2 * inset_y - 1)
    return inset_x, inset_y, usable_w, usable_h


def _scale_deg_per_cell(
    lat_min: float,
    lat_max: float,
    lon_min: float,
    lon_max: float,
    dot_w: int,
    dot_h: int,
) -> float:
    """Approximate degrees-per-cell for the legend caption (F1).

    Uses the same usable-span math as ``_project_to_dots`` so the
    caption reflects the actual current projection instead of a fixed
    guess -- scale varies enormously between a transcontinental trace
    and a helicopter hop. A terminal cell is ~2 dots wide x 4 dots
    tall, so lon-per-cell and lat-per-cell can differ; this returns
    the larger of the two, the dimension that would clip first.
    """
    _, _, usable_w, usable_h = _usable_dot_span(dot_w, dot_h)
    lon_per_cell = (lon_max - lon_min) / usable_w * 2
    lat_per_cell = (lat_max - lat_min) / usable_h * 4
    return max(lon_per_cell, lat_per_cell)


def _project_to_dots(
    points: list[TracePoint],
    lat_min: float,
    lat_max: float,
    lon_min: float,
    lon_max: float,
    dot_w: int,
    dot_h: int,
) -> list[tuple[int, int, str]]:
    """Project trace points into dot coordinates on the braille canvas.

    Returns ``(dot_x, dot_y, source)`` triples in input order. Lat/lon
    is projected into ``[0, dot_w-1] x [0, dot_h-1]`` with lat inverted
    (north = top of screen) using the caller-supplied bbox (computed
    once in ``_build_ctx``, not re-derived here).
    """
    if not points or dot_w <= 1 or dot_h <= 1:
        return []
    if lat_max == lat_min:
        lat_max = lat_min + 1e-6
    if lon_max == lon_min:
        lon_max = lon_min + 1e-6
    inset_x, inset_y, usable_w, usable_h = _usable_dot_span(dot_w, dot_h)
    out: list[tuple[int, int, str]] = []
    for p in points:
        x = inset_x + int((p.lon - lon_min) / (lon_max - lon_min) * usable_w)
        y = inset_y + usable_h - int((p.lat - lat_min) / (lat_max - lat_min) * usable_h)
        out.append((x, y, p.source))
    return out


def _rasterise_trace(
    canvas: BrailleCanvas,
    points: list[TracePoint],
    projected: list[tuple[int, int, str]],
    gap_secs: float,
) -> None:
    """Draw the trace with solid source-coloured segments; jumps in
    timestamp longer than ``gap_secs`` become dashed amber so a
    signal-loss gap is visible on the map.

    No-ops if ``projected`` is empty (F8): the projector returns ``[]``
    for a degenerate dot grid even when ``points`` is nonempty, and
    indexing into it here would otherwise raise.
    """
    if not projected:
        return
    for i in range(len(points) - 1):
        x0, y0, src = projected[i]
        x1, y1, _ = projected[i + 1]
        dt = points[i + 1].ts - points[i].ts
        if dt > gap_secs:
            canvas.line(x0, y0, x1, y1, ACCENT_AMBER, dash=(2, 2))
        else:
            canvas.line(x0, y0, x1, y1, _SOURCE_COLOUR.get(src, FG_0))


# ---------------------------------------------------------------------------
# Cell-grid compositor
# ---------------------------------------------------------------------------


@dataclass
class _Grid:
    """Character + per-cell colour grid the compositor paints into.

    Kept as two parallel arrays rather than per-cell markup strings so
    the final render can coalesce runs of same-coloured characters into
    a handful of ``rich.text.Text`` spans instead of emitting one markup
    tag per character (~10k tags for a full pane).
    """

    rows: int
    cols: int
    chars: list[list[str]] = field(init=False)
    colour: list[list[str | None]] = field(init=False)

    def __post_init__(self) -> None:
        self.chars = [[" "] * self.cols for _ in range(self.rows)]
        self.colour = [[None] * self.cols for _ in range(self.rows)]

    def set(self, row: int, col: int, ch: str, colour: str | None) -> None:
        if 0 <= row < self.rows and 0 <= col < self.cols:
            self.chars[row][col] = ch
            self.colour[row][col] = colour

    def to_text(self) -> Text:
        text = Text()
        for r in range(self.rows):
            if r:
                text.append("\n")
            char_row = self.chars[r]
            colour_row = self.colour[r]
            col = 0
            while col < self.cols:
                run_colour = colour_row[col]
                start = col
                while col < self.cols and colour_row[col] == run_colour:
                    col += 1
                run = "".join(char_row[start:col])
                text.append(run, style=run_colour or "")
        return text


def _place_text(grid: _Grid, row: int, col: int, text: str, colour: str) -> None:
    """Write ``text`` into the grid starting at ``(row, col)``.

    Each char becomes one cell, silently truncated at the pane edge.
    Spaces are written with no colour so background dots don't bleed
    through gaps in labels.
    """
    if row < 0 or row >= grid.rows:
        return
    for i, ch in enumerate(text):
        c = col + i
        if c < 0 or c >= grid.cols:
            continue
        grid.set(row, c, ch, None if ch == " " else colour)


def _draw_panel(
    grid: _Grid,
    *,
    top: int,
    left: int,
    width: int,
    rows: list[list[tuple[str, str]]],
) -> None:
    """Draw a bordered panel with content rows.

    ``rows`` is a list of ``(colour, text)`` segment sequences. Each
    segment sequence is concatenated horizontally into the panel's
    inner width (width - 4: 2 for the left/right borders, 2 for
    inner padding). Silently no-ops if the panel would overflow the
    grid.
    """
    height = len(rows) + 2
    if top < 0 or left < 0 or top + height > grid.rows or left + width > grid.cols:
        return
    inner_left = left + 2
    inner_width = width - 4

    # Top border
    grid.set(top, left, "┌", BD_0)
    for c in range(left + 1, left + width - 1):
        grid.set(top, c, "─", BD_0)
    grid.set(top, left + width - 1, "┐", BD_0)

    # Content rows
    for i, segments in enumerate(rows):
        r = top + 1 + i
        grid.set(r, left, "│", BD_0)
        # Clear inner cells so grid dots / trace don't bleed through.
        for c in range(left + 1, left + width - 1):
            grid.set(r, c, " ", None)
        grid.set(r, left + width - 1, "│", BD_0)
        col_cursor = inner_left
        max_col = inner_left + inner_width
        for colour, text in segments:
            if col_cursor >= max_col:
                break
            allowed = max_col - col_cursor
            _place_text(grid, r, col_cursor, text[:allowed], colour)
            col_cursor += min(len(text), allowed)

    # Bottom border
    br = top + height - 1
    grid.set(br, left, "└", BD_0)
    for c in range(left + 1, left + width - 1):
        grid.set(br, c, "─", BD_0)
    grid.set(br, left + width - 1, "┘", BD_0)


def _draw_grid_backdrop(grid: _Grid, occupied: set[tuple[int, int]]) -> None:
    """Sprinkle dim graph-paper dots across cells the trace didn't touch.

    ``occupied`` is the set of ``(row, col)`` that have a braille glyph
    already; we skip those so the dots don't clash with the path.
    """
    for r in range(1, grid.rows - 1, _GRID_ROW_STEP):
        for c in range(2, grid.cols - 1, _GRID_COL_STEP):
            if (r, c) in occupied:
                continue
            grid.set(r, c, "·", BD_0)


def _layers_panel(counts: Counter[str], total: int) -> list[list[tuple[str, str]]]:
    """Build the LAYERS panel content: source counts with coloured markers.

    Sources not covered by any ``_SOURCE_LEGEND`` entry (unknown, mode_s,
    the generic "other" tag) still count toward ``total``; without an
    aggregated row for them, a trace dominated by an unlisted source
    would show near-0% on every visible row (F3). The "other" row is
    always present, like every other legend row, so the panel's row
    count -- and therefore its height -- doesn't change with the data.
    """
    rows: list[list[tuple[str, str]]] = [[(ACCENT_CYAN, "LAYERS")]]
    accounted = 0
    for label, colour, src_keys in _SOURCE_LEGEND:
        n = sum(counts.get(key, 0) for key in src_keys)
        accounted += n
        pct = (100.0 * n / total) if total else 0.0
        marker_colour = colour if n else BD_0
        label_colour = FG_0 if n else FG_2
        rows.append(
            [
                (marker_colour, "● "),
                (label_colour, f"{label:<6}"),
                (FG_2, f"{pct:>5.1f}%"),
            ]
        )
    other_n = total - accounted
    pct = (100.0 * other_n / total) if total else 0.0
    marker_colour = FG_2 if other_n else BD_0
    label_colour = FG_0 if other_n else FG_2
    rows.append(
        [
            (marker_colour, "● "),
            (label_colour, f"{'other':<6}"),
            (FG_2, f"{pct:>5.1f}%"),
        ]
    )
    return rows


def _alt_range_str(alt_min: int | None, alt_max: int | None) -> str:
    """Format the TRACE panel's altitude range.

    Ground-only traces have no baro altitude reading at all (``alt_ft``
    is None on every ground point), so ``alt_min``/``alt_max`` are both
    None -- that means the aircraft was on the ground the whole trace,
    not "0 .. 0 ft" (F2).
    """
    if alt_min is None or alt_max is None:
        return "ground"
    return f"{alt_min:,} .. {alt_max:,} ft"


def _info_panel(ctx: _MapCtx) -> list[list[tuple[str, str]]]:
    """Build the TRACE info panel: bbox / count / alt range. This is the
    "WHERE in the world are we" panel."""

    def _lat(x: float) -> str:
        return f"{abs(x):.2f}°{'S' if x < 0 else 'N'}"

    def _lon(x: float) -> str:
        return f"{abs(x):.2f}°{'W' if x < 0 else 'E'}"

    return [
        [(ACCENT_CYAN, "TRACE")],
        [(FG_2, "date  "), (FG_0, ctx.date or "-")],
        [(FG_2, "pts   "), (FG_0, f"{len(ctx.points):,}")],
        [(FG_2, "lat   "), (FG_0, f"{_lat(ctx.lat_min)} .. {_lat(ctx.lat_max)}")],
        [(FG_2, "lon   "), (FG_0, f"{_lon(ctx.lon_min)} .. {_lon(ctx.lon_max)}")],
        [(FG_2, "alt   "), (FG_0, _alt_range_str(ctx.alt_min, ctx.alt_max))],
    ]


def _draw_endpoint(
    grid: _Grid,
    projected_pt: tuple[int, int, str],
    label: str,
    colour: str,
) -> None:
    """Paint a coloured endpoint marker with a ``label`` next to it.

    The label sits to the right of the marker if there's room; if the
    right-hand placement would spill past the pane edge, flip to the
    left. Placement always leaves a 1-cell gap before the marker and
    truncates the label to fit that gap rather than clamping its start
    position back across the marker cell (F4) -- a label that doesn't
    fit on the left gets shortened, never repositioned onto the marker.
    """
    x, y, _ = projected_pt
    row = y // 4
    col = x // 2
    if row < 0 or row >= grid.rows or col < 0 or col >= grid.cols:
        return
    grid.set(row, col, "●", colour)
    right_start = col + 2
    available_right = grid.cols - right_start
    if available_right >= len(label):
        _place_text(grid, row, right_start, label, colour)
        return
    available_left = col - 1  # columns [0, col-2], leaving a 1-cell gap
    if available_left <= 0:
        return
    text = label[:available_left]
    left_start = col - 1 - len(text)
    _place_text(grid, row, left_start, text, colour)


def _footer_legend(deg_per_cell: float) -> str:
    """Build the bottom legend from ``_SOURCE_LEGEND`` -- the single
    source of truth for the source -> colour mapping (F7) -- plus the
    live degrees-per-cell scale (F1)."""
    parts: list[str] = []
    for label, colour, _ in _SOURCE_LEGEND:
        short = label.lower().replace("-", "")
        suffix = _FOOTER_SUFFIX.get(label)
        text = f"{short} / {suffix}" if suffix else short
        parts.append(f"[{colour}]● {text}[/]")
    parts.append(f"[{FG_1}]grid ~{deg_per_cell:.3f}°/cell[/]")
    return "   ".join(parts)


def _compose(ctx: _MapCtx, cols: int, rows: int) -> Text:
    """Compose grid + trace + panels + labels into a coalesced Text."""
    grid = _Grid(rows=rows, cols=cols)

    # Pass 1: rasterise the trace onto the braille canvas.
    canvas = BrailleCanvas(cols=cols, rows=rows)
    projected = _project_to_dots(
        ctx.points, ctx.lat_min, ctx.lat_max, ctx.lon_min, ctx.lon_max, canvas.dot_width, canvas.dot_height
    )
    _rasterise_trace(canvas, ctx.points, projected, ctx.gap_secs)

    # Copy braille glyphs into the grid via the public cell iterator
    # (not canvas._bits/_colours) and remember which cells they touched
    # so the grid backdrop skips them.
    occupied: set[tuple[int, int]] = set()
    for r, c, ch, colour in canvas.cells():
        grid.set(r, c, ch, colour)
        occupied.add((r, c))

    # Pass 2: grid backdrop (only where trace didn't draw).
    _draw_grid_backdrop(grid, occupied)

    # Pass 3: overlay panels. Only draw them if the pane is wide/tall
    # enough; otherwise the map gets buried under chrome.
    if cols >= _MIN_PANEL_COLS and rows >= 9:
        _draw_panel(
            grid,
            top=0,
            left=_PANEL_MARGIN,
            width=_LAYERS_PANEL_WIDTH,
            rows=_layers_panel(ctx.source_counts, len(ctx.points)),
        )
        info_rows = _info_panel(ctx)
        _draw_panel(
            grid,
            top=0,
            left=max(_PANEL_MARGIN, cols - _INFO_PANEL_WIDTH - _PANEL_MARGIN),
            width=_INFO_PANEL_WIDTH,
            rows=info_rows,
        )

    # Pass 4: endpoint markers + labels, placed after panels so labels
    # stay visible even where they overlap a panel region.
    if projected:
        _draw_endpoint(grid, projected[0], f"start {ctx.start_label}", ACCENT_OK)
        _draw_endpoint(grid, projected[-1], f"end {ctx.end_label}", ACCENT_CYAN)

    return grid.to_text()


def _route_for(db: Database, icao: str, date: str) -> str | None:
    """Return a "KSPG > KHKY"-style route label for the displayed date's
    first flight, or ``None`` if there's no flight that day or neither
    endpoint resolves to anything displayable.

    Anchored to the flights table (the extractor's actual matched
    origin/destination for a specific flight) rather than a live
    nearest-airport lookup against the raw trace endpoints -- the two can
    disagree on a multi-leg day, since the trace's absolute first/last
    point isn't necessarily this flight's takeoff/landing. Reuses
    ``views.flights._display_origin`` / ``_display_destination`` (issue
    #18's ~ICAO near-match fallback) instead of reimplementing that
    fallback logic, so a flight whose real origin_icao missed the
    on-field match threshold still shows a route here, consistent with
    how the flights table renders that same flight.
    """
    day_flights = [f for f in list_flights(db, icao) if f.takeoff_date == date]
    if not day_flights:
        return None
    first = min(day_flights, key=lambda f: f.takeoff_time)
    origin = _display_origin(first)
    destination = _display_destination(first)
    if not origin or not destination:
        return None
    return f"{origin} > {destination}"


# ---------------------------------------------------------------------------
# Widgets
# ---------------------------------------------------------------------------


class MapCanvas(Widget):
    """Adaptive text-mode trace canvas backed by a braille raster."""

    DEFAULT_CSS = """
    MapCanvas {
        height: 1fr;
        width: 1fr;
        background: #0b0f14;
    }
    """

    def __init__(self) -> None:
        super().__init__(id="map-canvas")
        self._ctx: _MapCtx | None = None

    def set_context(self, ctx: _MapCtx | None) -> None:
        self._ctx = ctx
        self.refresh()

    def on_resize(self) -> None:
        self.refresh()

    def render(self) -> Text:
        w, h = self.size.width, self.size.height
        # Reserve one row for the legend.
        grid_h = max(1, h - 1)
        ctx = self._ctx
        if ctx is None or not ctx.points or w <= 2 or grid_h <= 2:
            return Text.from_markup(
                f"[{FG_2}]no trace points available. select an aircraft (1) with trace data, then hit 5.[/]"
            )
        body = _compose(ctx, cols=w, rows=grid_h)
        dot_w, dot_h = w * 2, grid_h * 4
        scale = _scale_deg_per_cell(ctx.lat_min, ctx.lat_max, ctx.lon_min, ctx.lon_max, dot_w, dot_h)
        body.append("\n")
        body.append_text(Text.from_markup(_footer_legend(scale)))
        return body


class MapView(Vertical):
    """Trace playback for one aircraft, one date."""

    def __init__(self) -> None:
        super().__init__(id="view-map")
        self._icao: str | None = None
        self._date: str | None = None
        self._header = PageHeader("map", crumb="select an aircraft first", widget_id="map-header")
        self._canvas = MapCanvas()

    def compose(self) -> ComposeResult:
        yield self._header
        yield self._canvas

    def set_icao(self, icao: str | None) -> None:
        self._icao = icao
        self._date = None
        self.refresh_data()

    def refresh_data(self) -> None:
        """Kick off a background worker to load the trace + build the map context.

        The query itself runs off the event loop in ``_fetch_map`` (Task 15);
        the result lands back here via ``on_worker_state_changed``.
        """
        if self._icao is None:
            self._canvas.set_context(None)
            self._header.set_crumb("select an aircraft first")
            self._header.set_trailing("")
            return
        self.loading = True
        self._fetch_map(self._icao, self._date)

    @work(thread=True, exclusive=True, group="map", exit_on_error=False)
    def _fetch_map(self, icao: str, date: str | None) -> _MapLoadResult:
        """Run the date-resolution + trace-point queries on a worker's own connection.

        Must not touch ``self.app.db`` (the main-thread connection) or any
        widget -- only DB reads and pure computation happen here.
        ``exit_on_error=False`` on the decorator keeps a raised exception
        from crashing the whole app before the ``ERROR`` branch in
        ``on_worker_state_changed`` runs. ``self.app.config`` is read
        (not ``self.app.db``) -- it's immutable in practice once the app
        is built, so sharing it into a worker thread is safe.
        """
        db = self.app.db_factory()
        try:
            if date is None:
                dates = distinct_dates_for_icao(db, icao)
                if not dates:
                    return _MapLoadResult(icao=icao, date=None, ctx=None, crumb="no trace data", trailing="")
                date = dates[0]
            points = load_trace_points(db, icao, date)
            if not points:
                return _MapLoadResult(icao=icao, date=date, ctx=None, crumb=f"{date} (no trace points)", trailing="")
            start_label = self._airport_or_coords(db, points[0])
            end_label = self._airport_or_coords(db, points[-1])
            ctx = _build_ctx(points, date, start_label, end_label, self.app.config.map_trace_gap_secs)
            # Prefer the flight's actual matched route (more precise than
            # a live nearest-airport guess against the raw trace
            # endpoints, and agrees with the flights table); fall back to
            # the trace-endpoint labels when no flight row covers this date.
            route = _route_for(db, icao, date)
            crumb = f"{date} {DOT} {route}" if route is not None else f"{date} {DOT} {start_label} > {end_label}"
            trailing = (
                f"{len(points):,} points {DOT} "
                f"bbox ({ctx.lat_min:.3f},{ctx.lon_min:.3f})-({ctx.lat_max:.3f},{ctx.lon_max:.3f})"
            )
            return _MapLoadResult(icao=icao, date=date, ctx=ctx, crumb=crumb, trailing=trailing)
        finally:
            db.close()

    def on_worker_state_changed(self, event: Worker.StateChanged) -> None:
        if event.worker.name != "_fetch_map":
            return
        if event.state == WorkerState.SUCCESS:
            result = event.worker.result
            assert result is not None  # a SUCCESS worker always has a result
            self._date = result.date
            self._canvas.set_context(result.ctx)
            self._header.set_title(result.icao)
            self._header.set_crumb(result.crumb)
            self._header.set_trailing(result.trailing)
            self.loading = False
        elif event.state == WorkerState.ERROR:
            self.loading = False
            self.app.notify(f"failed to load map: {event.worker.error}", severity="error")

    def _airport_or_coords(self, db: Database, point: TracePoint) -> str:
        """Return the nearest airport ident, or a lat/lon fallback.

        Narrowed to ``sqlite3.Error`` (F5): a bug in ``find_nearest_airport``
        should surface as a crash, not silently fall back to raw coords.
        """
        try:
            match = find_nearest_airport(db, point.lat, point.lon, self.app.config)
        except sqlite3.Error:
            match = None
        if match and match.ident:
            return match.ident
        return f"{point.lat:.2f},{point.lon:.2f}"

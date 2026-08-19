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
2. A cell grid records per-cell Rich markup (grid dots, braille glyphs,
   panel borders, labels). Panels and labels paint AFTER the trace so
   they cover the trace where they overlap; grid backdrop paints only
   where the trace hasn't already touched.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass

from rich.text import Text
from textual import work
from textual.app import ComposeResult
from textual.containers import Vertical
from textual.widget import Widget
from textual.worker import Worker, WorkerState

from ...airports import find_nearest_airport
from ...config import Config
from ...db import Database
from ..braille import BrailleCanvas
from ..queries import TracePoint, distinct_dates_for_icao, load_trace_points
from ..widgets import (
    ACCENT_AMBER,
    ACCENT_CYAN,
    ACCENT_OK,
    ACCENT_VIOLET,
    BD_0,
    FG_0,
    FG_1,
    FG_2,
    PageHeader,
)

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

# Gap threshold: jumps in timestamp longer than this are drawn dashed so
# the user can see where coverage dropped out.
_GAP_SECS = 60.0

# Grid backdrop: one dot every N cols / M rows.
_GRID_COL_STEP = 6
_GRID_ROW_STEP = 3


@dataclass(frozen=True)
class _MapCtx:
    """Everything the renderer needs in one immutable bundle."""

    points: list[TracePoint]
    date: str | None
    start_label: str
    end_label: str


@dataclass(frozen=True)
class _MapLoadResult:
    """Everything the worker fetched, applied to the UI on the event loop."""

    icao: str
    date: str | None
    ctx: _MapCtx | None
    crumb: str
    trailing: str


# ---------------------------------------------------------------------------
# Projection + trace rasterisation
# ---------------------------------------------------------------------------


def _project_to_dots(points: list[TracePoint], dot_w: int, dot_h: int) -> list[tuple[int, int, str]]:
    """Project trace points into dot coordinates on the braille canvas.

    Returns ``(dot_x, dot_y, source)`` triples in input order. Lat/lon
    is projected into ``[0, dot_w-1] x [0, dot_h-1]`` with lat inverted
    (north = top of screen) and a small inset so endpoint labels don't
    hit the edge. A tiny epsilon protects against a degenerate bbox
    from a single-point trace.
    """
    if not points or dot_w <= 1 or dot_h <= 1:
        return []
    lats = [p.lat for p in points]
    lons = [p.lon for p in points]
    lat_min, lat_max = min(lats), max(lats)
    lon_min, lon_max = min(lons), max(lons)
    if lat_max == lat_min:
        lat_max = lat_min + 1e-6
    if lon_max == lon_min:
        lon_max = lon_min + 1e-6
    # Inset the projection by ~10% so the trace doesn't hug the edges
    # of the pane where the labels want to live.
    inset_x = max(1, dot_w // 10)
    inset_y = max(1, dot_h // 10)
    usable_w = max(1, dot_w - 2 * inset_x - 1)
    usable_h = max(1, dot_h - 2 * inset_y - 1)
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
) -> None:
    """Draw the trace with solid source-coloured segments; jumps in
    timestamp longer than ``_GAP_SECS`` become dashed amber so a
    signal-loss gap is visible on the map."""
    for i in range(len(points) - 1):
        x0, y0, src = projected[i]
        x1, y1, _ = projected[i + 1]
        dt = points[i + 1].ts - points[i].ts
        if dt > _GAP_SECS:
            _dashed_line(canvas, x0, y0, x1, y1, ACCENT_AMBER)
        else:
            canvas.line(x0, y0, x1, y1, _SOURCE_COLOUR.get(src, FG_0))


def _dashed_line(canvas: BrailleCanvas, x0: int, y0: int, x1: int, y1: int, colour: str) -> None:
    """Bresenham variant that plots every third dot (dash 2, gap 2)."""
    dx = abs(x1 - x0)
    dy = -abs(y1 - y0)
    sx = 1 if x0 < x1 else -1
    sy = 1 if y0 < y1 else -1
    err = dx + dy
    step = 0
    while True:
        if step % 4 < 2:
            canvas.set(x0, y0, colour)
        step += 1
        if x0 == x1 and y0 == y1:
            return
        e2 = 2 * err
        if e2 >= dy:
            err += dy
            x0 += sx
        if e2 <= dx:
            err += dx
            y0 += sy


# ---------------------------------------------------------------------------
# Cell-grid compositor
# ---------------------------------------------------------------------------


def _place_text(
    cells: list[list[str | None]],
    row: int,
    col: int,
    text: str,
    colour: str,
) -> None:
    """Write ``text`` into the cell grid starting at ``(row, col)``.

    Each char becomes one cell, silently truncated at the right edge.
    Spaces are stored as a literal ``" "`` so background dots don't
    bleed through through gaps in labels.
    """
    if row < 0 or row >= len(cells):
        return
    width = len(cells[0])
    for i, ch in enumerate(text):
        c = col + i
        if c < 0 or c >= width:
            continue
        cells[row][c] = " " if ch == " " else f"[{colour}]{ch}[/]"


def _draw_panel(
    cells: list[list[str | None]],
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
    cell grid.
    """
    height = len(rows) + 2
    grid_rows = len(cells)
    grid_cols = len(cells[0]) if cells else 0
    if top < 0 or left < 0 or top + height > grid_rows or left + width > grid_cols:
        return
    inner_left = left + 2
    inner_width = width - 4

    # Top border
    cells[top][left] = f"[{BD_0}]┌[/]"
    for c in range(left + 1, left + width - 1):
        cells[top][c] = f"[{BD_0}]─[/]"
    cells[top][left + width - 1] = f"[{BD_0}]┐[/]"

    # Content rows
    for i, segments in enumerate(rows):
        r = top + 1 + i
        cells[r][left] = f"[{BD_0}]│[/]"
        # Clear inner cells so grid dots / trace don't bleed through.
        for c in range(left + 1, left + width - 1):
            cells[r][c] = " "
        cells[r][left + width - 1] = f"[{BD_0}]│[/]"
        col_cursor = inner_left
        max_col = inner_left + inner_width
        for colour, text in segments:
            if col_cursor >= max_col:
                break
            allowed = max_col - col_cursor
            _place_text(cells, r, col_cursor, text[:allowed], colour)
            col_cursor += min(len(text), allowed)

    # Bottom border
    br = top + height - 1
    cells[br][left] = f"[{BD_0}]└[/]"
    for c in range(left + 1, left + width - 1):
        cells[br][c] = f"[{BD_0}]─[/]"
    cells[br][left + width - 1] = f"[{BD_0}]┘[/]"


def _draw_grid_backdrop(cells: list[list[str | None]], occupied: set[tuple[int, int]]) -> None:
    """Sprinkle dim graph-paper dots across cells the trace didn't touch.

    ``occupied`` is the set of ``(row, col)`` that have a braille glyph
    already; we skip those so the dots don't clash with the path.
    """
    rows = len(cells)
    cols = len(cells[0])
    for r in range(1, rows - 1, _GRID_ROW_STEP):
        for c in range(2, cols - 1, _GRID_COL_STEP):
            if (r, c) in occupied:
                continue
            cells[r][c] = f"[{BD_0}]·[/]"


def _layers_panel(points: list[TracePoint]) -> list[list[tuple[str, str]]]:
    """Build the LAYERS panel content: source counts with coloured markers."""
    counts: Counter[str] = Counter(p.source for p in points)
    total = sum(counts.values())
    rows: list[list[tuple[str, str]]] = [[(ACCENT_CYAN, "LAYERS")]]
    for label, colour, src_keys in _SOURCE_LEGEND:
        n = sum(counts.get(key, 0) for key in src_keys)
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
    return rows


def _info_panel(ctx: _MapCtx) -> list[list[tuple[str, str]]]:
    """Build the TRACE info panel: bbox / count / alt range. This is the
    "WHERE in the world are we" panel."""
    pts = ctx.points
    lats = [p.lat for p in pts]
    lons = [p.lon for p in pts]
    alts = [p.alt_ft for p in pts if p.alt_ft is not None]

    def _lat(x: float) -> str:
        return f"{abs(x):.2f}°{'S' if x < 0 else 'N'}"

    def _lon(x: float) -> str:
        return f"{abs(x):.2f}°{'W' if x < 0 else 'E'}"

    lat_min, lat_max = min(lats), max(lats)
    lon_min, lon_max = min(lons), max(lons)
    alt_min = min(alts) if alts else 0
    alt_max = max(alts) if alts else 0

    return [
        [(ACCENT_CYAN, "TRACE")],
        [(FG_2, "date  "), (FG_0, ctx.date or "-")],
        [(FG_2, "pts   "), (FG_0, f"{len(pts):,}")],
        [(FG_2, "lat   "), (FG_0, f"{_lat(lat_min)} .. {_lat(lat_max)}")],
        [(FG_2, "lon   "), (FG_0, f"{_lon(lon_min)} .. {_lon(lon_max)}")],
        [(FG_2, "alt   "), (FG_0, f"{alt_min:,} .. {alt_max:,} ft")],
    ]


def _draw_endpoint(
    cells: list[list[str | None]],
    projected_pt: tuple[int, int, str],
    label: str,
    colour: str,
) -> None:
    """Paint a coloured endpoint marker with a ``label`` next to it.

    The label sits to the right of the marker if there's room; if the
    right-hand placement would spill past the pane edge, flip to the
    left so the label stays visible.
    """
    x, y, _ = projected_pt
    row = y // 4
    col = x // 2
    if row < 0 or row >= len(cells) or col < 0 or col >= len(cells[0]):
        return
    cells[row][col] = f"[{colour}]●[/]"
    width = len(cells[0])
    right_start = col + 2
    if right_start + len(label) <= width:
        _place_text(cells, row, right_start, label, colour)
    else:
        left_start = max(0, col - 1 - len(label))
        _place_text(cells, row, left_start, label, colour)


def _compose(ctx: _MapCtx, cols: int, rows: int) -> str:
    """Compose grid + trace + panels + labels into a Rich-markup string."""
    cells: list[list[str | None]] = [[None] * cols for _ in range(rows)]

    # Pass 1: rasterise the trace onto the braille canvas.
    canvas = BrailleCanvas(cols=cols, rows=rows)
    projected = _project_to_dots(ctx.points, canvas.dot_width, canvas.dot_height)
    _rasterise_trace(canvas, ctx.points, projected)

    # Extract occupied cells for grid-dot suppression AND copy braille
    # glyphs into the cell grid.
    occupied: set[tuple[int, int]] = set()
    for r in range(rows):
        for c in range(cols):
            mask = canvas._bits[r][c]
            if mask:
                ch = chr(0x2800 + mask)
                colour = canvas._colours.get((r, c), FG_0)
                cells[r][c] = f"[{colour}]{ch}[/]"
                occupied.add((r, c))

    # Pass 2: grid backdrop (only where trace didn't draw).
    _draw_grid_backdrop(cells, occupied)

    # Pass 3: overlay panels. Only draw them if the pane is wide/tall
    # enough; otherwise the map gets buried under chrome.
    if cols >= 56 and rows >= 9:
        _draw_panel(cells, top=0, left=1, width=22, rows=_layers_panel(ctx.points))
        info_rows = _info_panel(ctx)
        info_width = 32
        _draw_panel(cells, top=0, left=max(1, cols - info_width - 1), width=info_width, rows=info_rows)

    # Pass 4: endpoint markers + labels. Place after panels so labels
    # don't disappear under a panel; but if the endpoint lands in a
    # panel zone, the marker still overwrites that cell, so push the
    # label to the opposite side.
    if projected:
        _draw_endpoint(cells, projected[0], f"start {ctx.start_label}", ACCENT_OK)
        _draw_endpoint(cells, projected[-1], f"end {ctx.end_label}", ACCENT_CYAN)

    # Render
    lines: list[str] = []
    for row in cells:
        lines.append("".join(cell if cell is not None else " " for cell in row))
    return "\n".join(lines)


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
        legend = (
            f"[{ACCENT_OK}]● adsb[/]   [{FG_2}]● mlat[/]   [{ACCENT_AMBER}]● tisb / gap[/]   "
            f"[{ACCENT_CYAN}]● adsr / end[/]   [{ACCENT_VIOLET}]● adsc[/]   "
            f"[{FG_1}]grid 0.01°/cell approx[/]"
        )
        return Text.from_markup(f"{body}\n{legend}")


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

    @work(thread=True, exclusive=True, group="map")
    def _fetch_map(self, icao: str, date: str | None) -> _MapLoadResult:
        """Run the date-resolution + trace-point queries on a worker's own connection.

        Must not touch ``self.app.db`` (the main-thread connection) or any
        widget -- only DB reads and pure computation happen here.
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
                return _MapLoadResult(
                    icao=icao, date=date, ctx=None, crumb=f"map / {date} (no trace points)", trailing=""
                )
            start_label = self._airport_or_coords(db, points[0])
            end_label = self._airport_or_coords(db, points[-1])
            ctx = _MapCtx(points=points, date=date, start_label=start_label, end_label=end_label)
            lats = [p.lat for p in points]
            lons = [p.lon for p in points]
            crumb = f"map / {date}   {start_label} > {end_label}"
            trailing = (
                f"{len(points):,} points   bbox ({min(lats):.3f},{min(lons):.3f})-({max(lats):.3f},{max(lons):.3f})"
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
        """Return the nearest airport ident, or a lat/lon fallback."""
        try:
            match = find_nearest_airport(db, point.lat, point.lon, Config())
        except Exception:
            match = None
        if match and match.ident:
            return match.ident
        return f"{point.lat:.2f},{point.lon:.2f}"

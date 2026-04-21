"""Braille-based text-mode map for the TUI.

The canvas is a character grid where each cell encodes a 2x4 dot
sub-grid via Unicode braille characters. Trace points are projected
into the dot space and connected with Bresenham line segments so the
output reads as a continuous path instead of loose dots.

Real cartography still lives in the GUI export (Leaflet via
``adsbtrack gui``); this surface is the TUI's at-a-glance trace view.
"""

from __future__ import annotations

from rich.text import Text
from textual.app import ComposeResult
from textual.containers import Vertical
from textual.widget import Widget

from ..braille import BrailleCanvas
from ..queries import TracePoint, distinct_dates_for_icao, load_trace_points
from ..widgets import (
    ACCENT_CYAN,
    ACCENT_OK,
    ACCENT_VIOLET,
    FG_0,
    FG_2,
    PageHeader,
)

_SOURCE_COLOUR = {
    "adsb_icao": "#4ec07a",
    "adsb_other": "#4ec07a",
    "mlat": "#6b7885",
    "tisb_icao": "#f2b136",
    "tisb_other": "#f2b136",
    "adsr_icao": "#4fb8e0",
    "adsc": "#c24bd6",
    "other": "#6b7885",
    "mode_s": "#6b7885",
}


def _project_to_dots(points: list[TracePoint], dot_w: int, dot_h: int) -> list[tuple[int, int, str]]:
    """Project trace points into dot coordinates on the braille canvas.

    Returns ``(dot_x, dot_y, source)`` triples in input order. Lat/lon
    is projected into ``[0, dot_w-1] x [0, dot_h-1]`` with lat inverted
    (north = top of screen). A tiny epsilon protects against a
    degenerate bbox from a single-point trace.
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
    out: list[tuple[int, int, str]] = []
    for p in points:
        x = int((p.lon - lon_min) / (lon_max - lon_min) * (dot_w - 1))
        y = dot_h - 1 - int((p.lat - lat_min) / (lat_max - lat_min) * (dot_h - 1))
        out.append((x, y, p.source))
    return out


def _render_trace(points: list[TracePoint], cols: int, rows: int) -> str:
    """Rasterise the trace onto a new braille canvas sized ``cols x rows``."""
    canvas = BrailleCanvas(cols=cols, rows=rows)
    projected = _project_to_dots(points, canvas.dot_width, canvas.dot_height)
    for (x0, y0, src), (x1, y1, _) in zip(projected, projected[1:], strict=False):
        canvas.line(x0, y0, x1, y1, _SOURCE_COLOUR.get(src, FG_0))
    # Drop a highlighted dot at the final position so the user can see
    # where the trace ends.
    if projected:
        last_x, last_y, last_src = projected[-1]
        canvas.set(last_x, last_y, _SOURCE_COLOUR.get(last_src, FG_0))
    return canvas.render()


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
        self._points: list[TracePoint] = []

    def set_points(self, points: list[TracePoint]) -> None:
        self._points = points
        self.refresh()

    def on_resize(self) -> None:
        self.refresh()

    def render(self) -> Text:
        w, h = self.size.width, self.size.height
        # Reserve one row for the legend.
        grid_h = max(1, h - 1)
        if not self._points or w <= 2 or grid_h <= 2:
            return Text.from_markup(
                f"[{FG_2}]no trace points available. select an aircraft (1) with trace data, then hit 5.[/]"
            )
        body = _render_trace(self._points, cols=w, rows=grid_h)
        legend = (
            f"[{ACCENT_OK}]● adsb[/]   [{FG_2}]● mlat[/]   [#f2b136]● tisb[/]   "
            f"[{ACCENT_CYAN}]● adsr[/]   [{ACCENT_VIOLET}]● adsc[/]"
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
        if self._icao is None:
            self._canvas.set_points([])
            self._header.set_crumb("select an aircraft first")
            self._header.set_trailing("")
            return
        if self._date is None:
            dates = distinct_dates_for_icao(self.app.db, self._icao)
            if not dates:
                self._canvas.set_points([])
                self._header.set_title(self._icao)
                self._header.set_crumb("no trace data")
                self._header.set_trailing("")
                return
            self._date = dates[0]
        points = load_trace_points(self.app.db, self._icao, self._date)
        self._canvas.set_points(points)
        if not points:
            self._header.set_title(self._icao)
            self._header.set_crumb(f"map / {self._date} (no trace points)")
            self._header.set_trailing("")
            return
        lats = [p.lat for p in points]
        lons = [p.lon for p in points]
        self._header.set_title(self._icao)
        self._header.set_crumb(f"map / {self._date}")
        self._header.set_trailing(
            f"{len(points):,} points   bbox ({min(lats):.3f},{min(lons):.3f})-({max(lats):.3f},{max(lons):.3f})"
        )

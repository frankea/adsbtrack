"""Text-mode map view: trace points rendered into a character grid.

Textual has no native map widget, so we project trace lat/lon into a
character grid coloured by readsb source tag. The canvas sizes itself
to the available pane dimensions and re-projects on resize - the old
fixed 80x24 grid left most of a wide terminal black.

Real cartography lives in the GUI export (Leaflet via
``adsbtrack gui``). This is the TUI's at-a-glance trace shape.
"""

from __future__ import annotations

from rich.text import Text
from textual.app import ComposeResult
from textual.containers import Vertical
from textual.widget import Widget

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


def _project_points(points: list[TracePoint], width: int, height: int) -> dict[tuple[int, int], str]:
    if not points or width <= 0 or height <= 0:
        return {}
    lats = [p.lat for p in points]
    lons = [p.lon for p in points]
    lat_min, lat_max = min(lats), max(lats)
    lon_min, lon_max = min(lons), max(lons)
    if lat_max == lat_min:
        lat_max = lat_min + 0.0001
    if lon_max == lon_min:
        lon_max = lon_min + 0.0001
    grid: dict[tuple[int, int], str] = {}
    for p in points:
        col = int((p.lon - lon_min) / (lon_max - lon_min) * (width - 1))
        row = height - 1 - int((p.lat - lat_min) / (lat_max - lat_min) * (height - 1))
        col = max(0, min(width - 1, col))
        row = max(0, min(height - 1, row))
        grid[(row, col)] = p.source
    return grid


def _render_grid(grid: dict[tuple[int, int], str], width: int, height: int) -> str:
    lines: list[str] = []
    for r in range(height):
        row_chars: list[str] = []
        for c in range(width):
            src = grid.get((r, c))
            if src is None:
                row_chars.append(" ")
                continue
            colour = _SOURCE_COLOUR.get(src, FG_0)
            row_chars.append(f"[{colour}]*[/]")
        lines.append("".join(row_chars))
    return "\n".join(lines)


class MapCanvas(Widget):
    """Adaptive text-mode trace canvas.

    Reads its own ``self.size`` at render time so the grid fills
    whatever width/height the containing pane offers. Repaints on
    resize via ``on_resize``.
    """

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
        self._bbox: tuple[float, float, float, float] | None = None

    def set_points(self, points: list[TracePoint]) -> None:
        self._points = points
        if points:
            lats = [p.lat for p in points]
            lons = [p.lon for p in points]
            self._bbox = (min(lats), max(lats), min(lons), max(lons))
        else:
            self._bbox = None
        self.refresh()

    def on_resize(self) -> None:
        self.refresh()

    def render(self) -> Text:
        w, h = self.size.width, self.size.height
        # Reserve the bottom row for the legend line so points do not
        # overlap it.
        grid_h = max(1, h - 1)
        if not self._points or w <= 2 or grid_h <= 2:
            return Text.from_markup(f"[{FG_2}]no trace points available (resize the pane if this is wrong)[/]")
        grid = _project_points(self._points, w, grid_h)
        body = _render_grid(grid, w, grid_h)
        legend = (
            f"[{ACCENT_OK}]* adsb[/]   [{FG_2}]* mlat[/]   [#f2b136]* tisb[/]   "
            f"[{ACCENT_CYAN}]* adsr[/]   [{ACCENT_VIOLET}]* adsc[/]"
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

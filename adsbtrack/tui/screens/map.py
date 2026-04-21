"""Text-mode map view: trace points projected into a character grid.

Textual has no native map widget, so we render trace points into a
fixed-width character grid coloured by readsb source. Good enough for
at-a-glance trace shape inspection; the real cartographic view lives
in the GUI export.
"""

from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.screen import Screen
from textual.widgets import Static

from ..queries import TracePoint, distinct_dates_for_icao, load_trace_points
from ..widgets import PageHeader

_GRID_W = 80
_GRID_H = 24

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


def _project_points(points: list[TracePoint]) -> tuple[dict[tuple[int, int], str], tuple[float, float, float, float]]:
    """Project lat/lon to a _GRID_W x _GRID_H grid; return a cell -> source dict."""
    if not points:
        return {}, (0.0, 0.0, 0.0, 0.0)
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
        col = int((p.lon - lon_min) / (lon_max - lon_min) * (_GRID_W - 1))
        row = _GRID_H - 1 - int((p.lat - lat_min) / (lat_max - lat_min) * (_GRID_H - 1))
        col = max(0, min(_GRID_W - 1, col))
        row = max(0, min(_GRID_H - 1, row))
        grid[(row, col)] = p.source
    return grid, (lat_min, lat_max, lon_min, lon_max)


def _render_grid(grid: dict[tuple[int, int], str]) -> str:
    lines: list[str] = []
    for r in range(_GRID_H):
        row_chars: list[str] = []
        for c in range(_GRID_W):
            src = grid.get((r, c))
            if src is None:
                row_chars.append(" ")
                continue
            colour = _SOURCE_COLOUR.get(src, "#e4ecf3")
            row_chars.append(f"[{colour}]*[/]")
        lines.append("".join(row_chars))
    return "\n".join(lines)


class MapScreen(Screen):
    """Trace playback for one aircraft, one date."""

    BINDINGS = [("escape", "back", "Back")]

    def __init__(self, icao: str, date: str | None = None) -> None:
        super().__init__()
        self._icao = icao
        self._date = date
        self._header = PageHeader(icao, crumb="map")
        self._body = Static("loading...", id="map-body")

    def compose(self) -> ComposeResult:
        with Vertical():
            yield self._header
            yield self._body

    def on_mount(self) -> None:
        if self._date is None:
            dates = distinct_dates_for_icao(self.app.db, self._icao)
            if not dates:
                self._body.update(f"no trace data for {self._icao}")
                return
            self._date = dates[0]
        self._refresh()

    def action_back(self) -> None:
        self.app.pop_screen()

    def _refresh(self) -> None:
        points = load_trace_points(self.app.db, self._icao, self._date or "")
        grid, bounds = _project_points(points)
        if not grid:
            self._body.update(f"no trace points on {self._date} for {self._icao}")
            return
        lat_min, lat_max, lon_min, lon_max = bounds
        legend = (
            f"[#6b7885]bbox[/] ({lat_min:.3f},{lon_min:.3f})-({lat_max:.3f},{lon_max:.3f})  "
            f"[#6b7885]points[/] {len(points):,}\n"
            "[#4ec07a]* adsb[/]  [#6b7885]* mlat[/]  [#f2b136]* tisb[/]  "
            "[#4fb8e0]* adsr[/]  [#c24bd6]* adsc[/]\n"
        )
        self._body.update(legend + "\n" + _render_grid(grid))
        self._header.set_crumb(f"map / {self._date}")
        self._header.set_trailing(f"{len(points):,} points")

"""Text-mode map view: trace points rendered into a character grid.

Textual has no native map widget. We project the lat/lon of the loaded
trace into an 80x24 character grid and colour each cell by the readsb
source tag. Good enough for at-a-glance trace inspection; real
cartography lives in the GUI export.
"""

from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.widgets import Static

from ..queries import TracePoint, distinct_dates_for_icao, load_trace_points
from ..widgets import (
    ACCENT_CYAN,
    ACCENT_OK,
    ACCENT_VIOLET,
    FG_0,
    FG_2,
    PageHeader,
)

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


def _project_points(
    points: list[TracePoint],
) -> tuple[dict[tuple[int, int], str], tuple[float, float, float, float]]:
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
            colour = _SOURCE_COLOUR.get(src, FG_0)
            row_chars.append(f"[{colour}]*[/]")
        lines.append("".join(row_chars))
    return "\n".join(lines)


class MapView(Vertical):
    """Trace playback for one aircraft, one date."""

    def __init__(self) -> None:
        super().__init__(id="view-map")
        self._icao: str | None = None
        self._date: str | None = None
        self._header = PageHeader("map", crumb="select an aircraft first", widget_id="map-header")
        self._body = Static(" ", id="map-body")

    def compose(self) -> ComposeResult:
        yield self._header
        yield self._body

    def set_icao(self, icao: str | None) -> None:
        self._icao = icao
        self._date = None
        self.refresh_data()

    def refresh_data(self) -> None:
        if self._icao is None:
            self._body.update(f"[{FG_2}]no aircraft selected. press 1 and pick one.[/]")
            self._header.set_crumb("select an aircraft first")
            return
        if self._date is None:
            dates = distinct_dates_for_icao(self.app.db, self._icao)
            if not dates:
                self._body.update(f"[{FG_2}]no trace data for {self._icao}[/]")
                self._header.set_title(self._icao)
                self._header.set_crumb("no trace data")
                return
            self._date = dates[0]
        points = load_trace_points(self.app.db, self._icao, self._date)
        grid, bounds = _project_points(points)
        if not grid:
            self._body.update(f"[{FG_2}]no trace points on {self._date} for {self._icao}[/]")
            return
        lat_min, lat_max, lon_min, lon_max = bounds
        legend = (
            f"[{FG_2}]bbox[/] ({lat_min:.3f},{lon_min:.3f})-({lat_max:.3f},{lon_max:.3f})   "
            f"[{FG_2}]points[/] [{FG_0}]{len(points):,}[/]\n"
            f"[{ACCENT_OK}]* adsb[/]   [{FG_2}]* mlat[/]   [#f2b136]* tisb[/]   "
            f"[{ACCENT_CYAN}]* adsr[/]   [{ACCENT_VIOLET}]* adsc[/]\n"
        )
        self._body.update(legend + "\n" + _render_grid(grid))
        self._header.set_title(self._icao)
        self._header.set_crumb(f"map / {self._date}")
        self._header.set_trailing(f"{len(points):,} points")

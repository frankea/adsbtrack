"""Braille-based text-mode map for the TUI.

The canvas is a character grid where each cell encodes a 2x4 dot
sub-grid via Unicode braille characters. Trace points are projected
into the dot space and connected with Bresenham line segments so the
output reads as a continuous path instead of loose dots.

A HUD layer adds four panels around the canvas to mirror the concept's
SVG map: layers list + trace-info on top, scalebar + scrubber on
bottom. Real cartography still lives in the GUI export (Leaflet via
``adsbtrack gui``); this surface is the TUI's at-a-glance trace view.
"""

from __future__ import annotations

import math

from rich.text import Text
from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widget import Widget
from textual.widgets import Label

from ..braille import BrailleCanvas
from ..queries import TracePoint, distinct_dates_for_icao, load_trace_points
from ..widgets import (
    ACCENT_AMBER,
    ACCENT_CYAN,
    ACCENT_OK,
    ACCENT_VIOLET,
    DOT,
    FG_0,
    FG_1,
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
    canvas = BrailleCanvas(cols=cols, rows=rows)
    projected = _project_to_dots(points, canvas.dot_width, canvas.dot_height)
    for (x0, y0, src), (x1, y1, _) in zip(projected, projected[1:], strict=False):
        canvas.line(x0, y0, x1, y1, _SOURCE_COLOUR.get(src, FG_0))
    if projected:
        last_x, last_y, last_src = projected[-1]
        canvas.set(last_x, last_y, _SOURCE_COLOUR.get(last_src, FG_0))
    return canvas.render()


def _bbox_span_nm(points: list[TracePoint]) -> float:
    if len(points) < 2:
        return 0.0
    lats = [p.lat for p in points]
    lons = [p.lon for p in points]
    mean_lat = sum(lats) / len(lats)
    dlat_nm = (max(lats) - min(lats)) * 60.0
    dlon_nm = (max(lons) - min(lons)) * 60.0 * math.cos(math.radians(mean_lat))
    return max(dlat_nm, dlon_nm)


class MapCanvas(Widget):
    """Adaptive text-mode trace canvas backed by a braille raster."""

    DEFAULT_CSS = """
    MapCanvas {
        height: 1fr;
        width: 1fr;
        background: #0a1018;
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
        if not self._points or w <= 2 or h <= 2:
            return Text.from_markup(
                f"[{FG_2}]no trace points available. select an aircraft (1) with trace data, then hit 5.[/]"
            )
        body = _render_trace(self._points, cols=w, rows=h)
        return Text.from_markup(body)


class _HudLabel(Label):
    """Common styling for the HUD strips above/below the canvas."""

    DEFAULT_CSS = """
    _HudLabel {
        height: 1;
        width: auto;
        padding: 0 1;
        background: #141b23;
        color: #aab6c2;
    }
    """


class MapLayersStrip(_HudLabel):
    """Top-left: colour-coded list of active layers."""

    def __init__(self) -> None:
        super().__init__(self._build(), id="map-layers")

    def _build(self) -> Text:
        chunks = [
            f"[{FG_2}]LAYERS[/]",
            f"[{ACCENT_OK}]●[/] ADS-B",
            f"[{FG_2}]●[/] MLAT",
            f"[{ACCENT_AMBER}]●[/] TIS-B",
            f"[{ACCENT_CYAN}]●[/] ADS-R",
            f"[{ACCENT_VIOLET}]●[/] ADS-C",
            f"[{FG_2}]|[/]",
            f"[{ACCENT_AMBER}]●[/] gaps",
            f"[{ACCENT_VIOLET}]●[/] spoof",
        ]
        return Text.from_markup("  ".join(chunks))


class MapTraceInfoStrip(_HudLabel):
    """Top-right: one-line readout of the last trace point."""

    DEFAULT_CSS = """
    MapTraceInfoStrip {
        height: 1;
        width: auto;
        padding: 0 1;
        background: #141b23;
        color: #aab6c2;
        content-align: right middle;
    }
    """

    def __init__(self) -> None:
        self._text = Text.from_markup(f"[{FG_2}](no trace)[/]")
        super().__init__(self._text, id="map-info")

    def set_point(self, point: TracePoint | None, origin_ts: float | None = None) -> None:
        if point is None:
            self.update(Text.from_markup(f"[{FG_2}](no trace)[/]"))
            return
        delta = ""
        if origin_ts is not None:
            secs = max(0, int(point.ts - origin_ts))
            m = secs // 60
            s = secs % 60
            h = m // 60
            m %= 60
            delta = f"t+{h}:{m:02d}:{s:02d}" if h else f"t+{m}m{s:02d}s"
        src = point.source.replace("_", "-")
        src_colour = _SOURCE_COLOUR.get(point.source, FG_0)
        alt = f"{point.alt_ft:,} ft" if point.alt_ft is not None else "ground"
        parts = [
            f"[{FG_2}]LAST {delta}[/]",
            f"[{FG_2}]src[/] [{src_colour}]{src}[/]",
            f"[{FG_2}]lat[/] [{FG_0}]{point.lat:.4f}[/]",
            f"[{FG_2}]lon[/] [{FG_0}]{point.lon:.4f}[/]",
            f"[{FG_2}]alt[/] [{FG_0}]{alt}[/]",
        ]
        self.update(Text.from_markup("  ".join(parts)))


class MapScalebarStrip(_HudLabel):
    """Bottom-left: scale bar with distance + rough zoom."""

    def __init__(self) -> None:
        self._span_nm = 0.0
        super().__init__(self._build(), id="map-scalebar")

    def set_span(self, nm: float) -> None:
        self._span_nm = nm
        self.update(self._build())

    def _build(self) -> Text:
        bar = "━" * 10
        if self._span_nm <= 0:
            return Text.from_markup(f"[{FG_0}]{bar}[/]  [{FG_2}]no trace[/]")
        # zoom heuristic: log2 of world-span over current span; closer to 0 = zoomed out.
        zoom = max(1, min(18, int(round(math.log2(4000.0 / max(1.0, self._span_nm)) + 4))))
        return Text.from_markup(
            f"[{FG_0}]{bar}[/]  [{FG_1}]{self._span_nm:.0f} nm[/] [{FG_2}]{DOT}[/] [{FG_1}]zoom {zoom}[/]"
        )


class MapScrubberStrip(_HudLabel):
    """Bottom-right: static duration readout drawn as a full progress bar.

    The bar always renders at 1.0 progress; there is no playback or scrub
    input. It exists to show total trace duration in the bottom-right of
    the canvas.
    """

    DEFAULT_CSS = """
    MapScrubberStrip {
        height: 1;
        width: auto;
        padding: 0 1;
        background: #141b23;
        color: #aab6c2;
        content-align: right middle;
    }
    """

    def __init__(self) -> None:
        self._progress = 0.0
        self._label = "--:-- / --:--"
        super().__init__(self._build(), id="map-scrubber")

    def set_progress(self, progress: float, label: str) -> None:
        self._progress = max(0.0, min(1.0, progress))
        self._label = label
        self.update(self._build())

    def _build(self) -> Text:
        width = 24
        fill = int(round(self._progress * width))
        bar = f"[{ACCENT_CYAN}]{'█' * fill}[/][{FG_2}]{'░' * (width - fill)}[/]"
        return Text.from_markup(f"[{FG_2}]▸[/] {bar}  [{FG_1}]{self._label}[/]  [{FG_2}]×1.0[/]")


class _HudRow(Horizontal):
    """Horizontal strip that pins one child left and another right."""

    DEFAULT_CSS = """
    _HudRow {
        height: 1;
        width: 1fr;
        background: #141b23;
    }
    _HudRow > Label.left { width: auto; }
    _HudRow > .spacer { width: 1fr; background: #141b23; }
    _HudRow > Label.right { width: auto; }
    """


class MapView(Vertical):
    """Trace playback for one aircraft, one date."""

    def __init__(self) -> None:
        super().__init__(id="view-map")
        self._icao: str | None = None
        self._date: str | None = None
        self._header = PageHeader("map", crumb="select an aircraft first", widget_id="map-header")
        self._canvas = MapCanvas()
        self._layers = MapLayersStrip()
        self._info = MapTraceInfoStrip()
        self._scalebar = MapScalebarStrip()
        self._scrubber = MapScrubberStrip()

    def compose(self) -> ComposeResult:
        yield self._header
        self._layers.add_class("left")
        self._info.add_class("right")
        self._scalebar.add_class("left")
        self._scrubber.add_class("right")
        yield _HudRow(self._layers, Label(" ", classes="spacer"), self._info, id="map-hud-top")
        yield self._canvas
        yield _HudRow(self._scalebar, Label(" ", classes="spacer"), self._scrubber, id="map-hud-bottom")

    def set_icao(self, icao: str | None) -> None:
        self._icao = icao
        self._date = None
        self.refresh_data()

    def refresh_data(self) -> None:
        if self._icao is None:
            self._canvas.set_points([])
            self._info.set_point(None)
            self._scalebar.set_span(0)
            self._scrubber.set_progress(0, "--:-- / --:--")
            self._header.set_crumb("select an aircraft first")
            self._header.set_trailing("")
            return
        if self._date is None:
            dates = distinct_dates_for_icao(self.app.db, self._icao)
            if not dates:
                self._canvas.set_points([])
                self._info.set_point(None)
                self._scalebar.set_span(0)
                self._scrubber.set_progress(0, "--:-- / --:--")
                self._header.set_title(self._icao)
                self._header.set_crumb("no trace data")
                self._header.set_trailing("")
                return
            self._date = dates[0]
        points = load_trace_points(self.app.db, self._icao, self._date)
        self._canvas.set_points(points)
        if not points:
            self._info.set_point(None)
            self._scalebar.set_span(0)
            self._scrubber.set_progress(0, "--:-- / --:--")
            self._header.set_title(self._icao)
            self._header.set_crumb(f"{self._date} (no trace points)")
            self._header.set_trailing("")
            return
        lats = [p.lat for p in points]
        lons = [p.lon for p in points]
        self._header.set_title(self._icao)
        self._header.set_crumb(f"{self._date}")
        self._header.set_trailing(
            f"{len(points):,} points {DOT} bbox ({min(lats):.3f},{min(lons):.3f})-({max(lats):.3f},{max(lons):.3f})"
        )
        origin = points[0].ts
        tail = points[-1]
        self._info.set_point(tail, origin_ts=origin)
        self._scalebar.set_span(_bbox_span_nm(points))
        dur = tail.ts - origin
        self._scrubber.set_progress(1.0, _fmt_time_range(dur, dur))


def _fmt_time_range(pos_secs: float, total_secs: float) -> str:
    def mmss(secs: float) -> str:
        secs = max(0, int(secs))
        h = secs // 3600
        m = (secs % 3600) // 60
        s = secs % 60
        if h:
            return f"{h:d}:{m:02d}:{s:02d}"
        return f"{m:d}:{s:02d}"

    return f"{mmss(pos_secs)} / {mmss(total_secs)}"

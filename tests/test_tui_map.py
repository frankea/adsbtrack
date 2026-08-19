"""Tests for the map view (adsbtrack/tui/views/map.py) and its BrailleCanvas
backend, covering the eight review findings fixed in Task 17 (F1-F8):

* F1: legend degrees-per-cell is computed from the actual bbox/canvas size.
* F2: empty altitude data renders "ground", never "0 .. 0 ft".
* F3: LAYERS aggregates sources missing from _SOURCE_LEGEND into "other".
* F4: endpoint labels never overwrite their own marker cell.
* F5: _airport_or_coords only swallows sqlite3.Error.
* F6: the trace-gap threshold is a Config field, not a module constant.
* F7: BrailleCanvas.line gained a dash param; a public cells() iterator
  replaces private _bits/_colours access; the panel-gate width is derived
  from the panel constants instead of a magic 56.
* F8: _rasterise_trace no-ops on an empty projection instead of IndexError.

Most of this module is pure-function tests with no Textual app needed;
one Pilot smoke test at the bottom mounts the real MapView against a
seeded tmp DB to prove the F7 compositor refactor (coalesced Text runs,
public cell iterator, dashed BrailleCanvas.line) still renders.
"""

from __future__ import annotations

import asyncio
import sqlite3
from collections import Counter
from datetime import UTC, datetime

import pytest

pytest.importorskip("textual")  # tui extra: pyproject [project.optional-dependencies].tui

from adsbtrack.config import Config  # noqa: E402
from adsbtrack.db import Database  # noqa: E402
from adsbtrack.models import Flight  # noqa: E402
from adsbtrack.tui.app import AdsbtrackApp  # noqa: E402
from adsbtrack.tui.braille import BrailleCanvas  # noqa: E402
from adsbtrack.tui.queries import TracePoint  # noqa: E402
from adsbtrack.tui.views.map import (  # noqa: E402
    _FOOTER_SUFFIX,
    _INFO_PANEL_WIDTH,
    _LAYERS_PANEL_WIDTH,
    _MIN_PANEL_COLS,
    _PANEL_MARGIN,
    _SOURCE_LEGEND,
    MapView,
    _alt_range_str,
    _build_ctx,
    _draw_endpoint,
    _footer_legend,
    _Grid,
    _layers_panel,
    _rasterise_trace,
    _scale_deg_per_cell,
    _usable_dot_span,
)

# ---------------------------------------------------------------------------
# F1: legend scale computed from the actual bbox/canvas size
# ---------------------------------------------------------------------------


def test_usable_dot_span_matches_ten_percent_inset():
    # 100x80 dots -> 10% inset on each axis, minus the extra -1 the
    # projector reserves so a point never lands exactly on the far edge.
    inset_x, inset_y, usable_w, usable_h = _usable_dot_span(100, 80)
    assert inset_x == 10
    assert inset_y == 8
    assert usable_w == 100 - 2 * 10 - 1
    assert usable_h == 80 - 2 * 8 - 1


def test_scale_deg_per_cell_known_bbox():
    # A 10deg x 4deg bbox on a 200x80 dot canvas (cols=100, rows=20 cells).
    # usable_w = 200 - 2*20 - 1 = 159; usable_h = 80 - 2*8 - 1 = 63.
    # lon_per_cell = 10/159*2 ; lat_per_cell = 4/63*4. The lat term wins.
    scale = _scale_deg_per_cell(lat_min=10.0, lat_max=14.0, lon_min=-80.0, lon_max=-70.0, dot_w=200, dot_h=80)
    lon_per_cell = 10.0 / 159 * 2
    lat_per_cell = 4.0 / 63 * 4
    assert scale == pytest.approx(max(lon_per_cell, lat_per_cell))
    # Sanity: this is NOT the old hardcoded 0.01 guess.
    assert scale != pytest.approx(0.01)


def test_scale_deg_per_cell_varies_with_bbox_size():
    """A transcontinental bbox and a helicopter-hop bbox on the same canvas
    must produce very different scales -- that's the whole point of F1."""
    small = _scale_deg_per_cell(40.0, 40.05, -74.0, -73.95, dot_w=200, dot_h=80)
    large = _scale_deg_per_cell(25.0, 48.0, -124.0, -67.0, dot_w=200, dot_h=80)
    assert large > small * 100


# ---------------------------------------------------------------------------
# F2: empty altitude data renders "ground"
# ---------------------------------------------------------------------------


def test_alt_range_str_empty_is_ground():
    assert _alt_range_str(None, None) == "ground"


def test_alt_range_str_present():
    assert _alt_range_str(1000, 5000) == "1,000 .. 5,000 ft"


# ---------------------------------------------------------------------------
# F3: LAYERS panel aggregates unlisted sources into "other"
# ---------------------------------------------------------------------------


def test_layers_panel_aggregates_unknown_sources():
    counts = Counter({"adsb_icao": 40, "mode_s": 30, "unknown": 20, "mlat": 10})
    total = sum(counts.values())
    rows = _layers_panel(counts, total)
    # header + 5 known legend rows + 1 "other" row
    assert len(rows) == 1 + len(_SOURCE_LEGEND) + 1
    pct_texts = [seg[-1][1] for seg in rows[1:]]  # last segment of each row is the pct text
    pcts = [float(p.strip().rstrip("%")) for p in pct_texts]
    assert sum(pcts) == pytest.approx(100.0, abs=0.2)
    other_row_text = "".join(seg[1] for seg in rows[-1])
    assert "other" in other_row_text
    # mode_s (30) + unknown (20) = 50 of 100 = 50.0%
    assert "50.0%" in other_row_text


def test_layers_panel_all_known_sources_other_row_still_present_but_dim():
    counts = Counter({"adsb_icao": 100})
    rows = _layers_panel(counts, total=100)
    assert len(rows) == 1 + len(_SOURCE_LEGEND) + 1
    other_row_text = "".join(seg[1] for seg in rows[-1])
    assert "0.0%" in other_row_text


def test_layers_panel_empty_total_no_division_error():
    rows = _layers_panel(Counter(), total=0)
    assert len(rows) == 1 + len(_SOURCE_LEGEND) + 1


# ---------------------------------------------------------------------------
# F4: endpoint label never overwrites its own marker cell
# ---------------------------------------------------------------------------


def test_draw_endpoint_left_placement_never_overwrites_marker():
    """Review's exact arithmetic: 30-col pane, marker at col 14 (dot x=28,
    y=0 -> row 0), label 18 chars long. Right placement doesn't fit
    (14+2+18=34 > 30); left placement, uncapped, would start at
    max(0, 14-1-18) = 0 and span cols 0..17, overwriting the marker at
    col 14. The fix must truncate instead."""
    grid = _Grid(rows=1, cols=30)
    label = "A" * 18
    _draw_endpoint(grid, (28, 0, "adsb_icao"), label, "#4ec07a")
    assert grid.chars[0][14] == "●"  # marker glyph intact, painted with its own colour
    # The 1-cell gap immediately left of the marker must stay untouched.
    assert grid.chars[0][13] == " "
    assert grid.colour[0][13] is None
    # No label character landed past the marker column either.
    assert all(grid.colour[0][c] is None for c in range(15, 30))


def test_draw_endpoint_right_placement_when_it_fits():
    grid = _Grid(rows=1, cols=30)
    _draw_endpoint(grid, (4, 0, "adsb_icao"), "end KBOS", "#4fb8e0")
    assert grid.chars[0][2] == "●"
    assert "".join(grid.chars[0][4:12]) == "end KBOS"


def test_draw_endpoint_out_of_bounds_row_is_noop():
    grid = _Grid(rows=1, cols=10)
    _draw_endpoint(grid, (0, 100, "adsb_icao"), "x", "#ffffff")  # row way off-grid
    assert all(ch == " " for row in grid.chars for ch in row)


# ---------------------------------------------------------------------------
# F5: _airport_or_coords narrows the except clause to sqlite3.Error
# ---------------------------------------------------------------------------


class _StandInApp:
    """Duck-typed stand-in for the app object -- only its .config attribute
    is read by _airport_or_coords."""

    def __init__(self) -> None:
        self.config = Config()


class _StandInSelf:
    """Duck-typed stand-in for MapView -- _airport_or_coords only reads
    self.app.config, so a full Widget/App isn't needed to unit test it."""

    def __init__(self) -> None:
        self.app = _StandInApp()


def test_airport_or_coords_falls_back_on_sqlite_error(tmp_path, monkeypatch):
    import adsbtrack.tui.views.map as map_module

    def _raise_sqlite_error(db, lat, lon, config):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(map_module, "find_nearest_airport", _raise_sqlite_error)
    with Database(tmp_path / "airport.db") as db:
        point = TracePoint(ts=0.0, lat=40.0, lon=-74.0, alt_ft=1000, source="adsb_icao")
        result = MapView._airport_or_coords(_StandInSelf(), db, point)
    assert result == "40.00,-74.00"


def test_airport_or_coords_does_not_swallow_other_exceptions(tmp_path, monkeypatch):
    import adsbtrack.tui.views.map as map_module

    def _raise_value_error(db, lat, lon, config):
        raise ValueError("not a sqlite3.Error")

    monkeypatch.setattr(map_module, "find_nearest_airport", _raise_value_error)
    with Database(tmp_path / "airport2.db") as db:
        point = TracePoint(ts=0.0, lat=40.0, lon=-74.0, alt_ft=1000, source="adsb_icao")
        with pytest.raises(ValueError):
            MapView._airport_or_coords(_StandInSelf(), db, point)


# ---------------------------------------------------------------------------
# Route crumb (ports PR #16): "KSPG > KHKY" for the displayed date's
# first flight, reusing the flights view's issue #18 ~ICAO fallback so
# a near-match-only endpoint renders consistently in both places.
# ---------------------------------------------------------------------------


def _insert_flight(db: Database, **overrides) -> None:
    fields = dict(
        icao="aaa111",
        takeoff_time=datetime(2026, 3, 1, 12, 0, tzinfo=UTC),
        takeoff_lat=40.0,
        takeoff_lon=-74.0,
        takeoff_date="2026-03-01",
    )
    fields.update(overrides)
    db.insert_flight(Flight(**fields))


def test_route_for_returns_first_flight_route_for_date(tmp_path):
    from adsbtrack.tui.views.map import _route_for

    with Database(tmp_path / "route.db") as db:
        _insert_flight(db, origin_icao="KSPG", destination_icao="KHKY")
        route = _route_for(db, "aaa111", "2026-03-01")
    assert route == "KSPG > KHKY"


def test_route_for_picks_earliest_flight_when_multiple_that_date(tmp_path):
    from adsbtrack.tui.views.map import _route_for

    with Database(tmp_path / "route_multi.db") as db:
        _insert_flight(
            db,
            takeoff_time=datetime(2026, 3, 1, 18, 0, tzinfo=UTC),
            origin_icao="KHKY",
            destination_icao="KJFK",
        )
        _insert_flight(
            db,
            takeoff_time=datetime(2026, 3, 1, 6, 0, tzinfo=UTC),
            origin_icao="KSPG",
            destination_icao="KHKY",
        )
        route = _route_for(db, "aaa111", "2026-03-01")
    assert route == "KSPG > KHKY", "must be the earliest takeoff_time on the date, not insertion order"


def test_route_for_none_when_no_flight_that_date(tmp_path):
    from adsbtrack.tui.views.map import _route_for

    with Database(tmp_path / "route_none.db") as db:
        _insert_flight(db, origin_icao="KSPG", destination_icao="KHKY", takeoff_date="2026-03-02")
        route = _route_for(db, "aaa111", "2026-03-01")
    assert route is None


def test_route_for_uses_near_match_tilde_fallback_for_origin(tmp_path):
    """Issue #18: origin_icao missed the on-field match threshold but a
    nearest_origin_icao was found -- the route crumb must show the same
    ~ICAO marker the flights table shows for this exact flight."""
    from adsbtrack.tui.views.map import _route_for

    with Database(tmp_path / "route_tilde_origin.db") as db:
        _insert_flight(db, origin_icao=None, nearest_origin_icao="KTNX", destination_icao="KHKY")
        route = _route_for(db, "aaa111", "2026-03-01")
    assert route == "~KTNX > KHKY"


def test_route_for_uses_near_match_tilde_fallback_for_signal_lost_destination(tmp_path):
    from adsbtrack.tui.views.map import _route_for

    with Database(tmp_path / "route_tilde_dest.db") as db:
        _insert_flight(
            db,
            origin_icao="KSPG",
            destination_icao=None,
            probable_destination_icao="KTNX",
            landing_type="signal_lost",
        )
        route = _route_for(db, "aaa111", "2026-03-01")
    assert route == "KSPG > ~KTNX"


def test_route_for_none_when_destination_unresolvable(tmp_path):
    """No real destination, no probable fallback, and a landing_type that
    isn't signal_lost/dropped_on_approach: _display_destination returns
    None, so the whole route is dropped rather than showing a bare
    origin with nothing after it."""
    from adsbtrack.tui.views.map import _route_for

    with Database(tmp_path / "route_unresolvable.db") as db:
        _insert_flight(db, origin_icao="KSPG", destination_icao=None, landing_type="uncertain")
        route = _route_for(db, "aaa111", "2026-03-01")
    assert route is None


# ---------------------------------------------------------------------------
# F6: gap threshold lives in Config
# ---------------------------------------------------------------------------


def test_map_trace_gap_secs_is_a_config_field():
    assert Config().map_trace_gap_secs == 60.0


def test_build_ctx_carries_gap_secs_from_caller():
    points = [
        TracePoint(ts=0.0, lat=40.0, lon=-74.0, alt_ft=1000, source="adsb_icao"),
        TracePoint(ts=10.0, lat=40.1, lon=-74.1, alt_ft=1200, source="adsb_icao"),
    ]
    ctx = _build_ctx(points, "2026-03-01", "start", "end", gap_secs=12.5)
    assert ctx.gap_secs == 12.5


# ---------------------------------------------------------------------------
# F7: BrailleCanvas.line dash param, public cells() iterator, footer legend
# built from _SOURCE_LEGEND, derived panel-gate width
# ---------------------------------------------------------------------------


def test_braille_line_solid_lights_every_dot_on_path():
    canvas = BrailleCanvas(cols=10, rows=10)
    canvas.line(0, 0, 6, 0, "#ffffff")
    lit = sorted(x for x, y, *_ in _lit_dots(canvas))
    assert lit == list(range(7))


def test_braille_line_dash_pattern_skips_gaps():
    canvas = BrailleCanvas(cols=10, rows=10)
    canvas.line(0, 0, 7, 0, "#f2b136", dash=(2, 2))
    lit_x = sorted(x for x, y, *_ in _lit_dots(canvas))
    # dash(2, 2): steps 0,1 on, 2,3 off, 4,5 on, 6,7 off -> x in {0,1,4,5}
    assert lit_x == [0, 1, 4, 5]


def _lit_dots(canvas: BrailleCanvas) -> list[tuple[int, int]]:
    """Reconstruct which dot coordinates are lit by re-deriving from the
    public cells() iterator (bit membership), for assertions above."""
    dots: list[tuple[int, int]] = []
    for r, c, ch, _colour in canvas.cells():
        mask = ord(ch) - 0x2800
        for dx in (0, 1):
            for dy in range(4):
                bit = {(0, 0): 0, (0, 1): 1, (0, 2): 2, (0, 3): 6, (1, 0): 3, (1, 1): 4, (1, 2): 5, (1, 3): 7}[(dx, dy)]
                if mask & (1 << bit):
                    dots.append((c * 2 + dx, r * 4 + dy))
    return dots


def test_braille_canvas_cells_iterator_yields_only_lit_cells():
    canvas = BrailleCanvas(cols=5, rows=5)
    canvas.set(0, 0, "#ff0000")
    canvas.set(4, 4, "#00ff00")  # dot (4,4) -> cell col=4//2=2, row=4//4=1
    results = sorted((r, c, colour) for r, c, ch, colour in canvas.cells())
    assert results == [(0, 0, "#ff0000"), (1, 2, "#00ff00")]


def test_braille_canvas_render_still_works_and_matches_cells():
    canvas = BrailleCanvas(cols=3, rows=3)
    canvas.line(0, 0, 5, 0, "#4ec07a")
    text = canvas.render()
    assert "[#4ec07a]" in text
    assert text.count("\n") == 2  # 3 rows -> 2 newlines


def test_footer_legend_built_from_source_legend_and_scale():
    legend = _footer_legend(0.125)
    for label, colour, _keys in _SOURCE_LEGEND:
        short = label.lower().replace("-", "")
        assert short in legend
        assert colour in legend
    for suffix in _FOOTER_SUFFIX.values():
        assert suffix in legend
    assert "0.125" in legend


def test_min_panel_cols_is_derived_from_panel_constants():
    assert _MIN_PANEL_COLS == _PANEL_MARGIN + _LAYERS_PANEL_WIDTH + _INFO_PANEL_WIDTH + _PANEL_MARGIN
    assert _MIN_PANEL_COLS == 56  # review's arithmetic: 1+22+32+1


# ---------------------------------------------------------------------------
# F8: _rasterise_trace no-ops when the projection is empty
# ---------------------------------------------------------------------------


def test_rasterise_trace_empty_projection_does_not_raise():
    canvas = BrailleCanvas(cols=5, rows=5)
    points = [
        TracePoint(ts=0.0, lat=40.0, lon=-74.0, alt_ft=1000, source="adsb_icao"),
        TracePoint(ts=10.0, lat=40.1, lon=-74.1, alt_ft=1200, source="adsb_icao"),
    ]
    _rasterise_trace(canvas, points, projected=[], gap_secs=60.0)  # must not raise IndexError
    assert list(canvas.cells()) == []


def test_rasterise_trace_dashes_gaps_longer_than_threshold():
    canvas = BrailleCanvas(cols=20, rows=20)
    points = [
        TracePoint(ts=0.0, lat=40.0, lon=-74.0, alt_ft=1000, source="adsb_icao"),
        TracePoint(ts=1000.0, lat=40.5, lon=-74.5, alt_ft=1000, source="adsb_icao"),
    ]
    projected = [(0, 0, "adsb_icao"), (30, 30, "adsb_icao")]
    _rasterise_trace(canvas, points, projected, gap_secs=60.0)
    colours = {colour for _r, _c, _ch, colour in canvas.cells()}
    assert "#f2b136" in colours  # ACCENT_AMBER dashed-gap colour used


# ---------------------------------------------------------------------------
# Integration: Pilot smoke test against a seeded tmp DB (post F7 refactor)
# ---------------------------------------------------------------------------


async def _settle(app, pilot) -> None:
    for _ in range(500):
        active = [w for w in app.workers if w.state.name in ("PENDING", "RUNNING")]
        if not active:
            return
        await pilot.pause()
        await asyncio.sleep(0.01)
    raise AssertionError("workers did not settle in time")


def _seed_map_aircraft(db_path) -> None:
    """One aircraft with a rich trace: a ground-only leg (F2), a source
    unrecognized by _SOURCE_LEGEND (F3), and a 5-minute signal gap (F7
    dashed line) -- enough to exercise every rendering branch touched
    by this task."""
    with Database(db_path) as db:
        db.insert_flight(
            Flight(
                icao="aaa111",
                takeoff_time=datetime(2026, 3, 1, 12, 0, tzinfo=UTC),
                takeoff_lat=40.0,
                takeoff_lon=-74.0,
                takeoff_date="2026-03-01",
                landing_time=datetime(2026, 3, 1, 14, 0, tzinfo=UTC),
                landing_type="confirmed",
                callsign="UAL1",
                destination_icao="KBOS",
                origin_icao="KEWR",
                duration_minutes=120.0,
                max_altitude=35000,
                cruise_gs_kt=430,
                landing_confidence=0.9,
                mission_type="transport",
            )
        )
        db.conn.execute(
            "INSERT INTO aircraft_registry (icao, registration, type_code, description) VALUES (?, ?, ?, ?)",
            ("aaa111", "N111AA", "B738", "BOEING 737-800"),
        )
        trace = [
            [0, 40.00, -74.00, None, 0, None, None, None, {}, "adsb_icao"],  # ground leg -> None alt (F2)
            [30, 40.02, -74.02, 1500, 140, None, None, None, {}, "adsb_icao"],
            [60, 40.05, -74.05, 5000, 210, None, None, None, {}, "mode_s"],  # unlisted source (F3)
            [90, 40.10, -74.10, 12000, 260, None, None, None, {}, "mystery_feed"],  # unknown (F3)
            [400, 40.30, -74.30, 30000, 430, None, None, None, {}, "adsb_icao"],  # >60s gap -> dashed (F7)
        ]
        db.insert_trace_day(
            "aaa111",
            "2026-03-01",
            {"timestamp": 1772280000.0, "trace": trace},
            source="adsbx",
        )
        db.refresh_aircraft_stats("aaa111")
        db.commit()


def test_map_view_renders_after_f7_refactor(tmp_path):
    """Boots the app, selects the seeded aircraft, opens the map view, and
    confirms the widget tree mounts and MapCanvas produces non-empty Text
    output -- proving the per-character-markup -> coalesced Text run
    refactor, the public BrailleCanvas.cells() path, and the dashed-line
    param all still cooperate end to end."""
    db_path = tmp_path / "map_view.db"
    _seed_map_aircraft(db_path)

    async def scenario() -> None:
        from textual.widgets import ContentSwitcher

        from adsbtrack.tui.views.aircraft import AircraftOpenFlights
        from adsbtrack.tui.views.map import MapCanvas, MapView

        app = AdsbtrackApp(db_path)
        async with app.run_test() as pilot:
            await _settle(app, pilot)
            aircraft_view = app.query_one("#view-aircraft")
            aircraft_view.post_message(AircraftOpenFlights("aaa111"))
            await pilot.pause()
            await _settle(app, pilot)

            await pilot.press("5")
            await _settle(app, pilot)

            switcher = app.query_one(ContentSwitcher)
            assert switcher.current == "view-map"
            map_view = app.query_one(MapView)
            assert map_view is not None
            canvas = app.query_one(MapCanvas)
            rendered = canvas.render()
            plain = rendered.plain
            assert "no trace points" not in plain
            assert len(plain) > 0
            # LAYERS/TRACE panel text should show up somewhere once the
            # pane is wide enough (Pilot's default test size is 80x24).
            assert "LAYERS" in plain or "TRACE" in plain

    asyncio.run(scenario())


def test_map_view_header_crumb_shows_route_for_seeded_flight(tmp_path):
    """_seed_map_aircraft's flight has origin_icao=KEWR, destination_icao=KBOS
    on 2026-03-01; the header crumb for that date must surface that route
    instead of (or alongside) the nearest-airport-to-trace-endpoint labels."""
    db_path = tmp_path / "map_route.db"
    _seed_map_aircraft(db_path)

    async def scenario() -> None:
        from adsbtrack.tui.views.aircraft import AircraftOpenFlights
        from adsbtrack.tui.views.map import MapView

        app = AdsbtrackApp(db_path)
        async with app.run_test() as pilot:
            await _settle(app, pilot)
            aircraft_view = app.query_one("#view-aircraft")
            aircraft_view.post_message(AircraftOpenFlights("aaa111"))
            await pilot.pause()
            await _settle(app, pilot)

            await pilot.press("5")
            await _settle(app, pilot)

            map_view = app.query_one(MapView)
            crumb_text = map_view._header._build().plain
            assert "KEWR > KBOS" in crumb_text

    asyncio.run(scenario())


def test_map_view_uses_app_config_for_trace_gap_secs(tmp_path):
    """MapView must read map_trace_gap_secs off ``app.config`` -- the Config
    the `tui` command loads from config.toml -- instead of constructing its
    own default Config(). Otherwise a config.toml override to
    map_trace_gap_secs never reaches the map's dashed-gap rendering."""
    db_path = tmp_path / "map_config.db"
    _seed_map_aircraft(db_path)

    custom_gap = 12.5
    assert custom_gap != Config().map_trace_gap_secs, "sanity: must actually be an override"

    async def scenario() -> None:
        from adsbtrack.tui.views.aircraft import AircraftOpenFlights
        from adsbtrack.tui.views.map import MapCanvas

        app = AdsbtrackApp(db_path, config=Config(map_trace_gap_secs=custom_gap))
        async with app.run_test() as pilot:
            await _settle(app, pilot)
            aircraft_view = app.query_one("#view-aircraft")
            aircraft_view.post_message(AircraftOpenFlights("aaa111"))
            await pilot.pause()
            await _settle(app, pilot)

            await pilot.press("5")
            await _settle(app, pilot)

            canvas = app.query_one(MapCanvas)
            assert canvas._ctx is not None
            assert canvas._ctx.gap_secs == custom_gap

    asyncio.run(scenario())

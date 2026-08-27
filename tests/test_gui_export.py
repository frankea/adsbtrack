"""Tests for the static GUI exporter."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from adsbtrack.db import Database
from adsbtrack.gui_export import export_gui
from adsbtrack.models import Flight


@pytest.fixture
def exported_bundle(tmp_path):
    db_path = tmp_path / "src.db"
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
                origin_icao="KEWR",
                destination_icao="KBOS",
                duration_minutes=120.0,
                max_altitude=35000,
                cruise_gs_kt=430,
                landing_confidence=0.9,
                mission_type="transport",
            )
        )
        db.insert_flight(
            Flight(
                icao="bbb222",
                takeoff_time=datetime(2026, 3, 2, 9, 0, tzinfo=UTC),
                takeoff_lat=33.9,
                takeoff_lon=-118.4,
                takeoff_date="2026-03-02",
                landing_time=datetime(2026, 3, 2, 15, 0, tzinfo=UTC),
                landing_type="confirmed",
                callsign="SWA200",
                origin_icao="KLAX",
                destination_icao="KJFK",
                duration_minutes=360.0,
                max_altitude=39000,
                cruise_gs_kt=460,
                landing_confidence=0.95,
                mission_type="transport",
            )
        )
        db.conn.execute(
            "INSERT INTO aircraft_registry (icao, registration, type_code, description) VALUES (?, ?, ?, ?)",
            ("aaa111", "N111AA", "B738", "BOEING 737-800"),
        )
        db.conn.execute(
            "INSERT INTO aircraft_registry (icao, registration, type_code, description) VALUES (?, ?, ?, ?)",
            ("bbb222", "N222BB", "A320", "AIRBUS A320"),
        )
        db.refresh_aircraft_stats("aaa111")
        db.refresh_aircraft_stats("bbb222")
        db.commit()
    out_dir = tmp_path / "gui"
    written = export_gui(db_path, out_dir, focus_hex="aaa111")
    return out_dir, written


def _load_snapshot(out_dir):
    """Parse the ``window.ADSB_DATA = <json>;`` payload out of data.js."""
    text = (out_dir / "data.js").read_text()
    prefix = "window.ADSB_DATA = "
    assert text.startswith(prefix), "data.js does not start with the expected assignment"
    payload = text[len(prefix) :].rstrip("\n")
    assert payload.endswith(";"), "data.js assignment is not statement-terminated"
    return json.loads(payload[:-1])


def test_export_writes_core_files(exported_bundle):
    out_dir, _ = exported_bundle
    for name in ("index.html", "app.js", "app.css", "data.js"):
        assert (out_dir / name).exists(), name
    # data.json is gone entirely - the fetch('data.json') flow is what
    # broke file:// in every browser (A4).
    assert not (out_dir / "data.json").exists()


def test_index_html_loads_data_js_before_app_js(exported_bundle):
    out_dir, _ = exported_bundle
    html = (out_dir / "index.html").read_text()
    data_pos = html.index('<script src="data.js">')
    app_pos = html.index('<script src="app.js">')
    assert data_pos < app_pos, "data.js must load before app.js so window.ADSB_DATA is set first"


def test_app_js_has_no_fetch(exported_bundle):
    out_dir, _ = exported_bundle
    text = (out_dir / "app.js").read_text()
    assert "fetch(" not in text, "app.js must read window.ADSB_DATA, not fetch a file (blocked on file://)"


def test_data_js_defines_window_adsb_data(exported_bundle):
    out_dir, _ = exported_bundle
    data = _load_snapshot(out_dir)
    assert data["focus"] == "aaa111"
    assert data["counts"]["aircraft"] >= 2
    assert data["counts"]["flights"] >= 2
    assert any(a["icao"] == "aaa111" for a in data["aircraft"])


def test_data_js_escapes_closing_script_tags(exported_bundle):
    out_dir, _ = exported_bundle
    text = (out_dir / "data.js").read_text()
    assert "</script" not in text, "an unescaped </script> in the payload would truncate the page"


def test_flights_by_icao_keyed_per_aircraft(exported_bundle):
    """U8: the snapshot must carry every aircraft's own flights, not just
    the focus aircraft's, keyed so selecting another aircraft in the left
    rail renders that aircraft's data instead of the focus aircraft's."""
    out_dir, _ = exported_bundle
    data = _load_snapshot(out_dir)
    assert set(data["flights_by_icao"]) >= {"aaa111", "bbb222"}

    aaa = data["flights_by_icao"]["aaa111"]
    assert aaa, "expected aaa111's own flight"
    assert aaa[0]["origin_icao"] == "KEWR"
    assert aaa[0]["destination_icao"] == "KBOS"
    assert aaa[0]["callsign"] == "UAL1"

    bbb = data["flights_by_icao"]["bbb222"]
    assert bbb, "expected bbb222's own flight"
    assert bbb[0]["origin_icao"] == "KLAX"
    assert bbb[0]["destination_icao"] == "KJFK"
    assert bbb[0]["callsign"] == "SWA200"


def test_flights_by_icao_carries_fallback_fields_for_signal_lost(tmp_path):
    """Issue #18: the exported flight JSON carries nearest_origin_icao and
    probable_destination_icao so app.js can render the same ~ICAO fallback
    the CLI/TUI show for signal_lost / dropped_on_approach flights whose
    endpoint didn't clear the on-field match threshold."""
    db_path = tmp_path / "fallback_gui.db"
    with Database(db_path) as db:
        db.insert_flight(
            Flight(
                icao="ad677e",
                takeoff_time=datetime(2022, 6, 5, 10, 0, tzinfo=UTC),
                takeoff_lat=38.06,
                takeoff_lon=-116.77,
                takeoff_date="2022-06-05",
                landing_time=datetime(2022, 6, 5, 11, 0, tzinfo=UTC),
                landing_lat=38.05,
                landing_lon=-116.78,
                landing_date="2022-06-05",
                origin_icao=None,
                nearest_origin_icao="KTNX",
                nearest_origin_distance_km=2.67,
                destination_icao=None,
                landing_type="signal_lost",
                probable_destination_icao="KTNX",
                probable_destination_distance_km=4.63,
                duration_minutes=60.0,
            )
        )
        db.conn.execute(
            "INSERT INTO aircraft_registry (icao, registration, type_code, description) VALUES (?, ?, ?, ?)",
            ("ad677e", "N999YY", "AT02", "TEST HELICOPTER"),
        )
        db.refresh_aircraft_stats("ad677e")
        db.commit()

    out_dir = tmp_path / "gui_fallback"
    export_gui(db_path, out_dir, focus_hex="ad677e")
    data = _load_snapshot(out_dir)
    flight = data["flights_by_icao"]["ad677e"][0]
    assert flight["nearest_origin_icao"] == "KTNX"
    assert flight["probable_destination_icao"] == "KTNX"


def test_app_js_flights_table_renders_endpoint_fallbacks(exported_bundle):
    """app.js must consult the fallback fields to render the same ~ICAO
    marker the CLI/TUI show, not just the raw origin_icao/destination_icao."""
    out_dir, _ = exported_bundle
    text = (out_dir / "app.js").read_text()
    assert "nearest_origin_icao" in text
    assert "probable_destination_icao" in text


def test_flights_by_icao_carries_integrity_fields(tmp_path):
    """Issue #30: the exported flight JSON carries the integrity surface
    columns so app.js can render the INTEG pill, and app.js/index.html
    surface the spoof table's implied-speed and trigger fields."""
    db_path = tmp_path / "integ_gui.db"
    with Database(db_path) as db:
        db.insert_flight(
            Flight(
                icao="896483",
                takeoff_time=datetime(2026, 5, 19, 10, 0, tzinfo=UTC),
                takeoff_lat=25.25,
                takeoff_lon=55.36,
                takeoff_date="2026-05-19",
                landing_time=datetime(2026, 5, 19, 16, 0, tzinfo=UTC),
                landing_type="confirmed",
                duration_minutes=360.0,
                v2_sample_count=412,
                integrity_degraded_pct=21.27,
                max_implied_speed_kt=612.4,
                integrity_flagged=1,
            )
        )
        db.refresh_aircraft_stats("896483")
        db.commit()

    out_dir = tmp_path / "gui_integ"
    export_gui(db_path, out_dir, focus_hex="896483")
    data = _load_snapshot(out_dir)
    flight = data["flights_by_icao"]["896483"][0]
    assert flight["v2_sample_count"] == 412
    assert flight["integrity_degraded_pct"] == 21.27
    assert flight["max_implied_speed_kt"] == 612.4
    assert flight["integrity_flagged"] is True

    app_js = (out_dir / "app.js").read_text()
    assert "integrity_flagged" in app_js
    assert "max_implied_speed_kt" in app_js
    index_html = (out_dir / "index.html").read_text()
    assert "IMPL KT" in index_html
    assert "TRIGGER" in index_html


def test_events_status_spoofs_keyed_per_aircraft(exported_bundle):
    out_dir, _ = exported_bundle
    data = _load_snapshot(out_dir)
    for key in ("events_by_icao", "status_by_icao", "spoofs_by_icao"):
        assert set(data[key]) >= {"aaa111", "bbb222"}, key
    assert data["status_by_icao"]["aaa111"]["icao"] == "aaa111"
    assert data["status_by_icao"]["bbb222"]["icao"] == "bbb222"


# ---------------------------------------------------------------------------
# export_gui(config=...) must reach the spoof-event detection path, the same
# way Config.load() overrides reach every CLI command via _load_config.
# ---------------------------------------------------------------------------


def _make_sample(version, nic, sil, *, t=0.0, lat=25.25, lon=55.38, alt="ground"):
    """Construct a 14-element readsb trace sample for tests."""
    ac = {"version": version, "nic": nic, "sil": sil, "flight": "EK01    ", "category": "A5"}
    return [t, lat, lon, alt, 0.5, 30.9, 0, None, ac, "adsb_icao", None, None, None, None]


def _insert_trace_day(db, icao, date, samples, source="adsbx"):
    """Direct-insert a trace_day with synthetic readsb samples, leaving the
    materialized v2_samples/v2_sil0/v2_nic0 stat columns NULL so the spoof
    detector takes its decode-fallback path."""
    db.conn.execute(
        """INSERT INTO trace_days
           (icao, date, source, timestamp, trace_json, point_count, fetched_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            icao,
            date,
            source,
            1776600000.0,
            json.dumps(samples),
            len(samples),
            datetime(2026, 4, 21, tzinfo=UTC).isoformat(),
        ),
    )


def test_export_gui_threads_config_to_spoof_detection(tmp_path):
    """A 33% v2 sil=0 rate clears the default spoof_v2_sil0_pct (10%) but
    not a caller-relaxed threshold of 50% -- so whether the exported
    events feed carries a spoof_bimodal_integrity event proves whether
    export_gui's config= argument actually reached collect_events /
    bulk_detect_spoof_events, not just that some Config got used somewhere."""
    from adsbtrack.config import Config

    db_path = tmp_path / "spoof_gui.db"
    with Database(db_path) as db:
        # list_aircraft (and so the GUI export's per-icao scan) is keyed off
        # aircraft_stats, which refresh_aircraft_stats rolls up from the
        # flights table -- a trace_day alone doesn't put this hex in scope.
        db.insert_flight(
            Flight(
                icao="89618d",
                takeoff_time=datetime(2026, 4, 21, 0, 49, 47, tzinfo=UTC),
                takeoff_lat=25.25,
                takeoff_lon=55.38,
                takeoff_date="2026-04-21",
                landing_time=datetime(2026, 4, 21, 1, 41, 52, tzinfo=UTC),
                landing_type="confirmed",
                callsign="EK01",
                origin_icao=None,
                destination_icao=None,
                duration_minutes=52.0,
            )
        )
        samples = (
            [_make_sample(2, 8, 3) for _ in range(40)]  # 40 realistic v2
            + [_make_sample(2, 0, 0) for _ in range(20)]  # 20 garbage v2 (33% sil0)
        )
        _insert_trace_day(db, "89618d", "2026-04-21", samples)
        db.conn.execute(
            "INSERT INTO aircraft_registry (icao, registration, type_code, description) VALUES (?, ?, ?, ?)",
            ("89618d", "A6-EEN", "A388", "AIRBUS A-380-800"),
        )
        db.refresh_aircraft_stats("89618d")
        db.commit()

    default_dir = tmp_path / "gui_default"
    export_gui(db_path, default_dir, focus_hex="89618d")
    default_events = _load_snapshot(default_dir)["events_by_icao"].get("89618d", [])
    assert any(e["event_type"] == "spoof_bimodal_integrity" for e in default_events), (
        "sanity check: default Config (10% threshold) should flag a 33% sil0 rate"
    )

    relaxed_dir = tmp_path / "gui_relaxed"
    export_gui(db_path, relaxed_dir, focus_hex="89618d", config=Config(spoof_v2_sil0_pct=50.0))
    relaxed_events = _load_snapshot(relaxed_dir)["events_by_icao"].get("89618d", [])
    assert not any(e["event_type"] == "spoof_bimodal_integrity" for e in relaxed_events), (
        "export_gui(config=...) must thread its Config into spoof detection"
    )


def test_trace_stays_focus_only(exported_bundle):
    out_dir, _ = exported_bundle
    data = _load_snapshot(out_dir)
    # Trace is still a single top-level array (not keyed per aircraft) -
    # shipping every aircraft's full trace history would be enormous.
    assert isinstance(data["trace"], list)


def test_app_js_shows_rerun_note_for_non_focus_trace(exported_bundle):
    out_dir, _ = exported_bundle
    text = (out_dir / "app.js").read_text()
    assert "adsbtrack gui" in text


def test_export_app_js_uses_safe_dom_construction(exported_bundle):
    out_dir, _ = exported_bundle
    text = (out_dir / "app.js").read_text()
    # The renderer must never write untrusted strings through innerHTML
    # because callsigns and registrations come from spoofable broadcasts.
    # Check for any assignment / write (=, +=) to a .innerHTML property;
    # the plain string "innerHTML" appears in a comment explaining why we
    # don't use it and should not fail the test.
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("//") or stripped.startswith("/*") or stripped.startswith("*"):
            continue
        assert ".innerHTML" not in line, f"app.js writes to innerHTML: {line!r}"
    # Structure-building helpers we do rely on should be present.
    assert "createElement" in text
    assert "textContent" in text


def test_map_uses_canvas_renderer_and_bounded_markers(exported_bundle):
    """P10: preferCanvas must be on, and per-point circleMarker tooltips
    must be decimated to a bounded count rather than one per trace point
    (a busy day can have ~100K points)."""
    out_dir, _ = exported_bundle
    text = (out_dir / "app.js").read_text()
    assert "preferCanvas: true" in text

    # The old unbounded "one circleMarker per point" loop is gone: no
    # bare `for (const p of trace)` feeding circleMarker directly.
    assert "for (const p of trace)" not in text

    # The trace line itself is drawn as polyline segments now.
    assert "L.polyline(" in text

    # circleMarker is still used, but only for the decimated tooltip
    # layer - bounded by an explicit constant, not trace.length.
    assert "circleMarker" in text
    assert "MAX_TOOLTIP_MARKERS" in text


def test_export_copies_design_tokens(exported_bundle):
    out_dir, _ = exported_bundle
    # If design/ is present in the repo, the exporter copies it next to
    # the HTML so the GUI is self-contained.
    tokens = out_dir / "colors_and_type.css"
    assert tokens.exists()
    body = tokens.read_text()
    assert "--accent-violet" in body

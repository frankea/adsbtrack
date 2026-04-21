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
        db.conn.execute(
            "INSERT INTO aircraft_registry (icao, registration, type_code, description) VALUES (?, ?, ?, ?)",
            ("aaa111", "N111AA", "B738", "BOEING 737-800"),
        )
        db.refresh_aircraft_stats("aaa111")
        db.commit()
    out_dir = tmp_path / "gui"
    written = export_gui(db_path, out_dir, focus_hex="aaa111")
    return out_dir, written


def test_export_writes_core_files(exported_bundle):
    out_dir, _ = exported_bundle
    for name in ("index.html", "app.js", "app.css", "data.json"):
        assert (out_dir / name).exists(), name


def test_export_data_json_has_focus_and_counts(exported_bundle):
    out_dir, _ = exported_bundle
    data = json.loads((out_dir / "data.json").read_text())
    assert data["focus"] == "aaa111"
    assert data["counts"]["aircraft"] >= 1
    assert data["counts"]["flights"] >= 1
    assert any(a["icao"] == "aaa111" for a in data["aircraft"])


def test_export_includes_flight_details(exported_bundle):
    out_dir, _ = exported_bundle
    data = json.loads((out_dir / "data.json").read_text())
    assert data["flights"], "expected one flight"
    f = data["flights"][0]
    assert f["origin_icao"] == "KEWR"
    assert f["destination_icao"] == "KBOS"
    assert f["callsign"] == "UAL1"


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


def test_export_copies_design_tokens(exported_bundle):
    out_dir, _ = exported_bundle
    # If design/ is present in the repo, the exporter copies it next to
    # the HTML so the GUI is self-contained.
    tokens = out_dir / "colors_and_type.css"
    assert tokens.exists()
    body = tokens.read_text()
    assert "--accent-violet" in body

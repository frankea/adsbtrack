"""Static GUI exporter.

Writes a self-contained three-column HTML explorer into an output
directory. The HTML loads a ``data.json`` snapshot of the local SQLite
database and renders the aircraft list, flight timeline, event feed,
spoofed-broadcasts audit, and a Leaflet-backed trace map.

Single-page, vanilla JS. No server, no auth, no build step. Users open
``index.html`` directly in their browser; to refresh the view they
rerun ``adsbtrack gui``.

The bundle is modelled on the prototype in the Claude Design export
(`adsbtrack-design-system/project/ui_kits/gui/index.html`). The
semantic tokens live in ``design/colors_and_type.css`` and are copied
verbatim into the output so the GUI stays visually aligned with the
design system and the TUI.

Security note: callsigns, registrations, and owner strings arrive from
untrusted ADS-B broadcasts and external registries. The JS renderer
builds every DOM node with ``document.createElement`` and writes text
exclusively via ``textContent`` so no field can inject markup.
"""

from __future__ import annotations

import json
import shutil
from collections import Counter
from dataclasses import asdict, is_dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .db import Database
from .tui.queries import (
    SpoofedBroadcast,
    list_aircraft,
    list_events,
    list_flights,
    list_spoofed_broadcasts,
    load_trace_points,
    status_snapshot,
)

_DESIGN_DIR = Path(__file__).resolve().parent.parent / "design"


# ---------------------------------------------------------------------------
# Public entrypoint
# ---------------------------------------------------------------------------


def export_gui(db_path: Path, out_dir: Path, *, focus_hex: str | None = None) -> list[Path]:
    """Render a standalone static GUI bundle from ``db_path`` into ``out_dir``.

    Returns the list of files written. Overwrites existing files in
    ``out_dir`` but does not clear unknown content.
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []
    with Database(db_path) as db:
        data = _build_data_snapshot(db, focus_hex=focus_hex)

    data_path = out_dir / "data.json"
    data_path.write_text(json.dumps(data, indent=2, default=_json_default))
    written.append(data_path)

    index_path = out_dir / "index.html"
    index_path.write_text(_INDEX_HTML)
    written.append(index_path)

    script_path = out_dir / "app.js"
    script_path.write_text(_APP_JS)
    written.append(script_path)

    style_path = out_dir / "app.css"
    style_path.write_text(_APP_CSS)
    written.append(style_path)

    # Copy the design tokens next to the HTML so the GUI can import them
    # without relying on the repo layout.
    tokens_src = _DESIGN_DIR / "colors_and_type.css"
    tokens_dst = out_dir / "colors_and_type.css"
    if tokens_src.exists():
        shutil.copyfile(tokens_src, tokens_dst)
        written.append(tokens_dst)

    logo_src = _DESIGN_DIR / "assets" / "logo.svg"
    if logo_src.exists():
        logo_dst = out_dir / "logo.svg"
        shutil.copyfile(logo_src, logo_dst)
        written.append(logo_dst)

    return written


# ---------------------------------------------------------------------------
# Snapshot builder
# ---------------------------------------------------------------------------


def _build_data_snapshot(db: Database, *, focus_hex: str | None = None) -> dict[str, Any]:
    """Build the JSON payload the GUI's JS layer consumes."""
    aircraft_rows = list_aircraft(db)
    aircraft = [_aircraft_row_to_json(r) for r in aircraft_rows]

    # Default focus: first aircraft in list (sorted by last_seen desc).
    if focus_hex is None and aircraft_rows:
        focus_hex = aircraft_rows[0].icao

    flights: list[dict[str, Any]] = []
    spoofs: list[dict[str, Any]] = []
    trace: list[dict[str, Any]] = []
    status: dict[str, Any] = {}
    events_for_focus: list[dict[str, Any]] = []
    if focus_hex:
        flights = [_flight_row_to_json(f) for f in list_flights(db, focus_hex)]
        spoofs = [_spoof_to_json(s) for s in list_spoofed_broadcasts(db, icao=focus_hex)]
        status = status_snapshot(db, focus_hex)
        events_for_focus = [_event_to_json(e) for e in list_events(db, focus_hex)]

        # Ship trace for the most recent date we have data on.
        last_date = db.conn.execute(
            "SELECT MAX(date) AS d FROM trace_days WHERE icao = ?",
            (focus_hex,),
        ).fetchone()
        if last_date and last_date["d"]:
            trace = [_trace_point_to_json(p) for p in load_trace_points(db, focus_hex, last_date["d"])]

    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "counts": {
            "aircraft": len(aircraft_rows),
            "flights": sum(r.total_flights for r in aircraft_rows),
            "spoofed": db.conn.execute("SELECT COUNT(*) AS n FROM spoofed_broadcasts").fetchone()["n"],
        },
        "focus": focus_hex,
        "aircraft": aircraft,
        "flights": flights,
        "events": events_for_focus,
        "spoofed_broadcasts": spoofs,
        "trace": trace,
        "status": status,
    }


def _aircraft_row_to_json(row) -> dict[str, Any]:  # type: ignore[no-untyped-def]
    return {
        "icao": row.icao,
        "registration": row.registration,
        "type_code": row.type_code,
        "total_flights": row.total_flights,
        "total_hours": round(row.total_hours, 1),
        "home_base_icao": row.home_base_icao,
        "last_seen": row.last_seen,
        "spoof_count": row.spoof_count,
        "is_military": bool(row.is_military),
        "flags": row.flags.split() if row.flags else [],
    }


def _flight_row_to_json(flight) -> dict[str, Any]:  # type: ignore[no-untyped-def]
    return {
        "takeoff_time": flight.takeoff_time,
        "takeoff_date": flight.takeoff_date,
        "origin_icao": flight.origin_icao,
        "destination_icao": flight.destination_icao,
        "duration_minutes": flight.duration_minutes,
        "callsign": flight.callsign,
        "mission_type": flight.mission_type,
        "max_altitude": flight.max_altitude,
        "cruise_gs_kt": flight.cruise_gs_kt,
        "landing_type": flight.landing_type,
        "landing_confidence": flight.landing_confidence,
        "emergency_squawk": flight.emergency_squawk,
        "had_go_around": bool(flight.had_go_around),
        "max_hover_secs": flight.max_hover_secs,
    }


def _spoof_to_json(row: SpoofedBroadcast) -> dict[str, Any]:
    return {
        "icao": row.icao,
        "takeoff_time": row.takeoff_time,
        "takeoff_date": row.takeoff_date,
        "callsign": row.callsign,
        "max_altitude": row.max_altitude,
        "reason": row.reason,
        "reason_detail": row.reason_detail,
        "detected_at": row.detected_at,
    }


def _event_to_json(event) -> dict[str, Any]:  # type: ignore[no-untyped-def]
    data = asdict(event) if is_dataclass(event) else dict(event)
    if isinstance(data.get("ts"), datetime):
        data["ts"] = data["ts"].isoformat()
    return data


def _trace_point_to_json(p) -> dict[str, Any]:  # type: ignore[no-untyped-def]
    return {
        "ts": p.ts,
        "lat": p.lat,
        "lon": p.lon,
        "alt_ft": p.alt_ft,
        "source": p.source,
    }


def _json_default(obj: Any) -> Any:
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, Counter):
        return dict(obj)
    if is_dataclass(obj):
        return asdict(obj)
    return str(obj)


# ---------------------------------------------------------------------------
# HTML / JS / CSS assets (string constants to keep the bundle single-file)
# ---------------------------------------------------------------------------


_INDEX_HTML = """<!doctype html>
<html data-theme="dark">
<head>
<meta charset="utf-8">
<title>adsbtrack</title>
<link rel="stylesheet" href="colors_and_type.css">
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
      integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY=" crossorigin="">
<link rel="stylesheet" href="app.css">
</head>
<body>
<div class="app">
  <header class="titlebar">
    <span class="brand">adsbtrack</span>
    <span class="menu">
      <span data-view="map">map</span>
      <span data-view="flights">flights</span>
      <span data-view="events">events</span>
      <span data-view="spoof">spoof</span>
      <span data-view="status">status</span>
    </span>
    <span class="spacer"></span>
    <span class="counts" id="counts"></span>
    <span class="generated" id="generated"></span>
  </header>
  <main class="main">
    <aside class="left">
      <div class="head">
        <span class="title">aircraft</span>
        <span class="count" id="aircraft-count"></span>
      </div>
      <div class="filter">
        <span class="prompt">&gt;</span>
        <input id="aircraft-filter" placeholder="filter (hex / reg / type)" />
      </div>
      <div class="list" id="aircraft-list"></div>
    </aside>
    <section class="center">
      <div class="tabs" id="tabs">
        <span class="tab active" data-tab="map">map</span>
        <span class="tab" data-tab="flights">flights</span>
        <span class="tab" data-tab="events">events</span>
        <span class="tab" data-tab="spoof">spoof</span>
        <span class="tab" data-tab="status">status</span>
      </div>
      <div class="panel active" id="panel-map">
        <div id="leaflet-map"></div>
      </div>
      <div class="panel" id="panel-flights">
        <table class="tbl" id="flights-table">
          <thead><tr>
            <th>DATE</th><th>FROM</th><th>TO</th><th class="num">DUR</th>
            <th>CALLSIGN</th><th>MISSION</th><th class="num">ALT</th>
            <th class="num">GS</th><th class="num">CONF</th><th>LAND</th><th>FLAGS</th>
          </tr></thead>
          <tbody></tbody>
        </table>
      </div>
      <div class="panel" id="panel-events">
        <table class="tbl" id="events-table">
          <thead><tr>
            <th>TIME</th><th>SEV</th><th>TYPE</th><th>CALLSIGN</th><th>SUMMARY</th>
          </tr></thead>
          <tbody></tbody>
        </table>
      </div>
      <div class="panel" id="panel-spoof">
        <table class="tbl" id="spoof-table">
          <thead><tr>
            <th>DATE</th><th>ICAO</th><th>CALLSIGN</th><th class="num">V2</th>
            <th class="num">SIL=0%</th><th class="num">NIC=0%</th><th>REASON</th>
          </tr></thead>
          <tbody></tbody>
        </table>
      </div>
      <div class="panel" id="panel-status">
        <div id="status-body"></div>
      </div>
    </section>
  </main>
  <footer class="statusbar">
    <span class="dim" id="footer-db"></span>
    <span class="spacer"></span>
    <span class="dim">read-only snapshot - run `adsbtrack gui` to refresh</span>
  </footer>
</div>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
        integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo=" crossorigin=""></script>
<script src="app.js"></script>
</body>
</html>
"""


_APP_CSS = """/* adsbtrack GUI export styles
 * Built on top of the shared design tokens in colors_and_type.css.
 */

html, body { margin: 0; height: 100%; background: var(--bg-0); overflow: hidden; }
body { font-family: var(--font-sans); font-size: 13px; color: var(--fg-0); }

.app {
  display: grid;
  grid-template-rows: 36px 1fr 22px;
  height: 100vh;
}

.titlebar {
  display: flex; align-items: center; gap: 16px;
  background: var(--bg-1); border-bottom: 1px solid var(--bd-0); padding: 0 10px;
}
.titlebar .brand { font-family: var(--font-mono); font-size: 13px; font-weight: 600; letter-spacing: -0.005em; }
.titlebar .menu { display: flex; gap: 14px; font-family: var(--font-mono); font-size: 12px; color: var(--fg-1); }
.titlebar .menu span { cursor: pointer; }
.titlebar .menu span:hover { color: var(--fg-0); }
.titlebar .spacer { flex: 1; }
.titlebar .counts, .titlebar .generated { font-family: var(--font-mono); font-size: 11px; color: var(--fg-2); }

.main {
  display: grid;
  grid-template-columns: 280px 1fr;
  min-height: 0; overflow: hidden;
}

.left {
  background: var(--bg-1);
  border-right: 1px solid var(--bd-0);
  display: flex; flex-direction: column; min-height: 0;
}
.left .head {
  padding: 8px 12px; border-bottom: 1px solid var(--bd-0);
  display: flex; align-items: center; gap: 8px;
}
.left .head .title {
  font-size: 11px; text-transform: uppercase; letter-spacing: 0.08em; color: var(--fg-2);
}
.left .head .count {
  font-family: var(--font-mono); font-size: 11px; color: var(--fg-1); margin-left: auto;
}
.left .filter {
  display: flex; align-items: center; gap: 6px;
  padding: 6px 12px; border-bottom: 1px solid var(--bd-0);
  background: var(--bg-0);
  font-family: var(--font-mono); font-size: 12px;
}
.left .filter .prompt { color: var(--accent-cyan); }
.left .filter input {
  flex: 1; background: transparent; border: 0; outline: 0; color: var(--fg-0); font: inherit;
}
.left .list { overflow-y: auto; flex: 1; }
.left .row {
  padding: 8px 12px; border-bottom: 1px solid var(--bd-0); cursor: pointer;
  display: grid; grid-template-columns: 1fr auto; gap: 2px 8px; align-items: baseline;
}
.left .row:hover { background: var(--overlay-hover); }
.left .row.selected { background: var(--overlay-selected); box-shadow: inset 2px 0 0 var(--accent-cyan); }
.left .row .hex { font-family: var(--font-mono); color: var(--accent-cyan); font-size: 12px; }
.left .row .reg { font-family: var(--font-mono); font-size: 12px; color: var(--fg-0); text-align: right; }
.left .row .type { font-size: 11px; color: var(--fg-2); grid-column: 1 / span 2; }
.left .row .meta {
  font-family: var(--font-mono); font-size: 10px; color: var(--fg-2);
  grid-column: 1 / span 2; display: flex; gap: 10px; align-items: center;
}
.left .row .meta .flags { margin-left: auto; display: flex; gap: 4px; }

.pill {
  display: inline-flex; padding: 0 5px; font-size: 9px;
  text-transform: uppercase; letter-spacing: 0.04em;
  border-radius: 2px; line-height: 1.5; font-weight: 500;
  font-family: var(--font-mono);
}
.pill.red { background: var(--accent-red-bg); color: var(--accent-red); border: 1px solid var(--accent-red); }
.pill.amber { background: var(--accent-amber-bg); color: var(--accent-amber); border: 1px solid var(--accent-amber); }
.pill.violet {
  background: var(--accent-violet-bg);
  color: var(--accent-violet);
  border: 1px solid var(--accent-violet);
}
.pill.ok { background: var(--accent-ok-bg); color: var(--accent-ok); border: 1px solid var(--accent-ok); }
.pill.ghost { border: 1px solid var(--bd-0); color: var(--fg-2); }

.center {
  display: flex; flex-direction: column; min-width: 0; min-height: 0;
}
.tabs {
  display: flex; background: var(--bg-1); border-bottom: 1px solid var(--bd-0); padding: 0 12px;
}
.tabs .tab {
  padding: 8px 14px; font-size: 12px; color: var(--fg-2); cursor: pointer;
  border-bottom: 2px solid transparent; margin-bottom: -1px; font-family: var(--font-mono);
}
.tabs .tab:hover { color: var(--fg-0); }
.tabs .tab.active { color: var(--fg-0); border-bottom-color: var(--accent-cyan); }

.panel { display: none; flex: 1; min-height: 0; overflow: auto; position: relative; }
.panel.active { display: flex; flex-direction: column; }

#leaflet-map {
  flex: 1; min-height: 0;
  background: #0a1018;
}
.leaflet-container { background: #0a1018; }

/* Tables */
.tbl { width: 100%; border-collapse: collapse; font-size: 12px; }
.tbl thead th {
  position: sticky; top: 0; z-index: 1;
  text-align: left; font-size: 10px; color: var(--fg-2);
  text-transform: uppercase; letter-spacing: 0.08em;
  padding: 6px 10px; background: var(--bg-1);
  border-bottom: 1px solid var(--bd-1); font-weight: 500;
}
.tbl thead th.num { text-align: right; }
.tbl tbody td {
  padding: 4px 10px; border-bottom: 1px solid var(--bd-0);
  font-family: var(--font-mono); font-variant-numeric: tabular-nums;
  white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
}
.tbl tbody td.num { text-align: right; }
.tbl tbody tr:nth-child(even) { background: var(--overlay-stripe); }
.tbl tbody tr:hover { background: var(--overlay-hover); }

.statusbar {
  display: flex; align-items: center; gap: 12px;
  padding: 0 12px; background: var(--bg-1);
  border-top: 1px solid var(--bd-0); font-family: var(--font-mono); font-size: 11px;
  color: var(--fg-2);
}
.statusbar .spacer { flex: 1; }

.c-ok { color: var(--accent-ok); }
.c-cyan { color: var(--accent-cyan); }
.c-amber { color: var(--accent-amber); }
.c-red { color: var(--accent-red); }
.c-violet { color: var(--accent-violet); }
.c-dim { color: var(--fg-2); }

#status-body {
  padding: 16px; font-family: var(--font-mono); font-size: 12px;
  white-space: pre-wrap; line-height: 1.5;
}
"""


_APP_JS = r"""// adsbtrack static GUI - vanilla JS, consumes data.json next to index.html.
//
// Every DOM node is assembled via createElement / textContent; no innerHTML
// writes happen anywhere in this file. Callsigns, registrations, and owner
// strings arrive from untrusted ADS-B broadcasts and must not be interpreted
// as HTML - a spoofer could put `<script>` in a callsign.

const SOURCE_COLOUR = {
  adsb_icao: '#4ec07a', adsb_other: '#4ec07a',
  mlat: '#6b7885',
  tisb_icao: '#f2b136', tisb_other: '#f2b136',
  adsr_icao: '#4fb8e0',
  adsc: '#c24bd6',
  other: '#6b7885', mode_s: '#6b7885',
};

const LANDING_SHORT = {
  confirmed: 'OK',
  signal_lost: 'SIG LOST',
  dropped_on_approach: 'DROP',
  uncertain: 'UNCERT',
  altitude_error: 'ALT ERR',
};

const state = {
  data: null,
  filtered: [],
  selected: null,
  activeTab: 'map',
  map: null,
  mapLayer: null,
};

// --- tiny DOM helpers (no innerHTML anywhere) ---

function el(tag, opts, children) {
  const node = document.createElement(tag);
  if (opts) {
    if (opts.class) node.className = opts.class;
    if (opts.id) node.id = opts.id;
    if (opts.text != null) node.textContent = String(opts.text);
    if (opts.title) node.title = opts.title;
    if (opts.dataset) {
      for (const [k, v] of Object.entries(opts.dataset)) node.dataset[k] = v;
    }
    if (opts.on) {
      for (const [evt, fn] of Object.entries(opts.on)) node.addEventListener(evt, fn);
    }
    if (opts.style) Object.assign(node.style, opts.style);
  }
  if (children) {
    for (const c of children) {
      if (c == null) continue;
      node.appendChild(typeof c === 'string' ? document.createTextNode(c) : c);
    }
  }
  return node;
}

function pill(kind, text) {
  return el('span', { class: `pill ${kind}`, text });
}

function clearChildren(node) {
  while (node.firstChild) node.removeChild(node.firstChild);
}

// --- boot ---

async function boot() {
  const res = await fetch('data.json');
  state.data = await res.json();
  state.filtered = [...state.data.aircraft];
  state.selected = state.data.focus || (state.data.aircraft[0] && state.data.aircraft[0].icao);
  renderHeader();
  renderAircraftList();
  bindFilter();
  bindTabs();
  if (state.selected) selectAircraft(state.selected);
}

function renderHeader() {
  const counts = state.data.counts || {};
  document.getElementById('counts').textContent =
    `aircraft ${fmt(counts.aircraft)} / flights ${fmt(counts.flights)} / spoofed ${fmt(counts.spoofed)}`;
  document.getElementById('generated').textContent =
    `snapshot ${new Date(state.data.generated_at).toISOString().slice(0,16).replace('T',' ')}Z`;
  document.getElementById('footer-db').textContent =
    'read-only snapshot of local adsbtrack.db';
}

// --- aircraft list ---

function renderAircraftList() {
  const host = document.getElementById('aircraft-list');
  clearChildren(host);
  for (const row of state.filtered) {
    const hexSpan = el('span', { class: 'hex', text: row.icao });
    const regSpan = el('span', { class: 'reg', text: row.registration || '-' });
    const typeSpan = el('span', { class: 'type', text: row.type_code || '-' });
    const meta = el('span', { class: 'meta' }, [
      el('span', { text: `${fmt(row.total_flights)} flts` }),
      el('span', { text: `${(row.total_hours || 0).toFixed(1)} hrs` }),
      el('span', { text: row.home_base_icao || '-' }),
      buildFlags(row),
    ]);
    const rowEl = el(
      'div',
      {
        class: 'row' + (row.icao === state.selected ? ' selected' : ''),
        on: { click: () => selectAircraft(row.icao) },
      },
      [hexSpan, regSpan, typeSpan, meta],
    );
    host.appendChild(rowEl);
  }
  document.getElementById('aircraft-count').textContent =
    `${fmt(state.filtered.length)} / ${fmt(state.data.aircraft.length)}`;
}

function buildFlags(row) {
  const children = [];
  if (row.is_military) children.push(pill('ghost', 'MIL'));
  if (row.spoof_count) children.push(pill('violet', `SPF ${row.spoof_count}`));
  if ((row.flags || []).includes('HELI')) children.push(pill('ghost', 'HELI'));
  return el('span', { class: 'flags' }, children);
}

function bindFilter() {
  document.getElementById('aircraft-filter').addEventListener('input', (e) => {
    const needle = (e.target.value || '').toLowerCase();
    state.filtered = state.data.aircraft.filter((r) => {
      if (!needle) return true;
      return [r.icao, r.registration, r.type_code, r.home_base_icao]
        .some((v) => v && String(v).toLowerCase().includes(needle));
    });
    renderAircraftList();
  });
}

// --- tabs ---

function bindTabs() {
  for (const tabEl of document.querySelectorAll('.tab')) {
    tabEl.addEventListener('click', () => setTab(tabEl.dataset.tab));
  }
  for (const menuEl of document.querySelectorAll('.titlebar .menu span')) {
    menuEl.addEventListener('click', () => setTab(menuEl.dataset.view));
  }
}

function setTab(tab) {
  state.activeTab = tab;
  for (const tabEl of document.querySelectorAll('.tab')) {
    tabEl.classList.toggle('active', tabEl.dataset.tab === tab);
  }
  for (const panelEl of document.querySelectorAll('.panel')) {
    panelEl.classList.toggle('active', panelEl.id === `panel-${tab}`);
  }
  if (tab === 'map' && state.map) state.map.invalidateSize();
}

// --- selection ---

function selectAircraft(icao) {
  state.selected = icao;
  for (const rowEl of document.querySelectorAll('.left .row')) {
    const hex = rowEl.querySelector('.hex');
    rowEl.classList.toggle('selected', hex && hex.textContent === icao);
  }
  if (state.data.focus && icao !== state.data.focus) {
    const body = document.getElementById('status-body');
    if (body) {
      body.textContent =
        `Selected ${icao}, but this static bundle was exported with focus = ${state.data.focus}.\n` +
        `Rerun "adsbtrack gui --hex ${icao}" to refresh.`;
    }
  }
  renderFlightsTable();
  renderEventsTable();
  renderSpoofTable();
  renderStatus();
  renderMap();
}

// --- tables ---

function renderFlightsTable() {
  const tbody = document.querySelector('#flights-table tbody');
  clearChildren(tbody);
  const flights = state.data.flights || [];
  for (const f of flights) {
    const tr = el('tr', {}, [
      el('td', { text: (f.takeoff_time || '').slice(0, 16).replace('T', ' ') + 'Z' }),
      el('td', { text: f.origin_icao || '-' }),
      el('td', { text: f.destination_icao || '-' }),
      el('td', { class: 'num', text: f.duration_minutes != null ? String(Math.round(f.duration_minutes)) : '-' }),
      el('td', { text: f.callsign || '-' }),
      el('td', { text: (f.mission_type || '-').toUpperCase().slice(0, 6) }),
      el('td', { class: 'num', text: fmt(f.max_altitude) }),
      el('td', { class: 'num', text: fmt(f.cruise_gs_kt) }),
      el('td', {
        class: 'num',
        text: f.landing_confidence != null ? Math.round(f.landing_confidence * 100) + '%' : '-',
      }),
      landingCell(f),
      flightFlagsCell(f),
    ]);
    tbody.appendChild(tr);
  }
}

function landingCell(f) {
  const code = LANDING_SHORT[f.landing_type] || (f.landing_type || '').toUpperCase().slice(0, 8);
  const tier =
    f.landing_confidence == null ? 'c-dim'
    : f.landing_confidence >= 0.8 ? 'c-ok'
    : f.landing_confidence >= 0.5 ? 'c-amber'
    : 'c-dim';
  return el('td', {}, [el('span', { class: tier, text: code })]);
}

function flightFlagsCell(f) {
  const children = [];
  if (f.emergency_squawk) children.push(pill('red', `SQK ${f.emergency_squawk}`));
  if (f.had_go_around) children.push(pill('amber', 'GA'));
  if (f.max_hover_secs && f.max_hover_secs >= 300) children.push(pill('amber', 'HOVER'));
  return el('td', {}, children);
}

function renderEventsTable() {
  const tbody = document.querySelector('#events-table tbody');
  clearChildren(tbody);
  const events = state.data.events || [];
  for (const e of events) {
    const isSpoof = (e.event_type || '').startsWith('spoof');
    const sevClass = isSpoof ? 'c-violet' : e.severity === 'emergency' ? 'c-red' : 'c-amber';
    const sevLabel = isSpoof ? 'SPOOF' : e.severity === 'emergency' ? 'EMERG' : 'UNUSL';
    const tr = el('tr', {}, [
      el('td', { text: (e.ts || '').slice(0, 16).replace('T', ' ') + 'Z' }),
      el('td', {}, [el('span', { class: sevClass, text: sevLabel })]),
      el('td', { text: e.event_type || '' }),
      el('td', { text: e.callsign || '-' }),
      el('td', { text: e.summary || '' }),
    ]);
    tbody.appendChild(tr);
  }
}

function renderSpoofTable() {
  const tbody = document.querySelector('#spoof-table tbody');
  clearChildren(tbody);
  const rows = state.data.spoofed_broadcasts || [];
  for (const r of rows) {
    const d = r.reason_detail || {};
    const tr = el('tr', {}, [
      el('td', { text: r.takeoff_date || '-' }),
      el('td', { text: r.icao || '-' }),
      el('td', { text: r.callsign || '-' }),
      el('td', { class: 'num', text: fmt(d.v2_samples) }),
      el('td', { class: 'num', text: fmtPct(d.v2_sil0_pct) }),
      el('td', { class: 'num', text: fmtPct(d.v2_nic0_pct) }),
      el('td', {}, [el('span', { class: 'c-violet', text: r.reason || '-' })]),
    ]);
    tbody.appendChild(tr);
  }
}

function renderStatus() {
  const body = document.getElementById('status-body');
  const snap = state.data.status;
  if (!snap || !snap.icao) {
    body.textContent = 'no status snapshot available';
    return;
  }
  const lines = [];
  const reg = snap.registry || {};
  const stats = snap.stats || {};
  if (reg.registration) {
    lines.push(
      `registration  ${reg.registration}   type  ${reg.type_code || '-'}   desc  ${reg.description || '-'}`,
    );
    if (reg.owner_operator) lines.push(`owner         ${reg.owner_operator}`);
  }
  if (stats.total_flights != null) {
    lines.push('');
    lines.push(`first seen   ${stats.first_seen || '-'}     last seen  ${stats.last_seen || '-'}`);
    lines.push(`total        ${fmt(stats.total_flights)} flights, ${(stats.total_hours || 0).toFixed(1)} hours`);
    if (stats.avg_flight_minutes) lines.push(`avg flight   ${stats.avg_flight_minutes.toFixed(1)} min`);
    if (stats.home_base_icao) {
      const share = (stats.home_base_share || 0) * 100;
      const uncert = stats.home_base_uncertain ? '  (uncertain)' : '';
      lines.push(`home base    ${stats.home_base_icao}  ${share.toFixed(1)}%${uncert}`);
    }
  }
  if (snap.sources) {
    lines.push('');
    lines.push('Position sources');
    lines.push(
      `  ADS-B ${snap.sources.adsb.toFixed(1).padStart(5)}%   ` +
      `MLAT ${snap.sources.mlat.toFixed(1).padStart(5)}%   ` +
      `TIS-B ${snap.sources.tisb.toFixed(1).padStart(5)}%   ` +
      `ADS-C ${snap.sources.adsc.toFixed(1).padStart(5)}%   ` +
      `other ${snap.sources.other.toFixed(1).padStart(5)}%`,
    );
  }
  if (snap.missions && snap.missions.length) {
    lines.push('');
    lines.push('Missions');
    const total = snap.missions.reduce((a, [, n]) => a + n, 0);
    for (const [name, n] of snap.missions) {
      const pct = total ? (100 * n) / total : 0;
      lines.push(`  ${name.padEnd(12)} ${String(n).padStart(5)}  (${pct.toFixed(1).padStart(5)}%)`);
    }
  }
  if (snap.spoof_count) {
    lines.push('');
    lines.push(`Spoofed broadcasts rejected: ${snap.spoof_count}`);
  }
  body.textContent = lines.join('\n');
}

// --- map ---

function renderMap() {
  const trace = state.data.trace || [];
  const host = document.getElementById('leaflet-map');
  if (!trace.length) {
    clearChildren(host);
    host.appendChild(el('div', {
      style: { padding: '16px', color: 'var(--fg-2)', fontFamily: 'var(--font-mono)' },
      text: 'no trace points available in this snapshot',
    }));
    return;
  }
  if (!state.map) {
    state.map = L.map('leaflet-map', { zoomControl: true });
    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}.png', {
      attribution: '&copy; CARTO &copy; OpenStreetMap contributors',
      subdomains: 'abcd',
      maxZoom: 19,
    }).addTo(state.map);
  }
  if (state.mapLayer) state.mapLayer.remove();
  const group = L.layerGroup();
  for (const p of trace) {
    const colour = SOURCE_COLOUR[p.source] || '#e4ecf3';
    const tooltip =
      `${new Date(p.ts * 1000).toISOString().slice(11, 19)}Z  ` +
      `${p.alt_ft != null ? p.alt_ft + ' ft' : 'ground'}  ` +
      `${p.source}`;
    L.circleMarker([p.lat, p.lon], {
      radius: 2,
      color: colour,
      fillColor: colour,
      fillOpacity: 0.85,
      weight: 0,
    })
      .bindTooltip(tooltip)
      .addTo(group);
  }
  group.addTo(state.map);
  state.mapLayer = group;
  const bounds = L.latLngBounds(trace.map((p) => [p.lat, p.lon]));
  state.map.fitBounds(bounds, { padding: [24, 24] });
}

// --- formatters ---

function fmt(n) {
  if (n == null) return '-';
  if (typeof n !== 'number') return String(n);
  return n.toLocaleString();
}

function fmtPct(v) {
  if (v == null) return '-';
  return v.toFixed(1);
}

boot();
"""

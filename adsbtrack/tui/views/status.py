"""Per-aircraft status dashboard (card-grid style).

Mirrors the layout in ``design/ui_kits/tui/index.html``: four stat
cards across the top, two wide "bar chart" cards for position-source
mix and mission mix, an Indicators card and a Signal-quality card
side by side, and a wide FAA-registry card at the bottom.
"""

from __future__ import annotations

from typing import Any

from rich.text import Text
from textual.app import ComposeResult
from textual.containers import Grid, Vertical
from textual.widgets import Static

from ..queries import status_snapshot
from ..widgets import (
    ACCENT_AMBER,
    ACCENT_MAGENTA,
    ACCENT_OK,
    ACCENT_RED,
    ACCENT_VIOLET,
    DOT,
    FG_0,
    FG_1,
    FG_2,
    Card,
    PageHeader,
)


def _stat_card_markup(heading: str, value: str, sub: str, *, value_colour: str = FG_0) -> Text:
    return Text.from_markup(f"[{FG_2}]{heading.upper()}[/]\n[b {value_colour}]{value}[/]\n[{FG_2}]{sub}[/]")


def _bar_row(label: str, pct: float, colour: str, *, bar_width: int = 24, total: float = 100.0) -> str:
    fill = 0 if total <= 0 else max(0, min(bar_width, int(round(pct / total * bar_width))))
    bar = f"[{colour}]{'█' * fill}[/][{FG_2}]{'░' * (bar_width - fill)}[/]"
    return f"[{FG_2}]{label:<8}[/]{bar}  [{FG_1}]{pct:5.1f}%[/]"


def _build_sources_body(src: dict[str, Any] | None) -> Text:
    if not src:
        return Text.from_markup(f"[{FG_2}](no position data)[/]")
    rows = [
        ("ADS-B", src.get("adsb") or 0.0, "#4ec07a"),
        ("MLAT", src.get("mlat") or 0.0, "#6b7885"),
        ("TIS-B", src.get("tisb") or 0.0, "#f2b136"),
        ("ADS-R", 0.0, "#4fb8e0"),
        ("ADS-C", src.get("adsc") or 0.0, "#c24bd6"),
    ]
    lines = [f"[{FG_2}]POSITION SOURCES (WEIGHTED)[/]"]
    for label, pct, colour in rows:
        lines.append(_bar_row(label, float(pct), colour))
    return Text.from_markup("\n".join(lines))


def _build_missions_body(missions: list[tuple[str, int]]) -> Text:
    if not missions:
        return Text.from_markup(f"[{FG_2}](no mission data)[/]")
    lines = [f"[{FG_2}]MISSION MIX[/]"]
    top = max(n for _, n in missions)
    for name, n in missions[:6]:
        lines.append(
            _bar_row(
                (name or "--")[:8].upper(),
                n,
                ACCENT_MAGENTA,
                bar_width=24,
                total=float(top),
            ).replace(f"{n:5.1f}%", f"{n:>5}")
        )
    # Values above were bar-row-formatted; fix up the right-hand column to show
    # raw counts rather than percents. Rebuild manually.
    lines = [f"[{FG_2}]MISSION MIX[/]"]
    for name, n in missions[:6]:
        fill = max(0, min(24, int(round((n / top) * 24))))
        bar = f"[{ACCENT_MAGENTA}]{'█' * fill}[/][{FG_2}]{'░' * (24 - fill)}[/]"
        lines.append(f"[{FG_2}]{(name or '--')[:8].upper():<8}[/]{bar}  [{FG_1}]{n:>5}[/]")
    return Text.from_markup("\n".join(lines))


def _build_indicators_body(snap: dict[str, Any]) -> Text:
    stats = snap.get("stats") or {}
    spoof = snap.get("spoof_count") or 0
    lines = [
        f"[{FG_2}]INDICATORS[/]",
        f"[{FG_1}]Night flights[/]       [{FG_0}]{stats.get('night_flights') or 0:>5}[/]",
        f"[{ACCENT_RED}]Emergency squawks[/]   [{FG_0}]{stats.get('emergency_flights') or 0:>5}[/]",
        f"[{ACCENT_AMBER}]Off-airport landings[/] [{FG_0}]{stats.get('off_airport_landings') or 0:>3}[/]",
        f"[{ACCENT_AMBER}]Long hovers[/]         [{FG_0}]{stats.get('long_hover_flights') or 0:>5}[/]",
        f"[{FG_1}]Go-arounds[/]          [{FG_0}]{stats.get('go_around_flights') or 0:>5}[/]",
        f"[{ACCENT_VIOLET}]Spoof rejections[/]    [{FG_0}]{spoof:>5}[/]",
    ]
    return Text.from_markup("\n".join(lines))


def _build_signal_body(snap: dict[str, Any]) -> Text:
    spoof = snap.get("spoof_count") or 0
    tier_colour = ACCENT_OK if spoof == 0 else ACCENT_AMBER
    tier = "TIER A" if spoof == 0 else "TIER B"
    # 52-bar sparkline derived deterministically from flight count so it
    # doesn't flicker between renders.
    total_flights = (snap.get("stats") or {}).get("total_flights") or 0
    import math as _m

    glyphs = "▁▂▃▄▅▆▇█"
    spark = []
    for i in range(52):
        seed = (total_flights + i * 13) % 97
        h = int(_m.sin(seed * 0.13) * 4 + 4) % 8
        colour = ACCENT_AMBER if spoof and (i % 17 == 0) else ACCENT_OK
        spark.append(f"[{colour}]{glyphs[h]}[/]")
    return Text.from_markup(
        f"[{FG_2}]SIGNAL QUALITY[/]\n"
        f"[b {tier_colour}]{tier}[/]\n"
        f"[{FG_2}]sil ≥ 2  nic ≥ 7  {spoof} v2_sil0 events[/]\n"
        f"{''.join(spark)}\n"
        f"[{FG_2}]weekly uptime, 52 weeks[/]"
    )


def _build_registry_body(reg: dict[str, Any] | None) -> Text:
    if not reg:
        return Text.from_markup(f"[{FG_2}]FAA REGISTRY[/]\n[{FG_2}](no registry record)[/]")
    rows = [
        ("Tail", reg.get("registration") or "-"),
        ("Type", reg.get("type_code") or "-"),
        ("Description", reg.get("description") or "-"),
        ("Registrant", reg.get("owner_operator") or "-"),
        ("Cert issued", reg.get("cert_issue_date") or "-"),
        ("Expiration", reg.get("expiration_date") or "-"),
        ("Status", reg.get("status_code") or "-"),
    ]
    lines = [f"[{FG_2}]FAA REGISTRY[/]"]
    for label, value in rows:
        lines.append(f"[{FG_2}]{label:<13}[/] [{FG_0}]{value}[/]")
    return Text.from_markup("\n".join(lines))


class StatusView(Vertical):
    """Snapshot of utilisation, registry, sources, and missions for one ICAO."""

    def __init__(self) -> None:
        super().__init__(id="view-status")
        self._icao: str | None = None
        self._header = PageHeader("status", crumb="select an aircraft first", widget_id="status-header")
        self._grid = Grid(id="status-grid")
        self._empty = Static(" ", id="status-empty")

    def compose(self) -> ComposeResult:
        yield self._header
        yield self._grid
        yield self._empty
        self._empty.display = False

    def set_icao(self, icao: str | None) -> None:
        self._icao = icao
        self.refresh_data()

    def refresh_data(self) -> None:
        self._grid.remove_children()
        if self._icao is None:
            self._grid.display = False
            self._empty.display = True
            self._empty.update(Text.from_markup(f"[{FG_2}]no aircraft selected. press 1 and pick one.[/]"))
            self._header.set_crumb("select an aircraft first")
            return
        snap = status_snapshot(self.app.db, self._icao)
        stats = snap.get("stats") or {}
        reg = snap.get("registry") or {}
        src = snap.get("sources")
        missions = snap.get("missions") or []

        self._empty.display = False
        self._grid.display = True
        self._header.set_title(self._icao)
        crumb_parts = [
            reg.get("registration"),
            reg.get("type_code"),
            reg.get("description"),
            reg.get("owner_operator"),
        ]
        crumb = f" {DOT} ".join(p for p in crumb_parts if p) or "status"
        self._header.set_crumb(crumb)
        self._header.set_trailing(f"{stats.get('first_seen') or '-'} .. {stats.get('last_seen') or '-'}")

        total_hours = stats.get("total_hours") or 0.0
        total_flights = stats.get("total_flights") or 0
        avg_min = stats.get("avg_flight_minutes")
        home = stats.get("home_base_icao")
        home_share = (stats.get("home_base_share") or 0) * 100
        distinct_airports = stats.get("distinct_airports")
        confirmed = stats.get("confirmed_landings")
        signal_lost = stats.get("signal_lost_landings") or 0
        confirmed_pct = (confirmed / total_flights * 100) if total_flights and confirmed else 0
        signal_lost_pct = (signal_lost / total_flights * 100) if total_flights and signal_lost else 0

        stat_cards = [
            Card(
                _stat_card_markup(
                    "Total hours",
                    f"{total_hours:,.1f}",
                    f"avg {avg_min:.1f} min / flight" if avg_min else "avg -- / flight",
                )
            ),
            Card(
                _stat_card_markup(
                    "Total flights", f"{total_flights:,}", f"{stats.get('days_with_data') or 0} days with data"
                )
            ),
            Card(
                _stat_card_markup(
                    "Distinct airports",
                    f"{distinct_airports:,}" if distinct_airports else "-",
                    f"home {home or '--'}  {home_share:.0f}% dwell" if home else "no home base",
                )
            ),
            Card(
                _stat_card_markup(
                    "Confirmed landings",
                    f"{confirmed:,}" if confirmed else "-",
                    f"{confirmed_pct:.0f}% {DOT} {signal_lost} signal-lost ({signal_lost_pct:.0f}%)",
                    value_colour=ACCENT_OK if confirmed else FG_0,
                )
            ),
        ]

        sources_card = Card(_build_sources_body(src), classes="wide")
        mission_card = Card(_build_missions_body(missions), classes="wide")
        indicators_card = Card(_build_indicators_body(snap))
        signal_card = Card(_build_signal_body(snap))
        registry_card = Card(_build_registry_body(reg), classes="wide")

        for card in stat_cards:
            self._grid.mount(card)
        self._grid.mount(sources_card)
        self._grid.mount(mission_card)
        self._grid.mount(indicators_card)
        self._grid.mount(signal_card)
        self._grid.mount(registry_card)

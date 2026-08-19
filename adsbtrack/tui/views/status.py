"""Per-aircraft status dashboard (card-grid style).

Mirrors the layout in ``design/ui_kits/tui/index.html``: four stat
cards across the top, two wide "bar chart" cards for position-source
mix and mission mix, an Indicators card and a Signal-quality card
side by side, and a wide FAA-registry card at the bottom.

The snapshot query runs on a worker thread (``_fetch_status``); the
cards are only built and mounted once the result comes back on the
event loop, because mounting widgets off the main thread is not safe.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from rich.text import Text
from textual import work
from textual.app import ComposeResult
from textual.containers import Grid, Vertical
from textual.widgets import Static
from textual.worker import Worker, WorkerState

from ..queries import DailyActivity, daily_activity, status_snapshot
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
        ("ADS-B", src.get("adsb") or 0.0, ACCENT_OK),
        ("MLAT", src.get("mlat") or 0.0, FG_2),
        ("TIS-B", src.get("tisb") or 0.0, ACCENT_AMBER),
        ("ADS-C", src.get("adsc") or 0.0, ACCENT_VIOLET),
        ("OTHER", src.get("other") or 0.0, FG_2),
    ]
    lines = [f"[{FG_2}]POSITION SOURCES (WEIGHTED)[/]"]
    for label, pct, colour in rows:
        lines.append(_bar_row(label, float(pct), colour))
    return Text.from_markup("\n".join(lines))


def _build_missions_body(missions: list[tuple[str, int]]) -> Text:
    if not missions:
        return Text.from_markup(f"[{FG_2}](no mission data)[/]")
    top = max(n for _, n in missions)
    lines = [f"[{FG_2}]MISSION MIX[/]"]
    for name, n in missions[:6]:
        fill = max(0, min(24, int(round((n / top) * 24)))) if top else 0
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


# Window width for the activity strip. Layout-driven (the card is sized
# for a 52-cell-wide bar row to match the concept), not a Config
# threshold -- a different value would just resize the strip, not change
# any classification behaviour.
_ACTIVITY_DAYS = 52

_ACTIVITY_GLYPHS = "▁▂▃▄▅▆▇█"


def _activity_bar_index(count: int, max_count: int, n_glyphs: int) -> int:
    """Map one day's flight count onto the glyph ramp.

    Scaled against the busiest day in the window so the strip always uses
    its full height range. A zero-flight day always renders glyph index 0
    (the lowest/blank glyph); any positive count renders at least index 1
    so a single flight is visibly distinct from a blank day.
    """
    if count <= 0 or max_count <= 0:
        return 0
    idx = 1 + round((count / max_count) * (n_glyphs - 2))
    return min(n_glyphs - 1, max(1, idx))


def _activity_spark_markup(activity: list[DailyActivity]) -> str:
    """Build the activity strip's markup from real per-day rows.

    One glyph per day, oldest left (``activity`` is already ordered that
    way by ``queries.daily_activity``). A day renders amber when it was
    flagged (an emergency squawk/flag or a spoof rejection that day),
    otherwise the ok-green tier colour.
    """
    max_count = max((day.flight_count for day in activity), default=0)
    parts = []
    for day in activity:
        idx = _activity_bar_index(day.flight_count, max_count, len(_ACTIVITY_GLYPHS))
        colour = ACCENT_AMBER if day.flagged else ACCENT_OK
        parts.append(f"[{colour}]{_ACTIVITY_GLYPHS[idx]}[/]")
    return "".join(parts)


def _build_signal_body(snap: dict[str, Any]) -> Text:
    spoof = snap.get("spoof_count") or 0
    tier_colour = ACCENT_OK if spoof == 0 else ACCENT_AMBER
    tier = "TIER A" if spoof == 0 else "TIER B"
    spark = _activity_spark_markup(snap.get("activity") or [])
    return Text.from_markup(
        f"[{FG_2}]ACTIVITY ({_ACTIVITY_DAYS}D)[/]\n"
        f"[b {tier_colour}]{tier}[/]\n"
        f"[{FG_2}]sil ≥ 2  nic ≥ 7  {spoof} v2_sil0 events[/]\n"
        f"{spark}\n"
        f"[{FG_2}]flights/day - amber = emergency or spoof day[/]"
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


def _build_stat_cards(stats: dict[str, Any]) -> list[Card]:
    """Build the four top-row stat cards from the snapshot's stats block."""
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
    return [
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


def _registry_crumb(reg: dict[str, Any]) -> str:
    """Dot-joined registration / type / description / owner line."""
    parts = [
        reg.get("registration"),
        reg.get("type_code"),
        reg.get("description"),
        reg.get("owner_operator"),
    ]
    return f" {DOT} ".join(p for p in parts if p) or "status"


@dataclass(frozen=True)
class _StatusResult:
    """Everything the worker fetched, applied to the UI on the event loop."""

    icao: str
    snap: dict[str, Any]


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
        """Kick off a background worker to build the status snapshot.

        The query itself runs off the event loop in ``_fetch_status``
        (Task 15); the result lands back here via ``on_worker_state_changed``,
        which is where the cards get built and mounted.
        """
        if self._icao is None:
            self._grid.remove_children()
            self._grid.display = False
            self._empty.display = True
            self._empty.update(Text.from_markup(f"[{FG_2}]no aircraft selected. press 1 and pick one.[/]"))
            self._header.set_crumb("select an aircraft first")
            self._header.set_trailing("")
            return
        self.loading = True
        self._fetch_status(self._icao)

    @work(thread=True, exclusive=True, group="status", exit_on_error=False)
    def _fetch_status(self, icao: str) -> _StatusResult:
        """Run the status-snapshot query on a worker's own connection.

        Must not touch ``self.app.db`` (the main-thread connection) or any
        widget -- only DB reads happen here, and card construction stays
        on the event loop in ``on_worker_state_changed``.
        ``exit_on_error=False`` on the decorator keeps a raised exception
        from crashing the whole app before the ``ERROR`` branch in
        ``on_worker_state_changed`` runs.
        """
        db = self.app.db_factory()
        try:
            snap = status_snapshot(db, icao)
            snap["activity"] = daily_activity(db, icao, days=_ACTIVITY_DAYS)
            return _StatusResult(icao=icao, snap=snap)
        finally:
            db.close()

    def on_worker_state_changed(self, event: Worker.StateChanged) -> None:
        if event.worker.name != "_fetch_status":
            return
        if event.state == WorkerState.SUCCESS:
            result = event.worker.result
            assert result is not None  # a SUCCESS worker always has a result
            self._populate(result)
            self.loading = False
        elif event.state == WorkerState.ERROR:
            self.loading = False
            self.app.notify(f"failed to load status: {event.worker.error}", severity="error")

    def _populate(self, result: _StatusResult) -> None:
        """Rebuild the card grid from a finished snapshot.

        Named ``_populate`` rather than ``_render``: Textual's
        ``Widget._render()`` is a compositor slot, and shadowing it with
        a method of a different signature crashed this view previously.
        """
        snap = result.snap
        stats = snap.get("stats") or {}
        reg = snap.get("registry") or {}

        self._grid.remove_children()
        self._empty.display = False
        self._grid.display = True

        self._header.set_title(result.icao)
        self._header.set_crumb(_registry_crumb(reg))
        self._header.set_trailing(f"{stats.get('first_seen') or '-'} .. {stats.get('last_seen') or '-'}")

        for card in _build_stat_cards(stats):
            self._grid.mount(card)
        self._grid.mount(Card(_build_sources_body(snap.get("sources")), classes="wide"))
        self._grid.mount(Card(_build_missions_body(snap.get("missions") or []), classes="wide"))
        self._grid.mount(Card(_build_indicators_body(snap)))
        self._grid.mount(Card(_build_signal_body(snap)))
        self._grid.mount(Card(_build_registry_body(reg), classes="wide"))

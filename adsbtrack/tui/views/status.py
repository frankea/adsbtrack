"""Per-aircraft status dashboard (small-multiples style)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from textual import work
from textual.app import ComposeResult
from textual.containers import Vertical
from textual.widgets import Static
from textual.worker import Worker, WorkerState

from ..queries import status_snapshot
from ..widgets import (
    ACCENT_AMBER,
    ACCENT_CYAN,
    ACCENT_OK,
    ACCENT_VIOLET,
    FG_0,
    FG_1,
    FG_2,
    PageHeader,
)


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
        self._body = Static(" ", id="status-body")

    def compose(self) -> ComposeResult:
        yield self._header
        yield self._body

    def set_icao(self, icao: str | None) -> None:
        self._icao = icao
        self.refresh_data()

    def refresh_data(self) -> None:
        """Kick off a background worker to build the status snapshot.

        The query itself runs off the event loop in ``_fetch_status``
        (Task 15); the result lands back here via ``on_worker_state_changed``.
        """
        if self._icao is None:
            self._body.update(f"[{FG_2}]no aircraft selected. press 1 and pick one.[/]")
            self._header.set_crumb("select an aircraft first")
            return
        self.loading = True
        self._fetch_status(self._icao)

    @work(thread=True, exclusive=True, group="status", exit_on_error=False)
    def _fetch_status(self, icao: str) -> _StatusResult:
        """Run the status-snapshot query on a worker's own connection.

        Must not touch ``self.app.db`` (the main-thread connection) or any
        widget -- only DB reads happen here. ``exit_on_error=False`` on the
        decorator keeps a raised exception from crashing the whole app
        before the ``ERROR`` branch in ``on_worker_state_changed`` runs.
        """
        db = self.app.db_factory()
        try:
            return _StatusResult(icao=icao, snap=status_snapshot(db, icao))
        finally:
            db.close()

    def on_worker_state_changed(self, event: Worker.StateChanged) -> None:
        if event.worker.name != "_fetch_status":
            return
        if event.state == WorkerState.SUCCESS:
            result = event.worker.result
            assert result is not None  # a SUCCESS worker always has a result
            self._header.set_title(result.icao)
            self._header.set_crumb("status")
            self._header.set_trailing("")
            self._body.update(self._build_body(result.snap))
            self.loading = False
        elif event.state == WorkerState.ERROR:
            self.loading = False
            self.app.notify(f"failed to load status: {event.worker.error}", severity="error")

    def _build_body(self, snap: dict) -> str:
        """Render the dashboard body markup. Renamed from ``_render`` to
        avoid shadowing Textual's ``Widget._render()`` slot, which takes
        a ``snap`` arg of a different shape and crashed the view when
        the compositor tried to call it."""
        lines: list[str] = []
        reg = snap.get("registry") or {}
        stats = snap.get("stats") or {}
        if reg:
            lines.append(
                f"[{FG_2}]registration[/]  [b {FG_0}]{reg.get('registration') or '-'}[/]"
                f"    [{FG_2}]type[/]  [{FG_0}]{reg.get('type_code') or '-'}[/]"
                f"    [{FG_2}]desc[/]  [{FG_0}]{reg.get('description') or '-'}[/]"
            )
            if reg.get("owner_operator"):
                lines.append(f"[{FG_2}]owner[/]         {reg['owner_operator']}")
        if stats:
            lines.append("")
            lines.append(
                f"[{FG_2}]first seen[/]   [{ACCENT_CYAN}]{stats.get('first_seen') or '-'}[/]"
                f"    [{FG_2}]last seen[/]  [{ACCENT_CYAN}]{stats.get('last_seen') or '-'}[/]"
            )
            lines.append(
                f"[{FG_2}]total[/]        [{FG_0}]{stats.get('total_flights') or 0:,}[/] flights, "
                f"[{FG_0}]{stats.get('total_hours') or 0:.1f}[/] hours"
            )
            if stats.get("avg_flight_minutes") is not None:
                lines.append(f"[{FG_2}]avg flight[/]   {stats['avg_flight_minutes']:.1f} min")
            if stats.get("home_base_icao"):
                share = (stats.get("home_base_share") or 0) * 100
                uncert = " [amber](uncertain)[/amber]" if stats.get("home_base_uncertain") else ""
                lines.append(
                    f"[{FG_2}]home base[/]    [{ACCENT_CYAN}]{stats['home_base_icao']}[/]  "
                    f"{share:.1f}%{uncert}".replace("[amber]", f"[{ACCENT_AMBER}]").replace("[/amber]", "[/]")
                )
        src = snap.get("sources")
        if src:
            lines.append("")
            lines.append(f"[b {FG_0}]Position sources[/]")
            lines.append(
                f"  [{ACCENT_OK}]ADS-B {src['adsb']:5.1f}%[/]   "
                f"[{FG_2}]MLAT  {src['mlat']:5.1f}%[/]   "
                f"[{ACCENT_AMBER}]TIS-B {src['tisb']:5.1f}%[/]   "
                f"[{ACCENT_VIOLET}]ADS-C {src['adsc']:5.1f}%[/]   "
                f"[{FG_1}]other {src['other']:5.1f}%[/]"
            )
        missions = snap.get("missions") or []
        if missions:
            lines.append("")
            lines.append(f"[b {FG_0}]Missions[/]")
            total = sum(n for _, n in missions)
            for name, n in missions:
                pct = 100 * n / total if total else 0
                lines.append(f"  [{FG_0}]{name:<12}[/] [{FG_1}]{n:>5}[/]   ({pct:5.1f}%)")
        spoof = snap.get("spoof_count") or 0
        if spoof:
            lines.append("")
            lines.append(f"[{ACCENT_VIOLET}]Spoofed broadcasts rejected: {spoof}[/]")
        return "\n".join(lines) if lines else f"[{FG_2}]no data for {self._icao}[/]"

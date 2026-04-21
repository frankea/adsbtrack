"""Per-aircraft status dashboard (small-multiples style)."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.widgets import Static

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
        if self._icao is None:
            self._body.update(f"[{FG_2}]no aircraft selected. press 1 and pick one.[/]")
            self._header.set_crumb("select an aircraft first")
            return
        snap = status_snapshot(self.app.db, self._icao)
        self._header.set_title(self._icao)
        self._header.set_crumb("status")
        self._header.set_trailing("")
        self._body.update(self._render(snap))

    def _render(self, snap: dict) -> str:
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

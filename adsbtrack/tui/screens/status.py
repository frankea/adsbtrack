"""Per-aircraft status dashboard (small-multiples style)."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.screen import Screen
from textual.widgets import Static

from ..queries import status_snapshot
from ..widgets import PageHeader


class StatusScreen(Screen):
    """Snapshot of utilisation, sources, missions, and registry for one ICAO."""

    BINDINGS = [("escape", "back", "Back")]

    def __init__(self, icao: str) -> None:
        super().__init__()
        self._icao = icao
        self._header = PageHeader(icao, crumb="status")
        self._body = Static("", id="status-body")

    def compose(self) -> ComposeResult:
        with Vertical():
            yield self._header
            yield self._body

    def on_mount(self) -> None:
        self._refresh()

    def action_back(self) -> None:
        self.app.pop_screen()

    def _refresh(self) -> None:
        snap = status_snapshot(self.app.db, self._icao)
        lines: list[str] = []

        reg = snap.get("registry") or {}
        stats = snap.get("stats") or {}
        if reg:
            lines.append(
                f"[#6b7885]registration[/]  [b]{reg.get('registration') or '-'}[/b]   "
                f"[#6b7885]type[/]  {reg.get('type_code') or '-'}   "
                f"[#6b7885]desc[/]  {reg.get('description') or '-'}"
            )
            if reg.get("owner_operator"):
                lines.append(f"[#6b7885]owner[/]       {reg['owner_operator']}")
        if stats:
            lines.append("")
            lines.append(
                f"[#6b7885]first seen[/] [#4fb8e0]{stats.get('first_seen') or '-'}[/]"
                f"   [#6b7885]last seen[/] [#4fb8e0]{stats.get('last_seen') or '-'}[/]"
            )
            lines.append(
                f"[#6b7885]total[/]      [#e4ecf3]{stats.get('total_flights') or 0:,}[/] flights, "
                f"[#e4ecf3]{stats.get('total_hours') or 0:.1f}[/] hours"
            )
            lines.append(f"[#6b7885]avg flight[/] {stats.get('avg_flight_minutes') or 0:.1f} min")
            if stats.get("home_base_icao"):
                share = (stats.get("home_base_share") or 0) * 100
                uncert = " (uncertain)" if stats.get("home_base_uncertain") else ""
                lines.append(f"[#6b7885]home base[/]  [#4fb8e0]{stats['home_base_icao']}[/]  {share:.1f}%{uncert}")
        src = snap.get("sources")
        if src:
            lines.append("")
            lines.append("[b]Position sources[/b]")
            lines.append(
                f"  [#4ec07a]ADS-B {src['adsb']:5.1f}%[/]  "
                f"[#6b7885]MLAT {src['mlat']:5.1f}%[/]  "
                f"[#f2b136]TIS-B {src['tisb']:5.1f}%[/]  "
                f"[#c24bd6]ADS-C {src['adsc']:5.1f}%[/]  "
                f"[#4fb8e0]other {src['other']:5.1f}%[/]"
            )
        missions = snap.get("missions") or []
        if missions:
            lines.append("")
            lines.append("[b]Missions[/b]")
            total = sum(n for _, n in missions)
            for name, n in missions:
                pct = 100 * n / total if total else 0
                lines.append(f"  [#e4ecf3]{name:<12}[/] {n:>5} ({pct:5.1f}%)")
        spoof = snap.get("spoof_count") or 0
        if spoof:
            lines.append("")
            lines.append(f"[#c24bd6]Spoofed broadcasts rejected: {spoof}[/]")
        self._body.update("\n".join(lines) if lines else f"no data for {self._icao}")

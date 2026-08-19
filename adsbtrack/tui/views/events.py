"""Event feed view: unified chronological stream across event types."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from rich.text import Text
from textual import work
from textual.app import ComposeResult
from textual.containers import Vertical
from textual.widgets import DataTable, Input
from textual.worker import Worker, WorkerState

from ..queries import list_events
from ..widgets import (
    ACCENT_AMBER,
    ACCENT_CYAN,
    ACCENT_RED,
    ACCENT_VIOLET,
    DOT,
    FG_0,
    FG_1,
    FG_2,
    FilterBar,
    PageHeader,
    cell,
    pill_markup,
)

# Concept-specified pill label per event_type.
_EVENT_PILLS: dict[str, tuple[str, str]] = {
    "emergency_squawk": ("EMERGENCY", ACCENT_RED),
    "emergency_flag": ("EMERGENCY", ACCENT_RED),
    "off_airport_landing": ("OFF-AIRPORT", ACCENT_AMBER),
    "long_hover": ("LONG HOVER", ACCENT_AMBER),
    "multiple_go_arounds": ("GO-AROUND", ACCENT_AMBER),
}


def _pill_for(event_type: str, severity: str) -> tuple[str, str]:
    if event_type.startswith("spoof"):
        return "SPOOF", ACCENT_VIOLET
    if event_type in _EVENT_PILLS:
        return _EVENT_PILLS[event_type]
    if severity == "emergency":
        return "EMERGENCY", ACCENT_RED
    if severity == "unusual":
        return "UNUSUAL", ACCENT_AMBER
    return event_type.upper(), FG_2


def filter_events(rows: Iterable[Any], needle: str) -> list[Any]:
    """Return the events matching ``needle`` (case-insensitive substring).

    Pure function, no Textual/DB dependency, so the filter bar's re-filter
    path can be exercised without a running app. Matches ``event_type``,
    ``icao``, ``callsign``, and ``summary`` -- the same fields the old
    in-view ``_matches`` helper checked.
    """
    needle_low = needle.lower() if needle else ""
    if not needle_low:
        return list(rows)
    return [e for e in rows if _event_matches(e, needle_low)]


def _event_matches(event: Any, needle_low: str) -> bool:
    for field in ("event_type", "icao", "callsign", "summary"):
        v = getattr(event, field, None)
        if v and needle_low in str(v).lower():
            return True
    return False


@dataclass(frozen=True)
class _EventsResult:
    """Everything the worker fetched, applied to the UI on the event loop."""

    icao: str | None
    rows: list[Any]
    counts: dict[str, int]


class EventsView(Vertical):
    """Chronological event stream, optionally scoped to one ICAO."""

    def __init__(self) -> None:
        super().__init__(id="view-events")
        self._icao: str | None = None
        self._rows: list = []
        self._header = PageHeader(
            "events",
            crumb="all aircraft",
            widget_id="events-header",
        )
        self._filter = FilterBar(
            placeholder="filter events (type:emergency, icao:ae, since:3d)",
            widget_id="events-filter",
        )
        self._table = DataTable(id="events-table", zebra_stripes=True)

    def compose(self) -> ComposeResult:
        yield self._header
        yield self._filter.build()
        yield self._table

    def on_mount(self) -> None:
        self._table.cursor_type = "row"
        self._table.add_column("TIME", width=18)
        self._table.add_column("EVENT", width=14)
        self._table.add_column("ICAO", width=8)
        self._table.add_column("CALLSIGN", width=10)
        self._table.add_column("SUMMARY")

    def set_icao(self, icao: str | None) -> None:
        """Switch (or clear) the aircraft this view is scoped to.

        Clears the filter Input as part of the switch: a needle typed for
        the previous scope has no reason to apply to the new one, and
        leaving it in place would either silently narrow (or empty out)
        the new scope's table with no visible explanation, since the box
        and the table must always agree (see A4's fix in
        ``on_worker_state_changed`` below, which re-applies whatever the
        box holds -- this keeps that "whatever" honest across a scope
        switch too).
        """
        self._icao = icao
        self._filter.input_widget.value = ""
        self.refresh_data()

    def refresh_data(self) -> None:
        """Kick off a background worker to query events for the current scope.

        This is the only path that re-queries the DB; the filter bar
        re-filters the cached ``self._rows`` via ``_apply_filter`` without
        touching the DB again (Task 14). The query itself runs off the
        event loop in ``_fetch_events`` (Task 15); results land back here
        via ``on_worker_state_changed``.
        """
        self.loading = True
        self._fetch_events(self._icao)

    @work(thread=True, exclusive=True, group="events", exit_on_error=False)
    def _fetch_events(self, icao: str | None) -> _EventsResult:
        """Run the event query on a worker's own connection (thread-bound).

        Must not touch ``self.app.db`` (the main-thread connection) or any
        widget -- only DB reads and pure computation happen here.
        ``exit_on_error=False`` on the decorator: Textual's default
        (``True``) routes any exception raised here into a fatal app
        crash *before* the ``ERROR`` ``StateChanged`` message we handle
        below is delivered, which would make the ``on_worker_state_changed``
        error branch dead code.
        """
        db = self.app.db_factory()
        try:
            rows = list_events(db, icao, include_spoof_checks=True, config=self.app.config)
        finally:
            db.close()
        counts = {"emergency": 0, "unusual": 0, "spoof": 0}
        for e in rows:
            if e.event_type.startswith("spoof"):
                counts["spoof"] += 1
            elif e.severity == "emergency":
                counts["emergency"] += 1
            else:
                counts["unusual"] += 1
        return _EventsResult(icao=icao, rows=rows, counts=counts)

    def on_worker_state_changed(self, event: Worker.StateChanged) -> None:
        if event.worker.name != "_fetch_events":
            return
        if event.state == WorkerState.SUCCESS:
            result = event.worker.result
            assert result is not None  # a SUCCESS worker always has a result
            self._rows = result.rows
            crumb = "all aircraft" if result.icao is None else result.icao
            self._header.set_crumb(f"{crumb} {DOT} last 7d")
            # Trailing severity-pills line (tinted pills, per concept).
            self._header.set_trailing(
                Text.from_markup(
                    " ".join(
                        [
                            pill_markup(f"emergency {result.counts['emergency']}", ACCENT_RED),
                            pill_markup(f"unusual {result.counts['unusual']}", ACCENT_AMBER),
                            pill_markup(f"spoof {result.counts['spoof']}", ACCENT_VIOLET),
                        ]
                    )
                )
            )
            self._apply_filter(self._filter.input_widget.value)
            self.loading = False
        elif event.state == WorkerState.ERROR:
            self.loading = False
            self.app.notify(f"failed to load events: {event.worker.error}", severity="error")

    def _apply_filter(self, needle: str) -> None:
        rows = filter_events(self._rows, needle)
        self._table.clear()
        for e in rows:
            label, colour = _pill_for(e.event_type, e.severity)
            ts_short = e.ts.strftime("%Y-%m-%d %H:%MZ") if getattr(e, "ts", None) else "-"
            self._table.add_row(
                cell(ts_short, style=FG_1),
                Text.from_markup(pill_markup(label, colour)),
                cell(e.icao, style=ACCENT_CYAN),
                cell(e.callsign or "-", style=FG_0 if e.callsign else FG_2),
                cell(e.summary or "", style=FG_1),
            )
        self._filter.set_counts(matched=len(rows), total=len(self._rows))

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input is self._filter.input_widget:
            self._apply_filter(event.value or "")

    def focus_filter(self) -> None:
        self._filter.input_widget.focus()

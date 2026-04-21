"""Main Textual application for the adsbtrack TUI.

Architecture note. The whole app runs inside a single Screen that owns
a persistent 3-part layout (status strip on top, sidebar on the left,
content pane on the right). The content pane is a ``ContentSwitcher``
hosting every view as a sibling Container. Switching views changes the
current container - the sidebar and status strip never unmount.

This is different from the earlier push_screen()-per-view pattern,
which worked functionally but erased the persistent chrome every time
the user switched views.
"""

from __future__ import annotations

import contextlib
from pathlib import Path

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.widgets import ContentSwitcher, Footer

from ..db import Database
from .queries import count_aircraft, count_flights
from .views.aircraft import AircraftOpenFlights, AircraftView
from .views.events import EventsView
from .views.flights import FlightsView
from .views.map import MapView
from .views.ops import OpsView
from .views.spoof import SpoofView
from .views.status import StatusView
from .widgets import Sidebar, StatusStrip

_STYLES_PATH = Path(__file__).resolve().parent / "styles" / "app.tcss"


class AdsbtrackApp(App):
    """Single-screen workspace over the local SQLite DB the CLI writes."""

    CSS_PATH = str(_STYLES_PATH)
    TITLE = "adsbtrack"

    BINDINGS = [
        Binding("1", "goto('aircraft')", "Aircraft"),
        Binding("2", "goto('flights')", "Flights"),
        Binding("3", "goto('events')", "Events"),
        Binding("4", "goto('spoof')", "Spoof"),
        Binding("5", "goto('map')", "Map"),
        Binding("6", "goto('status')", "Status"),
        Binding("f", "goto('ops')", "Ops"),
        Binding("slash", "focus_filter", "Filter"),
        Binding("q", "quit", "Quit"),
        Binding("question_mark", "help", "Help"),
    ]

    def __init__(self, db_path: Path, *, project_root: Path | None = None) -> None:
        super().__init__()
        self._db_path = db_path
        self._db: Database | None = None
        self.project_root = project_root or Path.cwd()
        self._current_icao: str | None = None

    # --- lifecycle ---

    @property
    def db(self) -> Database:
        if self._db is None:
            self._db = Database(self._db_path)
        return self._db

    def on_mount(self) -> None:
        flights_n, aircraft_n = 0, 0
        with contextlib.suppress(Exception):
            flights_n = count_flights(self.db)
            aircraft_n = count_aircraft(self.db)
        self.query_one(StatusStrip).set_counts(flights=flights_n, aircraft=aircraft_n)
        self.query_one(Sidebar).set_active("aircraft")
        # Set the initial active view after all children are mounted so
        # ContentSwitcher can resolve the id to a real child widget.
        self.query_one(ContentSwitcher).current = "view-aircraft"

    def on_unmount(self) -> None:
        if self._db is not None:
            with contextlib.suppress(Exception):
                self._db.close()

    # --- composition ---

    def compose(self) -> ComposeResult:
        yield StatusStrip(db_path=str(self._db_path), flights=0, aircraft=0)
        with Horizontal(id="app-row"):
            yield Sidebar()
            with Vertical(id="content"):  # noqa: SIM117 -- Textual needs distinct with blocks
                with ContentSwitcher(id="switcher"):
                    yield AircraftView()
                    yield FlightsView()
                    yield EventsView()
                    yield SpoofView()
                    yield MapView()
                    yield StatusView()
                    yield OpsView()
        yield Footer()

    # --- navigation ---

    def action_goto(self, view_id: str) -> None:
        self._goto(view_id)

    def action_focus_filter(self) -> None:
        current = self._current_view()
        if current is None:
            return
        focus = getattr(current, "focus_filter", None)
        if callable(focus):
            focus()

    def action_help(self) -> None:
        self.notify(
            "1/2/3/4/5/6 switch views, f ops, / filter, enter opens flights, esc back, q quit",
            title="help",
        )

    # --- cross-view messages ---

    def on_aircraft_open_flights(self, message: AircraftOpenFlights) -> None:
        self._current_icao = message.icao
        # Tell the scoped views which ICAO they should be showing.
        self.query_one(FlightsView).set_icao(message.icao)
        self.query_one(EventsView).set_icao(message.icao)
        self.query_one(MapView).set_icao(message.icao)
        self.query_one(StatusView).set_icao(message.icao)
        self._goto("flights")

    # --- helpers ---

    def _goto(self, view_id: str) -> None:
        target = f"view-{view_id}"
        switcher = self.query_one(ContentSwitcher)
        if view_id in {"flights", "events", "map", "status"} and self._current_icao is None and view_id != "events":
            self.bell()
            self.notify("select an aircraft first (press 1)", severity="warning")
            return
        # EventsView can run against "all aircraft" when no ICAO is set,
        # but the others need a selection. We already rebuffered above;
        # still call the setters for freshness.
        if view_id == "flights":
            self.query_one(FlightsView).set_icao(self._current_icao or "")
        elif view_id == "map":
            self.query_one(MapView).set_icao(self._current_icao)
        elif view_id == "status":
            self.query_one(StatusView).set_icao(self._current_icao)
        elif view_id == "events":
            # Re-run without resetting ICAO so the scope stays consistent with the current selection.
            self.query_one(EventsView).set_icao(self._current_icao)
        elif view_id == "spoof":
            self.query_one(SpoofView).refresh_data()
        switcher.current = target
        self.query_one(Sidebar).set_active(
            view_id
            if view_id in {"aircraft", "flights", "events", "spoof", "map", "status"}
            else "ops"
            if view_id == "ops"
            else self._current_sidebar_id()
        )

    def _current_view(self):  # type: ignore[no-untyped-def]
        switcher = self.query_one(ContentSwitcher)
        current = switcher.current
        if not current:
            return None
        return switcher.get_child_by_id(current) if hasattr(switcher, "get_child_by_id") else None

    def _current_sidebar_id(self) -> str:
        switcher = self.query_one(ContentSwitcher)
        current = switcher.current or "view-aircraft"
        return current.replace("view-", "")

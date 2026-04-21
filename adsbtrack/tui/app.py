"""Main Textual application for the adsbtrack TUI."""

from __future__ import annotations

import contextlib
from pathlib import Path

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.widgets import Footer, Header, Label, ListItem, ListView

from ..db import Database
from .queries import count_aircraft, count_flights
from .screens.aircraft import AircraftScreen, OpenFlights
from .screens.events import EventsScreen
from .screens.flights import FlightsScreen
from .screens.map import MapScreen
from .screens.ops import OpsScreen
from .screens.spoof import SpoofScreen
from .screens.status import StatusScreen
from .widgets import StatusStrip

_STYLES_PATH = Path(__file__).resolve().parent / "styles" / "app.tcss"


class AdsbtrackApp(App):
    """Read-only workspace over the local SQLite DB the CLI writes."""

    CSS_PATH = str(_STYLES_PATH)

    BINDINGS = [
        Binding("1", "goto_aircraft", "Aircraft"),
        Binding("2", "goto_flights", "Flights"),
        Binding("3", "goto_events", "Events"),
        Binding("4", "goto_spoof", "Spoof"),
        Binding("5", "goto_map", "Map"),
        Binding("6", "goto_status", "Status"),
        Binding("f", "goto_ops", "Ops"),
        Binding("q", "quit", "Quit"),
        Binding("?", "help", "Help"),
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
        self.push_screen(AircraftScreen())

    def on_unmount(self) -> None:
        if self._db is not None:
            with contextlib.suppress(Exception):
                self._db.close()

    # --- composition ---

    def compose(self) -> ComposeResult:
        yield Header(show_clock=False)
        yield StatusStrip(
            db_path=str(self._db_path),
            flights=0,
            aircraft=0,
        )
        with Horizontal():
            yield ListView(
                ListItem(Label("1  aircraft"), id="nav-aircraft"),
                ListItem(Label("2  flights"), id="nav-flights"),
                ListItem(Label("3  events"), id="nav-events"),
                ListItem(Label("4  spoof"), id="nav-spoof"),
                ListItem(Label("5  map"), id="nav-map"),
                ListItem(Label("6  status"), id="nav-status"),
                ListItem(Label("f  ops"), id="nav-ops"),
                id="view-switcher",
            )
            with Vertical(id="content"):
                pass
        yield Footer()

    # --- navigation actions ---

    def action_goto_aircraft(self) -> None:
        self._reset_to(AircraftScreen())

    def action_goto_flights(self) -> None:
        if self._current_icao is None:
            self.bell()
            return
        self._reset_to(FlightsScreen(self._current_icao))

    def action_goto_events(self) -> None:
        self._reset_to(EventsScreen(self._current_icao))

    def action_goto_spoof(self) -> None:
        self._reset_to(SpoofScreen())

    def action_goto_map(self) -> None:
        if self._current_icao is None:
            self.bell()
            return
        self._reset_to(MapScreen(self._current_icao))

    def action_goto_status(self) -> None:
        if self._current_icao is None:
            self.bell()
            return
        self._reset_to(StatusScreen(self._current_icao))

    def action_goto_ops(self) -> None:
        self._reset_to(OpsScreen())

    def action_help(self) -> None:
        self.notify(
            "1/2/3/4/5/6 switch views, f ops, / filter, enter open, esc back, q quit",
            title="help",
        )

    def _reset_to(self, screen) -> None:  # type: ignore[no-untyped-def]
        while len(self.screen_stack) > 1:
            self.pop_screen()
        self.push_screen(screen)

    # --- cross-screen messages ---

    def on_open_flights(self, message: OpenFlights) -> None:
        self._current_icao = message.icao
        self.push_screen(FlightsScreen(message.icao))

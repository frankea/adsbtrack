"""Jump-to-hex modal screen.

Opens over the whole app when the user presses `:` and searches the
current DB for aircraft by ICAO hex, registration, type code, or
description. Pressing Enter on the highlighted match posts an
``AircraftOpenFlights`` message to the parent app which navigates to
the flight timeline for that aircraft.

The aircraft list is fetched once via a background worker on mount
(the fetch/filter pattern every other view under ``tui/views/`` uses,
see ``aircraft.py``); each keystroke re-filters the cached list
in-memory via ``filter_jump_matches`` rather than re-querying the DB.
"""

from __future__ import annotations

from collections.abc import Sequence

from rich.text import Text
from textual import work
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.message import Message
from textual.screen import ModalScreen
from textual.widgets import DataTable, Input, Label
from textual.worker import Worker, WorkerState

from ..queries import JumpMatch, search_aircraft
from ..widgets import ACCENT_CYAN, FG_0, FG_1, FG_2

# Visible-row cap for the palette table -- matches the DB-side LIMIT the
# old synchronous search_aircraft(db, query, limit=8) call used, now
# applied after in-memory filtering instead of in SQL.
_VISIBLE_MATCHES = 8


def filter_jump_matches(rows: Sequence[JumpMatch], needle: str) -> list[JumpMatch]:
    """Return the aircraft matching ``needle`` (case-insensitive substring).

    Pure function, no Textual/DB dependency, mirroring
    ``aircraft.filter_aircraft``. Matches icao, registration, type code,
    and description -- the same four columns ``queries.search_aircraft``'s
    SQL ``LIKE`` clause matched before this task moved filtering out of
    the query. Unlike ``filter_aircraft`` this also matches description
    (not home_base_icao, which JumpMatch doesn't carry) so the fields
    genuinely differ and this can't just reuse that function.
    """
    needle_low = needle.lower() if needle else ""
    if not needle_low:
        return list(rows)
    return [r for r in rows if _jump_match(r, needle_low)]


def _jump_match(row: JumpMatch, needle_low: str) -> bool:
    return any(
        hay and needle_low in str(hay).lower() for hay in (row.icao, row.registration, row.type_code, row.description)
    )


class JumpSelected(Message):
    """Bubble to the app when a jump target is chosen."""

    def __init__(self, icao: str) -> None:
        super().__init__()
        self.icao = icao


class JumpToHex(ModalScreen[str | None]):
    """Incremental search across the aircraft_stats / registry tables."""

    BINDINGS = [
        Binding("escape", "dismiss(None)", "Cancel"),
        Binding("enter", "accept", "Open"),
        Binding("down", "cursor_down", show=False),
        Binding("up", "cursor_up", show=False),
    ]

    DEFAULT_CSS = """
    JumpToHex {
        align: center top;
    }
    """

    def __init__(self) -> None:
        super().__init__()
        self._input = Input(placeholder="icao hex, N-number, or callsign", id="jump-input")
        self._results = DataTable(id="jump-results", show_header=False, cursor_type="row")
        self._rows: list[JumpMatch] = []
        self._matches: list[JumpMatch] = []

    def compose(self) -> ComposeResult:
        dialog = Vertical(
            Label(Text.from_markup(f"[{FG_2}]JUMP TO HEX[/]"), classes="jump-title"),
            self._input,
            self._results,
            id="jump-dialog",
        )
        yield dialog

    def on_mount(self) -> None:
        self._results.add_column("ICAO", width=10)
        self._results.add_column("REG", width=10)
        self._results.add_column("TYPE")
        self._input.focus()
        self.loading = True
        self._fetch_aircraft()

    @work(thread=True, exclusive=True, group="jump", exit_on_error=False)
    def _fetch_aircraft(self) -> list[JumpMatch]:
        """Fetch the full aircraft list once, on a worker's own connection.

        Must not touch ``self.app.db`` (the main-thread connection) or any
        widget -- only DB reads happen here, matching every other view's
        fetch worker under ``tui/views/``. ``exit_on_error=False`` keeps a
        raised exception from crashing the whole app before the ``ERROR``
        branch in ``on_worker_state_changed`` runs. An empty query returns
        every aircraft (search_aircraft's WHERE clause is only built when
        ``query`` is truthy); the palette then filters this cached list
        in-memory on every keystroke via ``filter_jump_matches``.
        """
        db = self.app.db_factory()  # type: ignore[attr-defined]
        try:
            return search_aircraft(db, "", limit=5000)
        finally:
            db.close()

    def on_worker_state_changed(self, event: Worker.StateChanged) -> None:
        if event.worker.name != "_fetch_aircraft":
            return
        if event.state == WorkerState.SUCCESS:
            self._rows = event.worker.result or []
            self._apply_filter(self._input.value or "")
            self.loading = False
        elif event.state == WorkerState.ERROR:
            self.loading = False
            self.app.notify(f"failed to load aircraft: {event.worker.error}", severity="error")  # type: ignore[attr-defined]

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input is self._input:
            self._apply_filter(event.value or "")

    def on_input_submitted(self, event: Input.Submitted) -> None:
        self.action_accept()

    def action_cursor_down(self) -> None:
        row = self._results.cursor_row
        if row is None:
            row = -1
        if row + 1 < len(self._matches):
            self._results.move_cursor(row=row + 1)

    def action_cursor_up(self) -> None:
        row = self._results.cursor_row or 0
        if row > 0:
            self._results.move_cursor(row=row - 1)

    def action_accept(self) -> None:
        if not self._matches:
            query = self._input.value.strip()
            if query:
                self.app.notify(f"no aircraft match '{query}'", severity="warning")
            self.dismiss(None)
            return
        idx = self._results.cursor_row or 0
        if 0 <= idx < len(self._matches):
            match = self._matches[idx]
            self.post_message(JumpSelected(match.icao))
            self.dismiss(match.icao)
        else:
            self.dismiss(None)

    def _apply_filter(self, needle: str) -> None:
        self._matches = filter_jump_matches(self._rows, needle)[:_VISIBLE_MATCHES]
        self._results.clear()
        for m in self._matches:
            self._results.add_row(
                Text(m.icao, style=ACCENT_CYAN),
                Text(m.registration or "-", style=FG_0 if m.registration else FG_2),
                Text(m.description or m.type_code or "-", style=FG_1),
            )
        if self._matches:
            self._results.move_cursor(row=0)


# Keyboard-shortcut hints shown in the help overlay. Every key here must
# match a real binding: row 0 is app.py's filter/movement/select keys
# (movement + select are DataTable's own built-in bindings, not app-level
# ones -- there is no "j"/"k"/"g g"/"G" binding anywhere in this app), row
# 1 is app-level chrome, row 2 is the view-switch bindings from
# AdsbtrackApp.BINDINGS. Kept as a module constant (not inline in
# compose()) so tests can cross-check it against the real bindings.
_HELP_ROWS: list[list[tuple[str, str]]] = [
    [
        ("/", "filter"),
        ("up", "cursor up"),
        ("down", "cursor down"),
        ("enter", "select"),
        ("ctrl+home", "top"),
        ("ctrl+end", "bottom"),
    ],
    [(":", "jump to hex"), ("esc", "back"), ("?", "help"), ("q", "quit")],
    [
        ("1", "aircraft"),
        ("2", "flights"),
        ("3", "events"),
        ("4", "spoof"),
        ("5", "map"),
        ("6", "status"),
        ("f", "ops"),
    ],
]


class HelpScreen(ModalScreen[None]):
    """Modal listing keyboard shortcuts, matching design/components-kbd.html."""

    BINDINGS = [
        Binding("escape", "dismiss(None)"),
        Binding("question_mark", "dismiss(None)"),
    ]

    DEFAULT_CSS = """
    HelpScreen {
        align: center middle;
    }
    """

    def compose(self) -> ComposeResult:
        lines: list[str] = [f"[b {FG_0}]Keyboard shortcuts[/]", ""]
        for row in _HELP_ROWS:
            cells = []
            for key, label in row:
                cells.append(f"[{FG_2} on #0b0f14] {key} [/] [{FG_1}]{label}[/]")
            lines.append("   ".join(cells))
        lines.append("")
        lines.append(f"[{FG_2}]press esc or ? to close[/]")
        yield Vertical(Label(Text.from_markup("\n".join(lines))), id="help-dialog")

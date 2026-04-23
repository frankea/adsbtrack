"""Jump-to-hex modal screen.

Opens over the whole app when the user presses `:` and searches the
current DB for aircraft by ICAO hex, registration, type code, or
description. Pressing Enter on the highlighted match posts an
``AircraftOpenFlights`` message to the parent app which navigates to
the flight timeline for that aircraft.
"""

from __future__ import annotations

from rich.text import Text
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.message import Message
from textual.screen import ModalScreen
from textual.widgets import DataTable, Input, Label

from ..queries import JumpMatch, search_aircraft
from ..widgets import ACCENT_CYAN, FG_0, FG_1, FG_2


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
        self._refresh("")
        self._input.focus()

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input is self._input:
            self._refresh(event.value or "")

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

    def _refresh(self, query: str) -> None:
        db = self.app.db  # type: ignore[attr-defined]
        self._matches = search_aircraft(db, query)
        self._results.clear()
        for m in self._matches:
            self._results.add_row(
                Text(m.icao, style=ACCENT_CYAN),
                Text(m.registration or "-", style=FG_0 if m.registration else FG_2),
                Text(m.description or m.type_code or "-", style=FG_1),
            )
        if self._matches:
            self._results.move_cursor(row=0)


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
        rows = [
            [("/", "search"), ("f", "filter"), ("j", "next"), ("k", "prev"), ("g g", "top"), ("G", "bottom")],
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
        lines: list[str] = [f"[b {FG_0}]Keyboard shortcuts[/]", ""]
        for row in rows:
            cells = []
            for key, label in row:
                cells.append(f"[{FG_2} on #0b0f14] {key} [/] [{FG_1}]{label}[/]")
            lines.append("   ".join(cells))
        lines.append("")
        lines.append(f"[{FG_2}]press esc or ? to close[/]")
        yield Vertical(Label(Text.from_markup("\n".join(lines))), id="help-dialog")

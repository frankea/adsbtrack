"""Textual TUI for adsbtrack.

Read-only workspace over the local SQLite database the CLI writes. Six
views (aircraft list, flight timeline, event feed, spoofed broadcasts,
map, status dashboard) plus an operations pane that wraps the
DB-writing commands (fetch, extract, enrich, acars, registry).

Launch via the CLI: ``adsbtrack tui --db adsbtrack.db``.

No eager imports here: ``AdsbtrackApp`` pulls in ``textual`` (an
optional extra), but the ``.queries`` submodule is pure sqlite3 and is
consumed by ``adsbtrack.gui_export``, which must be importable without
the tui extra installed. Import ``AdsbtrackApp`` from ``adsbtrack.tui.app``
directly.
"""

from __future__ import annotations

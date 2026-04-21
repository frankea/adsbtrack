"""Textual TUI for adsbtrack.

Read-only workspace over the local SQLite database the CLI writes. Six
views (aircraft list, flight timeline, event feed, spoofed broadcasts,
map, status dashboard) plus an operations pane that wraps the
DB-writing commands (fetch, extract, enrich, acars, registry).

Launch via the CLI: ``adsbtrack tui --db adsbtrack.db``.
"""

from __future__ import annotations

from .app import AdsbtrackApp

__all__ = ["AdsbtrackApp"]

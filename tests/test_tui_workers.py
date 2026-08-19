"""Regression test for Task 15's worker error-handling path.

Textual's ``@work`` decorator defaults to ``exit_on_error=True``, which
routes any exception raised inside a thread worker into the app's fatal
crash path (``App._handle_exception``) *before* the queued ``ERROR``
``Worker.StateChanged`` message is delivered to ``on_worker_state_changed``.
Every view's fetch worker in ``adsbtrack/tui/views/*.py`` sets
``exit_on_error=False`` specifically so a DB error (locked DB, malformed
row, etc.) degrades to a ``notify(..., severity="error")`` instead of
crashing the whole TUI. This test forces a fetch to raise and proves the
app survives, the view's loading state clears, and the failure notifies.

No pytest-asyncio (or similar) plugin is installed in this project, so
the async Pilot session is driven with ``asyncio.run()`` from a plain
sync test, matching how the rest of the suite avoids new test deps.
"""

from __future__ import annotations

import asyncio

import pytest

pytest.importorskip("textual")  # tui extra: pyproject [project.optional-dependencies].tui

import adsbtrack.tui.views.aircraft as aircraft_view_module  # noqa: E402
from adsbtrack.tui.app import AdsbtrackApp  # noqa: E402
from adsbtrack.tui.views.aircraft import AircraftView  # noqa: E402


async def _settle(app, pilot) -> None:
    """Poll until no worker on the app is PENDING/RUNNING.

    Deliberately avoids ``app.workers.wait_for_complete()`` -- it calls
    ``worker.wait()`` on every worker, which raises for one that was
    legitimately superseded via exclusive+group cancellation. Polling
    ``worker.state`` sidesteps that without treating cancellation as a
    failure.
    """
    for _ in range(500):
        active = [w for w in app.workers if w.state.name in ("PENDING", "RUNNING")]
        if not active:
            return
        await pilot.pause()
        await asyncio.sleep(0.01)
    raise AssertionError("workers did not settle in time")


def test_worker_exception_does_not_crash_app_and_notifies(tmp_path, monkeypatch):
    """A raised exception inside a fetch worker must not take down the app.

    Simulates a DB failure by making the aircraft view's underlying query
    raise, exactly the kind of runtime error (locked DB, malformed row)
    the reviewer flagged. With ``exit_on_error=False`` in place, this
    should: leave the app alive and responsive, clear ``loading`` on the
    view, and surface the failure through ``self.app.notify(...,
    severity="error")``.
    """
    db_path = tmp_path / "empty.db"

    async def scenario() -> None:
        app = AdsbtrackApp(db_path)
        async with app.run_test() as pilot:
            # Let the real on_mount aircraft-list worker (against the
            # freshly-created empty DB) settle first, so we start from a
            # known-good state before injecting the failure.
            await _settle(app, pilot)
            view = app.query_one(AircraftView)
            assert not view.loading

            # Record notify() calls instead of asserting on Textual's
            # internal notification/toast state.
            notify_calls: list[tuple[tuple, dict]] = []
            monkeypatch.setattr(app, "notify", lambda *a, **kw: notify_calls.append((a, kw)))

            # Force the real fetch worker's underlying query to raise,
            # exercising the actual @work(..., exit_on_error=False) path
            # rather than bypassing it.
            def _boom(*args, **kwargs):
                raise RuntimeError("simulated DB failure")

            monkeypatch.setattr(aircraft_view_module, "list_aircraft", _boom)

            view.refresh_data()
            assert view.loading, "loading should be set immediately when the worker starts"
            await _settle(app, pilot)

            # The app must still be alive and pumping the event loop --
            # exit_on_error=True would have crashed run_test() by now.
            assert not view.loading, "loading must clear on the ERROR branch"
            assert notify_calls, "a failed fetch must notify the user instead of crashing silently"
            (args, kwargs) = notify_calls[0]
            assert "failed to load aircraft" in args[0]
            assert kwargs.get("severity") == "error"

            # And the app is still responsive to further interaction.
            await pilot.press("2")
            await pilot.pause()

    asyncio.run(scenario())

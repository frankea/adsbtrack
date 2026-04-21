"""View containers for the adsbtrack TUI.

Each view is a ``Container`` (not a ``Screen``) so the top status strip
and the left sidebar stay persistent across navigation. Views are
stacked inside a ``ContentSwitcher`` in ``app.py`` and switched by id.
"""

"""Tiny braille-canvas for the TUI map view.

Each Unicode braille character (U+2800-U+28FF) encodes a 2x4 dot grid,
which gives the terminal-mode map roughly 8x the effective resolution
of a one-char-per-cell scatter plot. The canvas draws connected line
segments between consecutive trace points (Bresenham), so at-a-glance
the trace looks like a real path instead of loose dots.

No external dependencies. Intentionally small: the projection logic
lives in the map view; this module just rasterises line segments
into braille characters.

Braille dot numbering (from the Unicode reference):

    1 4
    2 5
    3 6
    7 8

Codepoint = 0x2800 + sum of bit flags, where dot N maps to bit N-1.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field

# Map (dx, dy) in 2x4 dot coordinates to the Unicode braille bit index.
_DOT_BITS: dict[tuple[int, int], int] = {
    (0, 0): 0,  # dot 1
    (0, 1): 1,  # dot 2
    (0, 2): 2,  # dot 3
    (0, 3): 6,  # dot 7
    (1, 0): 3,  # dot 4
    (1, 1): 4,  # dot 5
    (1, 2): 5,  # dot 6
    (1, 3): 7,  # dot 8
}


@dataclass
class BrailleCanvas:
    """Character-grid-backed dot buffer with per-cell colour."""

    cols: int  # terminal columns
    rows: int  # terminal rows
    _bits: list[list[int]] = field(init=False)
    _colours: dict[tuple[int, int], str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self._bits = [[0] * max(1, self.cols) for _ in range(max(1, self.rows))]

    # --- primitives ---

    @property
    def dot_width(self) -> int:
        return self.cols * 2

    @property
    def dot_height(self) -> int:
        return self.rows * 4

    def set(self, x: int, y: int, colour: str) -> None:
        """Light up dot at dot-coordinate ``(x, y)``.

        Out-of-bounds coords are dropped silently so callers don't need
        to clip beforehand.
        """
        if x < 0 or y < 0 or x >= self.dot_width or y >= self.dot_height:
            return
        col = x // 2
        row = y // 4
        self._bits[row][col] |= 1 << _DOT_BITS[(x % 2, y % 4)]
        self._colours[(row, col)] = colour

    def line(
        self,
        x0: int,
        y0: int,
        x1: int,
        y1: int,
        colour: str,
        *,
        dash: tuple[int, int] | None = None,
    ) -> None:
        """Bresenham line between two dot-coordinates.

        Draws one dot per step along the longer axis. Endpoints are
        inclusive, so a zero-length ``(x0 == x1, y0 == y1)`` segment
        still lights one dot.

        ``dash``, if given, is an ``(on, off)`` dot-count pattern: only
        the first ``on`` dots of every ``on + off`` step are plotted,
        producing a dashed line (used for signal-loss gaps on the map)
        without a separate copy of this Bresenham walk.
        """
        dx = abs(x1 - x0)
        dy = -abs(y1 - y0)
        sx = 1 if x0 < x1 else -1
        sy = 1 if y0 < y1 else -1
        err = dx + dy
        period = sum(dash) if dash else 0
        step = 0
        while True:
            if dash is None or step % period < dash[0]:
                self.set(x0, y0, colour)
            step += 1
            if x0 == x1 and y0 == y1:
                return
            e2 = 2 * err
            if e2 >= dy:
                err += dy
                x0 += sx
            if e2 <= dx:
                err += dx
                y0 += sy

    # --- rendering ---

    def cells(self) -> Iterator[tuple[int, int, str, str]]:
        """Yield ``(row, col, glyph, colour)`` for every non-empty cell.

        ``colour`` is always present for a lit cell: every ``set()`` call
        that lights a dot also records that cell's colour, so a nonzero
        bitmask always has a matching ``_colours`` entry. This is the
        single place that turns the dot bitmap into glyphs; ``render()``
        and callers outside this module (the map view's compositor) both
        go through it instead of reaching into ``_bits``/``_colours``.
        """
        for r in range(self.rows):
            for c in range(self.cols):
                mask = self._bits[r][c]
                if mask:
                    yield r, c, chr(0x2800 + mask), self._colours[(r, c)]

    def render(self) -> str:
        """Return the canvas as Rich-markup text, one line per row."""
        grid = [[" "] * self.cols for _ in range(self.rows)]
        for r, c, ch, colour in self.cells():
            grid[r][c] = f"[{colour}]{ch}[/]"
        return "\n".join("".join(row) for row in grid)

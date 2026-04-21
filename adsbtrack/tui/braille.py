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

    def line(self, x0: int, y0: int, x1: int, y1: int, colour: str) -> None:
        """Bresenham line between two dot-coordinates.

        Draws one dot per step along the longer axis. Endpoints are
        inclusive, so a zero-length ``(x0 == x1, y0 == y1)`` segment
        still lights one dot.
        """
        dx = abs(x1 - x0)
        dy = -abs(y1 - y0)
        sx = 1 if x0 < x1 else -1
        sy = 1 if y0 < y1 else -1
        err = dx + dy
        while True:
            self.set(x0, y0, colour)
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

    def render(self) -> str:
        """Return the canvas as Rich-markup text, one line per row."""
        lines: list[str] = []
        for r in range(self.rows):
            cells: list[str] = []
            for c in range(self.cols):
                mask = self._bits[r][c]
                if mask == 0:
                    cells.append(" ")
                    continue
                ch = chr(0x2800 + mask)
                colour = self._colours.get((r, c), "#ffffff")
                cells.append(f"[{colour}]{ch}[/]")
            lines.append("".join(cells))
        return "\n".join(lines)

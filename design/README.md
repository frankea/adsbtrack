# Design system

Source of truth for the visual language shared by the CLI's Rich output, the Textual
TUI in `adsbtrack/tui/`, and the static GUI export produced by `adsbtrack gui`.

## Files

- `colors_and_type.css` - CSS custom properties for both themes (dark primary,
  warm-neutral light alternative), typography, spacing, radii, and the
  semantic accent palette. Imported by the GUI export and the preview cards.
- `assets/logo.svg` - lightweight geometric placeholder mark.
- `preview/` - specimen cards (type scale, colour swatches, buttons, inputs,
  kbd, pills, status bar, tables). Each file is standalone and imports
  `../colors_and_type.css`. Open any of them in a browser to verify a token.

The Textual TUI does not consume this CSS directly (Textual's styling format
is `.tcss`). It lives at `adsbtrack/tui/styles/app.tcss`, and the tokens are
kept in lockstep with `colors_and_type.css` by hand.

## Accent palette (semantic, never decorative)

| Token | Hex (dark) | Use |
|-------|-----------|-----|
| `--accent-ok` | `#4ec07a` | confirmed landings, high-confidence rows, OK status |
| `--accent-cyan` | `#4fb8e0` | ICAO hex, runway, ACARS, focus, selection |
| `--accent-amber` | `#f2b136` | anomalies: off-airport landing, long hover, multiple go-arounds |
| `--accent-red` | `#e0433a` | emergency squawks 7500 / 7600 / 7700 |
| `--accent-violet` | `#c24bd6` | spoofed broadcasts |
| `--accent-magenta` | `#d47bd4` | secondary call-outs, mission tags |
| `--accent-bluegrey` | `#6b7885` | MLAT position source, tertiary text |

Position-source overlay colours (map view): `--src-adsb`, `--src-mlat`,
`--src-tisb`, `--src-adsr`, `--src-adsc`. Each maps onto one of the accents
above.

## Rules that carry across surfaces

- Never hide ground truth. `N512WB (a66ad3)`, not `N512WB`. `KSPG (St.
  Petersburg)`, not `St. Petersburg`.
- Timestamps are UTC, ISO 8601 with trailing `Z`.
- Coordinates are decimal degrees, 4 dp, lat first.
- Monospace for every identifier (ICAO hex, tail, callsign, squawk, UTC
  timestamp, coordinates). Tabular numerics for every numeric column.
- No emoji. No em dashes. Regular hyphens only.
- Sentence case for headings and UI labels.
- Accent colours only highlight meaningful content. A whole panel with an
  amber background is wrong; amber text on one cell is right.

## Provenance

Generated in Claude Design (claude.ai/design) on 2026-04-21 using the
adsbtrack repo at that commit as the primary input. The export bundle lived
at `adsbtrack-design-system/` in the download; the preview/assets/CSS files
were moved verbatim into this directory, the HTML prototypes for the TUI
and GUI kits were used as specifications and re-implemented in Textual
(`adsbtrack/tui/`) and static HTML/JS (`adsbtrack/gui_export.py` output)
respectively rather than vendored as-is.

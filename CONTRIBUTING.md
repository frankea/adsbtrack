# Contributing to adsbtrack

## Development setup

Requires Python 3.12+ and [uv](https://github.com/astral-sh/uv).

```
git clone https://github.com/frankea/adsbtrack.git
cd adsbtrack
uv sync --extra dev
```

## Running tests

```
uv run pytest
```

## Linting and formatting

```
uv run ruff check .
uv run ruff format .
```

Both must pass before merging. CI runs these automatically.

## Type checking

```
uv run mypy adsbtrack
```

Mypy runs in CI but is informational (non-blocking).

## Making changes

1. Create a branch from `main`
2. Make your changes
3. Run `uv run pytest && uv run ruff check . && uv run ruff format --check .`
4. Open a pull request against `main`

## Commit messages

Keep commit messages concise (1-2 sentences). Focus on the "why" rather than the "what". Look at `git log --oneline` for the existing style.

## Code style

- Line length: 120 characters
- Python 3.12+ features are fine (type unions with `|`, etc.)
- Ruff handles formatting and import sorting -- don't fight it
- Prefer editing existing files over creating new ones

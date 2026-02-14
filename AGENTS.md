# Repository Guidelines

## Project Structure & Module Organization

- `s3_overlay/`: library source (`app.py` ASGI app factory, `proxy.py` core proxy logic, `__init__.py` exports).
- `tests/`: pytest suite (unit + integration).
- `pyproject.toml`: package metadata, dependencies, pytest config.
- `README.md`: usage, configuration, and architecture notes.

## Build, Test, and Development Commands

- `uv run litestar --app s3_overlay.app:create_app run --host 0.0.0.0`: run the proxy locally (default port 8000).
- `uv run pytest -v`: run the test suite (uses MinIO containers via fixtures).
- `uv add s3-overlay`: install this package into another project.

## Coding Style & Naming Conventions

- Python 4-space indentation; keep line lengths reasonable and match existing patterns.
- Naming: modules/functions/variables use `snake_case`; classes use `CapWords`.
- No formatter or linter is configured in `pyproject.toml`; avoid large refactors and keep changes minimal and consistent with adjacent code.

## Testing Guidelines

- Framework: `pytest` (see `pyproject.toml` `[tool.pytest.ini_options]`).
- Naming: files `tests/test_*.py`, functions `test_*`.
- Prefer adding tests alongside existing integration coverage; keep tests deterministic and isolate MinIO setup in fixtures.

## Commit & Pull Request Guidelines

- Git history currently contains a single commit (`initial`), so there is no established message convention.
- Use short, imperative commit summaries (e.g., "Add range caching tests").
- PRs should include:
  - A clear summary of behavior changes.
  - Test results (`uv run pytest -v`), or a note explaining why tests were not run.
  - Any new/updated configuration or environment variables.

## Security & Configuration Tips

- Do not commit credentials. Configure S3/MinIO via environment variables (see `README.md`).
- For local-only development, leave remote S3 variables unset to avoid accidental upstream access.

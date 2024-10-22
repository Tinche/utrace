lint:
    uv run ruff format src/ tests/ --quiet
    uv run ruff check src/ tests/ --quiet
    uv run mypy src/ tests/

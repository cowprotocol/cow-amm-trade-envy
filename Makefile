test:
	uv run pytest tests/

format:
	uv run ruff

lint:
	uv run ruff check
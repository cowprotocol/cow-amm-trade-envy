test:
	uv run pytest tests/

format:
	uv run ruff format

lint:
	uv run ruff check
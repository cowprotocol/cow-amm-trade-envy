test:
	uv run pytest tests/

format:
	uv run ruff format

lint:
	uv run ruff check

run-pipeline:
	uv run src/cow_amm_trade_envy/main.py
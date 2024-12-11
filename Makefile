test:
	uv run pytest tests/

format:
	uv run ruff format

lint:
	uv run ruff check

run-dagster:
	uv run dagster dev -w workspace.yaml

render-report:
	uv run src/cow_amm_trade_envy/render_report.py

test:
	uv run pytest tests/

format:
	uv run ruff format

lint:
	uv run ruff check

dagster-run:
	uv run dagster dev -w workspace.yaml

report-render:
	uv run src/cow_amm_trade_envy/render_report.py

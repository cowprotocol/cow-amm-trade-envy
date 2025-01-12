test:
	uv run pytest tests/

format:
	uv run ruff format

lint:
	uv run ruff check

run-pipeline-test:
	uv run src/cow_amm_trade_envy/main.py --time_start '2024-09-27 14:12:23' --time_end '2024-09-27 15:00:23'

run-pipeline-lastweek-usdcweth:
	uv run src/cow_amm_trade_envy/main.py --used_pool_names "['USDC-WETH']" --time_start '2025-01-04 00:00:00' --time_end '2025-01-11 23:59:59'

run-pipeline-lastweek:
	uv run src/cow_amm_trade_envy/main.py --time_start '2025-01-05 00:00:00' --time_end '2025-01-11 23:59:59'


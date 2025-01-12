test:
	uv run pytest tests/

format:
	uv run ruff format

lint:
	uv run ruff check

run-pipeline-test:
	uv run src/cow_amm_trade_envy/main.py --min_block 20842478 --max_block 20842717

run-pipeline-usdcweth:
	uv run src/cow_amm_trade_envy/main.py --used_pool_names "['USDC-WETH']" --min_block 21500000

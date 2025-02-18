RELEASE := 'v0.1.0'

tag-version:
	git diff --exit-code && git diff --staged --exit-code && git tag -a $(RELEASE) -m 'Release $(RELEASE)' || (echo "Repo is dirty please commit first" && exit 1)

test:
	uv run pytest tests/

format:
	uv run ruff format

lint:
	uv run ruff check

dev-run-pipeline-test1:
	uv run src/cow_amm_trade_envy/main.py --time_start '2024-09-27 14:12:23' --time_end '2024-09-27 15:00:11' --dev # dev means the .env is used

dev-run-pipeline-test2:
	uv run src/cow_amm_trade_envy/main.py --time_start '2024-12-28 09:14:47' --time_end '2024-12-28 12:35:47' --dev # dev means the .env is used

dev-run-pipeline-lastweek-usdcweth:
	uv run src/cow_amm_trade_envy/main.py --used_pool_names "['USDC-WETH']" --time_start '2025-01-04 00:00:00' --time_end '2025-01-11 23:59:59' --dev # dev means the .env is used

dev-run-pipeline-lastweek:
	uv run src/cow_amm_trade_envy/main.py --time_start '2025-01-05 00:00:00' --time_end '2025-01-11 23:59:59' --dev # dev means the .env is used

build:
	@echo "Building the docker image"
	docker build -t cow_amm_trade_envy .

run-incremental-ingest-prod:
	@echo "Running the docker image"
	docker run --env-file .env.prod --network host cow_amm_trade_envy --used_pool_names "['USDC-WETH']" --time_start '2025-01-18 00:00:00'

sync-to-dune:
	docker run --rm --network=host   -v "$(pwd)/config.yaml:/app/config.yaml"  --env-file .env   ghcr.io/bh2smith/dune-sync:latest   --jobs envy_to_dune

RELEASE := 'v0.2.0'


update-and-sync:
	make update
	make sync-to-dune

tag-version:
	git diff --exit-code && git diff --staged --exit-code && git tag -a $(RELEASE) -m 'Release $(RELEASE)' || (echo "Repo is dirty please commit first" && exit 1)

test:
	uv run pytest tests/

format:
	uv run ruff format

lint:
	uv run ruff check

dev-run-pipeline-test1:
	uv run src/cow_amm_trade_envy/main.py --network "ethereum" --time_start '2024-09-27 14:12:23' --time_end '2024-09-27 15:00:11'

dev-run-pipeline-test2:
	uv run src/cow_amm_trade_envy/main.py --network "ethereum" --time_start '2024-12-28 09:14:47' --time_end '2024-12-28 12:35:47'

gno:
	uv run src/cow_amm_trade_envy/main.py --time_start '2025-02-15 00:00:00' --network "gnosis"

build:
	docker build -t cow_amm_trade_envy .

run-incremental-ingest-USDCWETH-ETH:
	docker run --env-file .env.prod --network host cow_amm_trade_envy --used_pool_names "['USDC-WETH']" --time_start '2025-01-18 00:00:00' --network "ethereum"

run-incremental-ingest-USDCWETH-GNO:
	docker run --env-file .env.prod --network host cow_amm_trade_envy --used_pool_names "['WETH-GNO']" --time_start '2025-02-15 00:00:00' --network "gnosis"

update:
	docker run --env-file .env.prod --network host cow_amm_trade_envy --time_start '2025-01-18 00:00:00' --network "ethereum"
	docker run --env-file .env.prod --network host cow_amm_trade_envy --time_start '2025-02-15 00:00:00' --network "gnosis"

sync-to-dune:
	docker run --rm --network=host -v "$$(pwd)/dune_sync_config.yaml:/app/config.yaml"  --env-file .env   ghcr.io/bh2smith/dune-sync:latest --jobs envy_to_dune_ethereum envy_to_dune_gnosis

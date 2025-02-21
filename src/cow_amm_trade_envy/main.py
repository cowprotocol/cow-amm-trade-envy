from cow_amm_trade_envy.datasources import DataFetcher
from dotenv import load_dotenv
import os
from cow_amm_trade_envy.envy_calculation import TradeEnvyCalculator
from cow_amm_trade_envy.configs import EnvyCalculatorConfig, DataFetcherConfig, PGConfig
from fire import Fire
import datetime
from cow_amm_trade_envy.models import pools_factory

SUPPORTED_NETWORKS = ["ethereum", "gnosis"]


def main_by_time(
    network: str, time_start: str, time_end: str = None, used_pool_names: list = None
):
    load_dotenv()

    # check that the env vars are set
    with open(".env.example", "r") as f:
        for line in f.readlines():
            var_name = line.split("=")[0]
            if os.getenv(var_name) is None:
                raise ValueError(f"Env var {var_name} is not set.")

    pg_config = PGConfig(postgres_url=os.getenv("DB_URL"))

    data_fetcher = DataFetcher(
        DataFetcherConfig(
            min_block=0,  # just a dummy
            pg_config=pg_config,
            network=network,
        )
    )
    if time_end is None:
        date_end = datetime.datetime.now(datetime.timezone.utc)
        time_end = date_end.strftime("%Y-%m-%d %H:%M:%S")

    print(f"Getting blocks for times {time_start} and {time_end}...")
    min_block = data_fetcher.get_block_number_by_time(time_start)
    max_block = data_fetcher.get_block_number_by_time(time_end)
    print(f"Got blocks {min_block} and {max_block}")

    main(network, min_block, max_block, used_pool_names)


def main(
    network: str, min_block: int, max_block: int = None, used_pool_names: list = None
):
    supported_pools = pools_factory(network).get_pools()

    # make sure there are no duplicates
    supported_pool_names = [x.NAME for x in supported_pools]
    used_pools = []
    if used_pool_names is not None:
        assert len(used_pool_names) == len(set(used_pool_names)), "Duplicate pool names"
        for pool_name in used_pool_names:
            if pool_name not in supported_pool_names:
                raise ValueError(
                    f"Pool {pool_name} is not supported. Possible pools are {supported_pool_names}"
                )
            used_pools.append(supported_pools[supported_pool_names.index(pool_name)])

        if len(used_pools) == 0:
            raise ValueError("No pools selected")
    else:
        used_pools = supported_pools

    config = EnvyCalculatorConfig(network=network)  # DB_FILE

    pg_config = PGConfig(postgres_url=os.getenv("DB_URL"))

    dfc = DataFetcherConfig(
        config.network,
        # min_block=20 * 10 ** 6, max_block=20 * 10 ** 6 + 10000  # todo
        min_block=min_block,  # 21475765,
        max_block=max_block,  # 21525891,
        pg_config=pg_config,
        used_pools=used_pools,
    )

    data_fetcher = DataFetcher(dfc)
    # todo add network config

    # fetch data (from dune)
    data_fetcher.populate_settlement_and_price()
    calculator = TradeEnvyCalculator(config, dfc, used_pools)
    calculator.create_envy_data()


if __name__ == "__main__":
    Fire(main_by_time)
    # load_dotenv()
    # main(21762909, 21762910, used_pool_names=["USDC-WETH"])  # todo remove

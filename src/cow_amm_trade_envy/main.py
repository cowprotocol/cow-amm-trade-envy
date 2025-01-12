from cow_amm_trade_envy.datasources import DataFetcher
from cow_amm_trade_envy.models import Pools
from cow_amm_trade_envy.configs import DataFetcherConfig
from dotenv import load_dotenv
import os
from cow_amm_trade_envy.envy_calculation import TradeEnvyCalculator
from cow_amm_trade_envy.configs import EnvyCalculatorConfig
from cow_amm_trade_envy.render_report import render_report
from fire import Fire
from datetime import datetime

def main_by_time(time_start: str, time_end: str = None, used_pool_names: list = None):

    data_fetcher = DataFetcher(DataFetcherConfig(
        min_block=0,
        network="ethereum",
        node_url=os.getenv("NODE_URL"),
    ))

    print(f"Getting blocks for times {time_start} and {time_end}...")
    min_block = data_fetcher.get_block_number_by_time(time_start)
    max_block = data_fetcher.get_block_number_by_time(time_end)
    print(f"Got blocks {min_block} and {max_block}")

    main(min_block, max_block, used_pool_names)




def main(min_block: int, max_block: int = None, used_pool_names: list = None):
    supported_pools = Pools().get_pools()

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

    config = EnvyCalculatorConfig(network="ethereum")  # DB_FILE

    dfc = DataFetcherConfig(
        config.network,
        node_url=os.getenv("NODE_URL"),
        # min_block=20 * 10 ** 6, max_block=20 * 10 ** 6 + 10000  # todo
        min_block=min_block,  # 21475765,
        max_block=max_block,  # 21525891,
        used_pools=used_pools,
    )

    data_fetcher = DataFetcher(dfc)
    # todo add network config

    # fetch data (from dune)
    data_fetcher.populate_settlement_and_price()
    calculator = TradeEnvyCalculator(config, dfc, used_pools)
    calculator.create_envy_data()

    # generate report
    render_report()


if __name__ == "__main__":
    load_dotenv()
    Fire(main_by_time)
    #main(21500000, None)  # todo remove

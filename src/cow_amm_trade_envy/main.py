from cow_amm_trade_envy.datasources import DataFetcher
from cow_amm_trade_envy.configs import DataFetcherConfig
from dotenv import load_dotenv
import os
from cow_amm_trade_envy.envy_calculation import TradeEnvyCalculator
from cow_amm_trade_envy.configs import EnvyCalculatorConfig
from cow_amm_trade_envy.render_report import render_report


def main():
    config = EnvyCalculatorConfig(network="ethereum", db_file="data.duckdb")  # DB_FILE
    dfc = DataFetcherConfig(
        config.network,
        config.db_file,
        node_url=os.getenv("NODE_URL"),
        # min_block=20 * 10 ** 6, max_block=20 * 10 ** 6 + 10000  # todo
        min_block=20842476,
        max_block=20842716,
    )
    # todo add network config

    # fetch data (from dune)
    data_fetcher = DataFetcher(dfc)
    data_fetcher.populate_settlement_and_price()

    # calculate (and fetch data from local storage or node)
    calculator = TradeEnvyCalculator(config, dfc)
    calculator.create_envy_data()

    # generate report
    render_report()


if __name__ == "__main__":
    load_dotenv()
    main()

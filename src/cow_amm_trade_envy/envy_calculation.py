import json
import pandas as pd
from tqdm import tqdm
from typing import Optional, List, Dict, Any

from cow_amm_trade_envy.configs import EnvyCalculatorConfig, DataFetcherConfig
from cow_amm_trade_envy.models import (
    UCP,
    Pools,
    Tokens,
    trades_from_lists,
    Trade,
    BCowPool,
    CoWAmmOrderData,
)
from cow_amm_trade_envy.datasources import BCoWHelper, DataFetcher
from cow_amm_trade_envy.db_utils import upsert_data


class TradeEnvyCalculator:
    def __init__(
        self,
        config: EnvyCalculatorConfig,
        dfc: DataFetcherConfig,
        used_pools: List[BCowPool] = None,
    ):
        self.config = config
        self.helper = BCoWHelper(dfc)
        self.data_fetcher = DataFetcher(dfc)
        if used_pools is None:
            used_pools = Pools().get_pools()
        self.used_pools = used_pools

    @staticmethod
    def preprocess_row(row: pd.Series) -> pd.Series:
        """Preprocesses a row of settlement data."""
        row = row.copy()
        row["tokens"] = row["tokens"].lower().strip("[]").split()
        row["clearing_prices"] = row["clearing_prices"].lower().strip("[]").split()
        row["trades"] = json.loads(row["trades"].replace(" ", ","))
        row["call_block_number"] = int(row["call_block_number"])
        row["gas_price"] = int(row["gas_price"])
        return row

    def calc_max_cow_sell_amount(
        self,
        pool: BCowPool,
        order: CoWAmmOrderData,
        block_num: int,
        selling_token: Tokens,
        max_cow_amm_buy_amount: int,
        cow_amm_buy_amount: int,
    ) -> int:
        """Calculates maximum amount of selling token that CoW AMM can buy."""
        ratio_filled = max_cow_amm_buy_amount / cow_amm_buy_amount

        if ratio_filled == 1:
            return order.sellAmount

        partial_order = self.helper.order_from_buy_amount(
            pool, selling_token.address, max_cow_amm_buy_amount, block_num
        )

        if partial_order is None:
            return int(order.sellAmount * ratio_filled)

        return partial_order.sellAmount

    def calc_surplus_per_trade(
        self, ucp: UCP, trade: Trade, block_num: int
    ) -> Optional[dict]:

        if block_num == 20842479:
            print("asd") # todo remove

        pool = Pools().get_fitting_pool(trade)
        order = self.helper.order(
            pool=pool,
            prices=[ucp[pool.TOKEN0], ucp[pool.TOKEN1]],
            block_num=block_num,
        )

        # The CoW AMM takes the counterparty of a CoW
        # The helper gives us the trade the CoW AMM would want to make
        order_and_trade_aligned = (
            order.buyToken == trade.sellToken and order.sellToken == trade.buyToken
        )
        if not order_and_trade_aligned:
            return None

        if trade.isOneToZero(pool):
            selling_token, buying_token = pool.TOKEN1, pool.TOKEN0
        elif trade.isZeroToOne(pool):
            selling_token, buying_token = pool.TOKEN0, pool.TOKEN1
        else:
            #  shouldnt even happen when we only use eligible trades
            raise ValueError("Trade not supported")

        # actual calculation
        cow_amm_buy_amount = order.buyAmount
        max_cow_amm_buy_amount = min(trade.sellAmount, cow_amm_buy_amount)

        max_cow_amm_sell_amount = self.calc_max_cow_sell_amount(
            pool,
            order,
            block_num,
            selling_token,
            max_cow_amm_buy_amount,
            cow_amm_buy_amount,
        )

        executed_buy_amount = (
            max_cow_amm_buy_amount * ucp[selling_token] / ucp[buying_token]
        )

        surplus = (
            max_cow_amm_sell_amount - executed_buy_amount
        )  # denominated in buying token

        if trade.isOneToZero(pool):  # make sure its denominated in token1 of the pool
            surplus = surplus * ucp[pool.TOKEN0] / ucp[pool.TOKEN1]

        # make sure its denominated in native token using pre-downloaded prices
        if buying_token != Tokens.native:
            rate_in_wrapped_native = self.data_fetcher.get_token_to_native_rate(
                pool.TOKEN1.address, block_num
            )
            decimal_correction_factor = 10 ** (
                Tokens.native.decimals - pool.TOKEN1.decimals
            )
            surplus = surplus * rate_in_wrapped_native * decimal_correction_factor

        return {"surplus": surplus, "pool": pool.ADDRESS}

    def calc_gas(self, gas_price: int) -> int:
        """Calculates gas cost for a trade."""
        return gas_price * self.config.gas_cost_estimate

    def calc_envy_per_settlement(self, row: pd.Series) -> List[Dict[str, Any]]:
        """Calculates envy for all trades in a settlement."""
        row = self.preprocess_row(row)
        ucp = UCP.from_lists(row["tokens"], row["clearing_prices"])

        settlement_trades = trades_from_lists(
            row["tokens"],
            row["clearing_prices"],
            row["trades"],
            row["call_block_number"],
        )  # todo remove filtering from eligible trades list function and rename
        # Add an index to each trade
        settlement_trades_indexed = list(
            enumerate(settlement_trades)
        )

        # remove all None trades because they are not supported
        eligible_settlement_trades_indexed = [
            (index, trade)
            for index, trade in settlement_trades_indexed
            if trade is not None
        ]

        # could in theory cause issues to match on pool name
        eligible_settlement_trades = [
            (i, trade)
            for i, trade in eligible_settlement_trades_indexed
            if Pools().get_fitting_pool(trade) in self.used_pools
        ]


        envy_list = []
        for i, trade in eligible_settlement_trades:
            surplus_data = self.calc_surplus_per_trade(
                ucp, trade, row["call_block_number"]
            )
            if surplus_data:
                surplus = surplus_data["surplus"]
                pool = surplus_data["pool"]
                gas = self.calc_gas(row["gas_price"])
                trade_envy = (surplus - gas) * 10 ** (-Tokens.native.decimals)
                envy_list.append({"trade_envy": trade_envy, "pool": pool, "trade_index": i})

        return envy_list


    def check_pool_already_used(self, ucp_data: pd.DataFrame):

        def isin_pool(topic: str, pool_address: str) -> bool:
            if pool_address == "None":
                return False
            assert "0x" == pool_address[:2]
            return pool_address[2:] in topic

        def log_is_used(log, pool_address: str) -> bool:
            return any(isin_pool(topic, pool_address) for topic in log["topics"])

        def logs_are_used(logs, pool_address: str) -> bool | None:
            if pool_address is None:
                return False
            return any(log_is_used(log, pool_address) for log in logs)

        # Convert the 'call_tx_hash' column to a list
        call_tx_hashes = ucp_data["call_tx_hash"].tolist()

        # Parallelize the get_logs function
        #with ThreadPoolExecutor(max_workers=12) as executor:
        #    logs_list = list(
        #        tqdm(
        #            executor.map(self.helper.get_logs, call_tx_hashes),
        #            total=len(call_tx_hashes),
        #            desc="Fetching logs",
        #        )
        #    )
        logs_list = self.helper.get_logs_batch(call_tx_hashes)
        ucp_data["logs"] = logs_list

        is_used_list = tqdm([
            logs_are_used(logs, pool_address) for logs, pool_address in list(zip(ucp_data["logs"], ucp_data["pool"]))
        ], desc="Processing is_used")

        ucp_data["is_used"] = is_used_list

        return ucp_data

    def create_envy_data(self):
        with self.data_fetcher.db_manager.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT * FROM trade_envy.{self.config.network}_settle;")
                ucp_data = pd.DataFrame(
                    cursor.fetchall(), columns=[desc[0] for desc in cursor.description]
                )

        ucp_data = ucp_data.sort_values("call_block_number", ascending=False)
        ucp_data.reset_index(drop=True, inplace=True)

        trade_envy_per_settlement = [
            self.calc_envy_per_settlement(row) for _, row in tqdm(ucp_data.iterrows(),
                                                                  total=len(ucp_data),
                                                                desc="Calculating envy for all pools per settlement")
        ]

        df_te = pd.DataFrame({"data": trade_envy_per_settlement,
                              "call_tx_hash": ucp_data["call_tx_hash"]})


        df_te = df_te.explode("data")
        df_te["pool"] = df_te["data"].apply(lambda x: None if pd.isna(x) else x["pool"])
        df_te["trade_envy"] = df_te["data"].apply(
            lambda x: None if pd.isna(x) else x["trade_envy"]
        )
        df_te["trade_index"] = df_te["data"].apply(
            lambda x: None if pd.isna(x) else x["trade_index"]
        )
        #ucp_data["trade_envy"] = df_te["trade_envy"]
        #ucp_data["pool"] = df_te["pool"]

        df_te2 = self.check_pool_already_used(df_te)

        envy_data = pd.DataFrame(
            {
                "call_tx_hash": df_te2["call_tx_hash"],
                "trade_index": [int(x) if pd.notna(x) else -1 for x in df_te2["trade_index"]],
                "pool": ["None" if pd.isna(x) else x for x in df_te2["pool"]],
                "trade_envy": df_te2["trade_envy"],
                "is_used": df_te2["is_used"],
            }
        )

        table_name = f"{self.config.network}_envy"

        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS trade_envy.{table_name} (
        call_tx_hash TEXT,
        trade_index INTEGER,
        pool TEXT,
        trade_envy NUMERIC,
        is_used BOOLEAN,
        PRIMARY KEY (call_tx_hash, trade_index)
        );
        """

        with self.data_fetcher.db_manager.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(create_table_query)
                upsert_data(table_name, envy_data, conn)

        # todo remove in the end
        if self.config.network == "ethereum" and len(self.used_pools) == len(Pools().get_pools()):
            outfile = "data/cow_amm_missed_surplus.csv"
            envy_data.to_csv(outfile)  # keep this for now to see changes in the diffs
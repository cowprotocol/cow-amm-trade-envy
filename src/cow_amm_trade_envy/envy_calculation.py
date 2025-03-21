import json
import pandas as pd
from tqdm import tqdm
from typing import Optional, List, Dict, Any

from cow_amm_trade_envy.configs import EnvyCalculatorConfig, DataFetcherConfig
from cow_amm_trade_envy.models import (
    UCP,
    pools_factory,
    Pools,
    tokens_factory,
    Token,
    trades_from_lists,
    Trade,
    BCowPool,
    CoWAmmOrderData,
)
from cow_amm_trade_envy.datasources import BCoWHelper, DataFetcher
from cow_amm_trade_envy.db_utils import upsert_data
import math


class TradeEnvyCalculator:
    def __init__(
        self,
        config: EnvyCalculatorConfig,
        dfc: DataFetcherConfig,
        used_pool_list: List[BCowPool] = None,
    ):
        self.config = config
        self.helper = BCoWHelper(dfc)
        self.data_fetcher = DataFetcher(dfc)
        self.network_pools: Pools = pools_factory(self.config.network)
        if used_pool_list is None:
            used_pool_list = self.network_pools.get_pools()
        self.used_pool_list: List[BCowPool] = used_pool_list
        self.tokens = tokens_factory(self.config.network)

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
        buying_token_cowamm: Token,
        max_cow_amm_buy_amount: int,
        cow_amm_buy_amount: int,
    ) -> int:
        """Calculates maximum amount of selling token that CoW AMM can buy."""
        ratio_filled = max_cow_amm_buy_amount / cow_amm_buy_amount

        if ratio_filled == 1:
            return order.sellAmount

        partial_order = self.helper.order_from_buy_amount(
            pool, buying_token_cowamm.address, max_cow_amm_buy_amount, block_num
        )

        if partial_order is None:
            return int(order.sellAmount * ratio_filled)

        return partial_order.sellAmount

    def scale_prices(self, ucp_token0, ucp_token1, max_scale=45, min_scale=4) -> tuple:
        # prices are just relative to each other. it may happen that UCPs are too large
        # in that case we want to scale them back. An example that made a problem
        # had UCP sizes of 10**52 and 10**56. To be safe just stay below 10**45 should
        # still be accurate enough. problem block on eth: 22047212

        if ucp_token0 > 10**max_scale or ucp_token1 > 10**max_scale:
            exponent = max_scale - int(math.log(max(ucp_token0, ucp_token1), 10)) - 1
            scaling_factor = 10**exponent

            ucp_scaled_token0 = int(ucp_token0 * scaling_factor)
            ucp_scaled_token1 = int(ucp_token1 * scaling_factor)

            # if either of them is too small, we might have a problem
            if (
                math.log(ucp_scaled_token0, 10) < min_scale
                or math.log(ucp_scaled_token1, 10) < min_scale
            ):
                raise ValueError(
                    "scaled UCP is pretty small. I hoped this wouldnt"
                    "happen but you might be able to lower the min_scale,"
                    "just make sure it is still accurate enough."
                )

            ucp_token0 = ucp_scaled_token0
            ucp_token1 = ucp_scaled_token1

        return ucp_token0, ucp_token1

    def calc_surplus_per_trade(
        self, ucp: UCP, trade: Trade, block_num: int
    ) -> Optional[dict]:
        pool = self.network_pools.get_fitting_pool(trade)
        ucp_token0 = ucp[pool.TOKEN0]
        ucp_token1 = ucp[pool.TOKEN1]

        ucp_scaled_token0, ucp_scaled_token1 = self.scale_prices(ucp_token0, ucp_token1)
        order = self.helper.order(
            pool=pool,
            prices=[ucp_scaled_token0, ucp_scaled_token1],
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

        buying_token_cowamm = selling_token
        max_cow_amm_sell_amount = self.calc_max_cow_sell_amount(
            pool,
            order,
            block_num,
            buying_token_cowamm,
            max_cow_amm_buy_amount,
            cow_amm_buy_amount,
        )

        executed_buy_amount = (
            max_cow_amm_buy_amount * ucp[selling_token] / ucp[buying_token]
        )

        # denominated in buying token
        surplus = max_cow_amm_sell_amount - executed_buy_amount

        if trade.isOneToZero(pool):  # make sure its denominated in token1 of the pool
            surplus = surplus * ucp[pool.TOKEN0] / ucp[pool.TOKEN1]

        # make sure its denominated in native token using pre-downloaded prices
        if pool.TOKEN1 != self.tokens.native:
            rate_in_wrapped_native = self.data_fetcher.get_token_to_native_rate(
                pool.TOKEN1.address, block_num
            )
            decimal_correction_factor = 10 ** (
                self.tokens.native.decimals - pool.TOKEN1.decimals
            )
            surplus = surplus * rate_in_wrapped_native * decimal_correction_factor

        return {"surplus": surplus, "pool": pool.ADDRESS}

    def calc_gas(self, gas_price: int) -> int:
        """Calculates gas cost for a trade."""
        return gas_price * self.config.gas_cost_estimate

    def calc_envy_per_settlement(self, row: pd.Series) -> List[Dict[str, Any]]:
        """Calculates envy for all trades in a settlement."""
        row = self.preprocess_row(row)

        settlement_trades = trades_from_lists(
            row["tokens"],
            row["clearing_prices"],
            row["trades"],
            row["call_block_number"],
            self.config.network,
        )  # todo remove filtering from eligible trades list function and rename

        n_trades = len(settlement_trades)
        ucp = UCP.from_lists(
            row["tokens"],
            row["clearing_prices"],
            n_trades=n_trades,
            native_address=self.tokens.native.address,
        )

        # Add an index to each trade
        settlement_trades_indexed = list(enumerate(settlement_trades))

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
            if self.network_pools.get_fitting_pool(trade) in self.used_pool_list
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
                trade_envy = (surplus - gas) * 10 ** (-self.tokens.native.decimals)
                envy_list.append(
                    {"trade_envy": trade_envy, "pool": pool, "trade_index": i}
                )

        return envy_list

    def check_pool_already_used(self, df: pd.DataFrame):
        def isin_pool(topic: str, pool_address: str) -> bool:
            assert "0x" == pool_address[:2]
            return pool_address[2:] in topic

        def log_is_used(log, pool_address: str) -> bool:
            return any(isin_pool(topic, pool_address) for topic in log["topics"])

        def logs_are_used(logs, pool_address: str) -> bool | None:
            if pool_address is None:
                return False
            return any(log_is_used(log, pool_address) for log in logs)

        # Convert the 'call_tx_hash' column to a list
        mask_poolnotna = [not pd.isna(x) for x in df["pool"]]
        call_tx_hashes = list(set(df["call_tx_hash"][mask_poolnotna].tolist()))

        logs_list = self.helper.get_logs_batch(call_tx_hashes)
        tx_hash_to_logs = dict(zip(call_tx_hashes, logs_list))
        df["logs"] = df["call_tx_hash"].apply(lambda x: tx_hash_to_logs.get(x))

        df["pool_used_already"] = None
        df.loc[mask_poolnotna, "pool_used_already"] = [
            logs_are_used(logs, pool_address)
            for logs, pool_address in list(
                zip(df["logs"][mask_poolnotna], df["pool"][mask_poolnotna])
            )
        ]

        return df

    def create_envy_data(self):
        table_name = f"{self.config.network}_envy"
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS trade_envy.{table_name} (
            call_tx_hash TEXT,
            block_number INTEGER,
            block_time TIMESTAMP,
            trade_index INTEGER,
            pool TEXT,
            pool_name TEXT,
            solver TEXT,
            trade_envy NUMERIC,
            pool_used_already BOOLEAN,
            PRIMARY KEY (call_tx_hash, trade_index)
        );
        """

        with self.data_fetcher.db_manager.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(create_table_query)
                cursor.execute(f"""
                                SELECT settle.* 
                                FROM trade_envy.{self.config.network}_settle AS settle
                                LEFT JOIN trade_envy.{table_name} AS envy
                                ON settle.call_tx_hash = envy.call_tx_hash
                                WHERE envy.call_tx_hash IS NULL;
                            """)
                ucp_data = pd.DataFrame(
                    cursor.fetchall(), columns=[desc[0] for desc in cursor.description]
                )

        ucp_data = ucp_data.sort_values("call_block_number", ascending=False)
        ucp_data.reset_index(drop=True, inplace=True)
        trade_envy_per_settlement = [
            self.calc_envy_per_settlement(row)
            for _, row in tqdm(
                ucp_data.iterrows(),
                total=len(ucp_data),
                desc="Calculating envy for all pools per settlement (including helper query)",
            )
        ]

        df_envy = pd.DataFrame(
            {
                "data": trade_envy_per_settlement,
                "call_tx_hash": ucp_data["call_tx_hash"],
            }
        )
        df_envy = df_envy.explode("data")
        df_envy["pool"] = df_envy["data"].apply(
            lambda x: None if pd.isna(x) else x["pool"]
        )
        df_envy["trade_envy"] = df_envy["data"].apply(
            lambda x: None if pd.isna(x) else x["trade_envy"]
        )
        df_envy["trade_index"] = df_envy["data"].apply(
            lambda x: None if pd.isna(x) else x["trade_index"]
        )
        df_envy = self.check_pool_already_used(df_envy)

        # Merge the solver column from the settlement (ucp_data) into the envy DataFrame.
        df_envy = df_envy.merge(
            ucp_data[
                ["call_tx_hash", "solver", "call_block_number", "call_block_time"]
            ],
            on="call_tx_hash",
            how="left",
        )

        # Compute the pool_name based on the pool address, if available.
        df_envy["pool_name"] = df_envy["pool"].apply(
            lambda x: self.network_pools.get_name_from_address(x)
            if pd.notna(x)
            else None
        )

        envy_data = pd.DataFrame(
            {
                "call_tx_hash": df_envy["call_tx_hash"],
                "block_number": df_envy["call_block_number"],
                "block_time": df_envy["call_block_time"],
                "trade_index": [
                    int(x) if pd.notna(x) else -1 for x in df_envy["trade_index"]
                ],
                "pool": df_envy["pool"],
                "pool_name": df_envy["pool_name"],
                "solver": df_envy["solver"],
                "trade_envy": df_envy["trade_envy"],
                "pool_used_already": df_envy["pool_used_already"],
            }
        )

        with self.data_fetcher.db_manager.connect() as conn:
            upsert_data(table_name, envy_data, conn)

        # todo remove in the end
        if self.config.network == "ethereum" and len(self.used_pool_list) == len(
            pools_factory(self.config.network).get_pools()
        ):
            outfile = "data/cow_amm_missed_surplus.csv"
            envy_data.to_csv(outfile)  # keep this for now to see changes in the diffs

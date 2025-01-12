import os

from cow_amm_trade_envy.configs import DataFetcherConfig
from typing import Optional, List, Tuple, Any
from dotenv import load_dotenv
from web3 import Web3
from web3.types import HexBytes
from web3.datastructures import AttributeDict
import json
import pandas as pd
import polars as pl
import psycopg2
from psycopg2.extras import execute_values
import spice
from tqdm import tqdm
from tenacity import retry, stop_after_attempt, wait_fixed
from logging import warning

from cow_amm_trade_envy.constants import (
    BCOW_FULL_COW_HELPER_ABI,
    BCOW_PARTIAL_COW_HELPER_ABI,
)
from cow_amm_trade_envy.models import CoWAmmOrderData, BCowPool, Tokens, Token
from cow_amm_trade_envy.db_utils import upsert_data


class Web3Helper:
    def __init__(self, node_url: str):
        self.w3 = Web3(Web3.HTTPProvider(node_url, request_kwargs={"timeout": 60}))

    def get_block_number(self) -> int:
        return self.w3.eth.get_block_number()

    def to_checksum_address(self, address: str) -> str:
        return self.w3.to_checksum_address(address)


class DatabaseManager:
    def __init__(self, default_min_block_number: int):
        self.default_min_block_number = default_min_block_number
        self.initialize_tables()

    def connect(self):
        # Get connection params from environment variables
        db_params = {
            "dbname": os.getenv("DB_NAME"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "host": os.getenv("DB_HOST"),
            "port": os.getenv("DB_PORT"),
        }
        return psycopg2.connect(**db_params)

    def initialize_tables(self):
        with self.connect() as conn, conn.cursor() as cursor:
            cursor.execute(
                f"""
                CREATE SCHEMA IF NOT EXISTS trade_envy;
                """
            )

            cursor.execute("""
            CREATE TABLE IF NOT EXISTS trade_envy.order_cache (
                key TEXT PRIMARY KEY,
                response TEXT
            );
            """)
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS trade_envy.receipt_cache (
                key TEXT PRIMARY KEY,
                response TEXT
            );
            """)
            conn.commit()

    def get_cached_order(self, cache_key: str) -> Optional[str]:
        with self.connect() as conn, conn.cursor() as cursor:
            cursor.execute(
                "SELECT response FROM trade_envy.order_cache WHERE key = %s",
                (cache_key,),
            )
            result = cursor.fetchone()
        return result[0] if result else None

    def cache_order(self, cache_key: str, response: str):
        query = """
        INSERT INTO trade_envy.order_cache (key, response)
        VALUES (%s, %s)
        ON CONFLICT (key) DO UPDATE SET response = EXCLUDED.response;
        """
        with self.connect() as conn, conn.cursor() as cursor:
            cursor.execute(query, (cache_key, response))
            conn.commit()

    def get_last_block_ingested(self, table_name: str, block_col_name: str) -> int:
        query = f"SELECT MAX({block_col_name}) FROM trade_envy.{table_name}"
        with self.connect() as conn, conn.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchone()
        return self.default_min_block_number if result[0] is None else int(result[0])


class BCoWHelper:
    def __init__(self, config: DataFetcherConfig):
        self.config = config
        self.w3_helper = Web3Helper(config.node_url)
        self.db_manager = DatabaseManager(config.min_block)

        # Full cow contract setup
        self.address_full_cow = "0x3FF0041A614A9E6Bf392cbB961C97DA214E9CB31"
        self.contract_full_cow = self.w3_helper.w3.eth.contract(
            address=self.address_full_cow, abi=BCOW_FULL_COW_HELPER_ABI
        )

        # Partial cow contract setup
        self.address_partial_cow = self.w3_helper.to_checksum_address(
            "0x03362f847b4fabc12e1ce98b6b59f94401e4588e"
        )
        self.contract_partial_cow = self.w3_helper.w3.eth.contract(
            address=self.address_partial_cow, abi=BCOW_PARTIAL_COW_HELPER_ABI
        )
        self.contract_partial_cow_deployment = 20963124

    @staticmethod
    def json_serializer(obj: Any) -> Any:
        if isinstance(obj, (HexBytes, bytes)):
            return obj.hex()
        if isinstance(obj, AttributeDict):
            return dict(obj)
        if isinstance(obj, dict):
            return {k: BCoWHelper.json_serializer(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [BCoWHelper.json_serializer(item) for item in obj]
        if isinstance(obj, (int, float, str, bool, type(None))):
            return obj
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

    def query_contract(
        self, contract_function: Any, pool: BCowPool, params: dict, block_num: int
    ) -> list:
        fun = contract_function(pool.checksum_address, **params)
        response = fun.call(block_identifier=block_num)
        return self.json_serializer(response)

    def fetch_from_cache_or_query(
        self, contract_function: Any, pool: BCowPool, params: dict, block_num: int
    ) -> Any:
        cache_key = f"{self.config.network}_{contract_function.address}_{contract_function.abi_element_identifier}_{pool}_{json.dumps(params)}_{block_num}"

        cached_response = self.db_manager.get_cached_order(cache_key)
        if cached_response:
            return json.loads(cached_response)

        response = self.query_contract(contract_function, pool, params, block_num)
        self.db_manager.cache_order(cache_key, json.dumps(response))
        return response

    def order(self, pool: BCowPool, prices: list, block_num: int) -> CoWAmmOrderData:
        contract_function = self.contract_full_cow.functions.order
        response = self.fetch_from_cache_or_query(
            contract_function, pool, {"prices": prices}, block_num
        )
        order, _, _, _ = response
        return CoWAmmOrderData.from_order_response(order)

    def order_from_buy_amount(
        self, pool: BCowPool, buy_token: str, buy_amount: int, block_num: int
    ) -> Optional[CoWAmmOrderData]:
        if block_num <= self.contract_partial_cow_deployment:
            return None

        contract_function = self.contract_partial_cow.functions.orderFromBuyAmount
        params = {
            "buyAmount": buy_amount,
            "buyToken": self.w3_helper.to_checksum_address(buy_token),
        }
        response = self.fetch_from_cache_or_query(
            contract_function, pool, params, block_num
        )
        order, _, _, _ = response
        return CoWAmmOrderData.from_order_response(order)

    # def get_logs(self, tx_hash: str):
    #    """Fetch logs from cache or blockchain."""
    #    cache_key = f"{self.config.network}_{tx_hash}"
    #    with self.db_manager.connect() as conn, conn.cursor() as cursor:
    #        cursor.execute(
    #            "SELECT response FROM trade_envy.receipt_cache WHERE key = %s",
    #            (cache_key,),
    #        )
    #        result = cursor.fetchone()
    #
    #    if result:
    #        return json.loads(result[0])
    #    else:
    #        receipt = self.w3_helper.w3.eth.get_transaction_receipt(tx_hash)
    #        logs = json.dumps(receipt["logs"], default=self.json_serializer)
    #        df_insert = pd.DataFrame({"key": [cache_key], "response": [logs]})
    #        with self.db_manager.connect() as conn:
    #            # doesnt need to be upsert but shouldnt hurt
    #            upsert_data("receipt_cache", df_insert, conn)
    #        return json.loads(logs)

    def get_logs_batch(self, tx_hashes: List[str]):
        """Fetch logs for a batch of transaction hashes from cache or blockchain."""
        cache_keys = [f"{self.config.network}_{tx_hash}" for tx_hash in tx_hashes]

        print(f"Fetching logs for {len(tx_hashes)} tx_hashes")
        # Fetch all relevant cache entries in one query
        with self.db_manager.connect() as conn, conn.cursor() as cursor:
            cursor.execute(
                "SELECT key, response FROM trade_envy.receipt_cache WHERE key = ANY(%s)",
                (cache_keys,),
            )
            cached_results = cursor.fetchall()

        # Convert cached results to a dictionary for quick lookup
        cache_dict = {row[0]: json.loads(row[1]) for row in cached_results}

        # Identify tx_hashes not found in the cache
        uncached_keys = [
            cache_key for cache_key in cache_keys if cache_key not in cache_dict
        ]

        uncached_logs = []

        if uncached_keys:
            # Fetch uncached logs from the blockchain
            uncached_tx_hashes = [
                tx_hashes[cache_keys.index(key)] for key in uncached_keys
            ]

            for tx_hash in tqdm(
                uncached_tx_hashes, desc="Fetching logs from blockchain"
            ):
                receipt = self.w3_helper.w3.eth.get_transaction_receipt(tx_hash)
                logs = json.dumps(receipt["logs"], default=self.json_serializer)
                uncached_logs.append((f"{self.config.network}_{tx_hash}", logs))

            # Insert the uncached logs into the cache
            if uncached_logs:
                df_insert = pd.DataFrame(uncached_logs, columns=["key", "response"])
                with self.db_manager.connect() as conn:
                    upsert_data("receipt_cache", df_insert, conn)

            # Update cache_dict with the newly fetched logs
            cache_dict.update({key: json.loads(logs) for key, logs in uncached_logs})

        # Return the logs in the same order as the input tx_hashes
        result_logs = [
            cache_dict[f"{self.config.network}_{tx_hash}"] for tx_hash in tx_hashes
        ]
        return result_logs


class DataFetcher:
    def __init__(self, config: DataFetcherConfig):
        self.config = config
        self.db_manager = DatabaseManager(config.min_block)
        self.w3_helper = Web3Helper(config.node_url)

    def create_settlement_table(self):
        table_name = f"{self.config.network}_settle"
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS trade_envy.{table_name} (
            call_tx_hash TEXT PRIMARY KEY,
            contract_address TEXT,
            call_success BOOLEAN,
            call_trace_address TEXT,
            call_block_time TIMESTAMP,
            call_block_number INTEGER,
            tokens TEXT,
            clearing_prices TEXT,
            trades TEXT,
            interactions TEXT,
            gas_price BIGINT,
            solver TEXT
        );
        """
        with self.db_manager.connect() as conn, conn.cursor() as cursor:
            cursor.execute(create_table_query)
            conn.commit()

    def populate_settlement_table_by_blockrange(self, start_block: int, end_block: int):
        if start_block > end_block:
            return

        self.create_settlement_table()
        table_name = f"{self.config.network}_settle"
        splits = self.split_intervals(
            start_block, end_block, self.config.interval_length_settle
        )

        dfs = []
        for left, right in tqdm(
            splits,
            desc=f"Fetching settlement data from {start_block} to {end_block}",
            unit="interval",
        ):
            params = {
                "start_block": left,
                "end_block": right,
                "network": self.config.network,
            }
            df = self.query_dune_data(self.config.dune_query_settle, params)
            dfs.append(df)

        if dfs:
            df = pl.concat(dfs).to_pandas()
            df["gas_price"] = df["gas_price"].astype(int)

            with self.db_manager.connect() as conn:
                upsert_data(table_name, df, conn)

    def get_highest_block(self) -> int:
        if self.config.network == "ethereum":
            highest_block = (
                self.w3_helper.get_block_number()
                - self.config.backoff_blocks[self.config.network]
            )
            if self.config.max_block is None:
                return highest_block
            else:
                return min(highest_block, self.config.max_block)
        raise ValueError(f"Network {self.config.network} not supported")

    @staticmethod
    def split_intervals(
        beginning_block: int, current_block: int, interval_len: int
    ) -> List[Tuple[int, int]]:
        return [
            (start, min(start + interval_len - 1, current_block))
            for start in range(beginning_block, current_block + 1, interval_len)
        ]

    @retry(stop=stop_after_attempt(5), wait=wait_fixed(2))
    def query_dune_data(self, query_nr: int, parameters: dict) -> pl.DataFrame:
        return spice.query(query_nr, parameters=parameters, verbose=False)

    def populate_settlement_table(self):
        self.create_settlement_table()
        table_name = f"{self.config.network}_settle"

        current_block = self.get_highest_block()
        beginning_block = (
            self.db_manager.get_last_block_ingested(table_name, "call_block_number") + 1
        )

        self.populate_settlement_table_by_blockrange(beginning_block, current_block)

    def populate_price_tables(self):
        used_pools = self.config.used_pools
        pool_tokens = [pool.TOKEN0 for pool in used_pools] + [
            pool.TOKEN1 for pool in used_pools
        ]
        tokens_to_query = list(set(pool_tokens))

        for token in tokens_to_query:
            self.populate_price_table(token)

    def populate_price_tables_by_blockrange(self, start_block: int, end_block: int):
        for token in Tokens.tokens:
            self.populate_price_table_by_blockrange(token, start_block, end_block)

    def create_price_table(self, token_address: str):
        table_name = f"{self.config.network}_{token_address}_price"
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS trade_envy.{table_name} (
            block_number INTEGER PRIMARY KEY,
            price NUMERIC
        );
        """
        with self.db_manager.connect() as conn, conn.cursor() as cursor:
            cursor.execute(create_table_query)
            conn.commit()

    def populate_price_table(self, token: Token):
        token_address = token.address
        table_name = f"{self.config.network}_{token_address}_price"

        self.create_price_table(token_address)

        current_block = self.get_highest_block()
        beginning_block = (
            self.db_manager.get_last_block_ingested(table_name, "block_number") + 1
        )

        self.populate_price_table_by_blockrange(token, beginning_block, current_block)

    def populate_price_table_by_blockrange(
        self, token: Token, start_block: int, end_block: int
    ):
        if start_block > end_block:
            return
        token_address = token.address
        self.create_price_table(token_address)

        table_name = f"{self.config.network}_{token_address}_price"
        splits = self.split_intervals(
            start_block, end_block, self.config.interval_length_price
        )

        dfs = []
        for left, right in tqdm(
            splits,
            desc=f"Fetching {token.name} price from {start_block} to {end_block}",
            unit="interval",
        ):
            params = {
                "contract_address": token_address,
                "network": self.config.network,
                "start_block": left,
                "end_block": right,
            }
            df = self.query_dune_data(self.config.dune_query_price, params)
            dfs.append(df)

        if dfs:
            df = pl.concat(dfs).to_pandas()

            if df["price"].isna().sum() == len(df):
                warning(
                    f"All NaNs in {token.name} price. Not writing new price data for this token"
                )
                return

            df["price"] = df["price"].astype(float)
            df["price"] = df["price"].interpolate(method="linear")
            if df["price"].isna().sum() > 0:
                warning(
                    f"NaNs in {token.name} price: {df['price'].isna().sum()}/{len(df)}. Interpolating"
                )

            with self.db_manager.connect() as conn:
                upsert_data(table_name, df, conn)

    def get_token_to_native_rate(
        self, token_address: str, block_number: int
    ) -> float | None:
        network = self.config.network
        table_name = f"{network}_{token_address}_price"
        native = Tokens.native.address
        with self.db_manager.connect() as conn, conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT price 
                FROM trade_envy.{table_name} 
                WHERE block_number <= %s 
                ORDER BY block_number DESC 
                LIMIT 1
                """,
                (block_number,),
            )
            res = cursor.fetchone()

            # Query for the native token price
            cursor.execute(
                f"""
                SELECT price 
                FROM trade_envy.{network}_{native}_price 
                WHERE block_number <= %s 
                ORDER BY block_number DESC 
                LIMIT 1
                """,
                (block_number,),
            )
            native_price_res = cursor.fetchone()

        if res is None or native_price_res is None:
            return None

        price = float(res[0])  # usd/token
        native_price = float(native_price_res[0])  # usd/native

        # native/token = usd/token  * 1/(native/usd)
        token_to_native_rate = price / native_price

        return token_to_native_rate

    def populate_settlement_and_price(self):
        self.populate_settlement_table()
        self.populate_price_tables()


def main():
    load_dotenv()

    config = DataFetcherConfig(
        network="ethereum",
        node_url=os.getenv("NODE_URL"),
    )

    fetcher = DataFetcher(config)
    fetcher.populate_settlement_and_price()


if __name__ == "__main__":
    main()

import os
from dotenv import load_dotenv
from web3 import Web3
from web3.types import HexBytes
from web3.datastructures import AttributeDict
from cow_amm_trade_envy.constants import BCOW_HELPER_ABI
from cow_amm_trade_envy.models import CoWAmmOrderData
import json
import pandas as pd
import spice
from tqdm import tqdm
import polars as pl
import duckdb
from tenacity import retry, stop_after_attempt, wait_fixed
from cow_amm_trade_envy.db_utils import upsert_data
from cow_amm_trade_envy.constants import DB_FILE

load_dotenv()


QUERY_NR = 4448838
INTERVAL_LENGTH = 10_000
supported_networks = ["ethereum"]
node_url = os.getenv("NODE_URL")
w3 = Web3(Web3.HTTPProvider(node_url, request_kwargs={"timeout": 60}))


with duckdb.connect(database=DB_FILE) as conn:
    conn.execute("""
    CREATE TABLE IF NOT EXISTS order_cache (
        key TEXT PRIMARY KEY,
        response TEXT
    )
    """)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS receipt_cache (
        key TEXT PRIMARY KEY,
        response TEXT
    )
    """)


def json_serializer(obj):
    """Custom serializer to handle complex objects."""
    if isinstance(obj, (HexBytes, bytes)):
        return obj.hex()
    if isinstance(obj, AttributeDict):
        return dict(obj)
    if isinstance(obj, dict):
        return {k: json_serializer(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [json_serializer(item) for item in obj]
    if isinstance(obj, (int, float, str, bool, type(None))):
        return obj
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def query(pool: str, prices: list, block_num: int, contract) -> list:
    """Fetch data from blockchain."""
    fun = contract.functions.order(pool, prices)
    response = fun.call(block_identifier=block_num)
    order, preInteractions, postInteractions, sig = response
    return json_serializer(order)


def fetch_from_cache_or_query(pool: str, prices: list, block_num: int | None, contract):
    """Fetch data from DuckDB cache or query the blockchain."""
    cache_key = f"{pool}_{prices}_{block_num}"

    with duckdb.connect(database=DB_FILE) as conn:
        result = conn.execute(
            f"SELECT response FROM order_cache WHERE key = '{cache_key}'"
        ).fetchone()

    if result:
        return json.loads(result[0])
    else:
        order_data = query(pool, prices, block_num, contract)
        df_insert = pd.DataFrame(
            {"key": [cache_key], "response": [json.dumps(order_data)]}
        )
        with duckdb.connect(database=DB_FILE) as conn:
            # doesnt need to be upsert but shouldnt hurt
            upsert_data("order_cache", df_insert, conn)
        return order_data


class BCoWHelper:
    """Helper class for interacting with the blockchain contract."""

    def __init__(self):
        self.address = "0x3FF0041A614A9E6Bf392cbB961C97DA214E9CB31"  # todo different on other chains
        self.abi = BCOW_HELPER_ABI
        self.contract = w3.eth.contract(address=self.address, abi=self.abi)

    def order(
        self, pool: str, prices: list, block_num: int | None = None
    ) -> CoWAmmOrderData:
        order = fetch_from_cache_or_query(pool, prices, block_num, self.contract)
        return CoWAmmOrderData.from_order_response(order)


def get_logs(tx_hash: str):
    """Fetch logs from DuckDB cache or blockchain."""
    cache_key = tx_hash

    with duckdb.connect(database=DB_FILE) as conn:
        result = conn.execute(
            f"SELECT response FROM receipt_cache WHERE key = '{cache_key}'"
        ).fetchone()

    if result:
        return json.loads(result[0])
    else:
        receipt = w3.eth.get_transaction_receipt(tx_hash)
        logs = json.dumps(receipt["logs"], default=json_serializer)
        df_insert = pd.DataFrame({"key": [cache_key], "response": [logs]})
        with duckdb.connect(database=DB_FILE) as conn:
            # doesnt need to be upsert but shouldnt hurt
            upsert_data("receipt_cache", df_insert, conn)
        return json.loads(logs)


def get_highest_block(network: str) -> int:
    if network not in supported_networks:
        raise ValueError(
            f"Network {network} not supported. Supported networks are {supported_networks}"
        )

    highest_block = 20842716
    return highest_block


def get_last_block_ingested(network: str) -> int:
    if network not in supported_networks:
        raise ValueError(
            f"Network {network} not supported. Supported networks are {supported_networks}"
        )

    final_block_ingested = 20842476
    return final_block_ingested


def split_intervals(
    beginning_block: int, current_block: int, interval_len: int
) -> list:
    splits = []
    for start in range(beginning_block, current_block + 1, interval_len):
        end = min(start + interval_len - 1, current_block)
        splits.append((start, end))
    return splits


@retry(stop=stop_after_attempt(5), wait=wait_fixed(2))
def query_settle_data(network: str, left: int, right: int) -> pl.DataFrame:
    df = spice.query(
        QUERY_NR,
        parameters={"start_block": left, "end_block": right, "network": network},
    )
    return df


def populate_settlement_table(network: str):
    current_block = get_highest_block(network)
    beginning_block = get_last_block_ingested(network) + 1
    splits = split_intervals(beginning_block, current_block, INTERVAL_LENGTH)

    dfs = []
    for left, right in tqdm(splits):
        print(f"Fetching {left} to {right}")
        df = query_settle_data(network, left, right)
        dfs.append(df)

    df = pl.concat(dfs)
    df = df.to_pandas()
    df["gas_price"] = df["gas_price"].astype(int)

    table_name = f"{network}_settle"

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        call_tx_hash TEXT PRIMARY KEY,
        contract_address TEXT,
        call_success BOOLEAN,
        call_trace_address TEXT,
        call_block_time TIMESTAMP,
        call_block_number INTEGER,
        tokens TEXT,
        clearingPrices TEXT,
        trades TEXT,
        interactions TEXT,
        gas_price BIGINT
    );
    """

    with duckdb.connect(database=DB_FILE) as conn:
        conn.execute(create_table_query)
        upsert_data(table_name, df, conn)


if __name__ == "__main__":
    network_in_use = "ethereum"
    populate_settlement_table(network_in_use)

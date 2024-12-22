import os
import duckdb
from dotenv import load_dotenv
from web3 import Web3
from web3.types import HexBytes
from web3.datastructures import AttributeDict
from cow_amm_trade_envy.constants import BCOW_HELPER_ABI
from cow_amm_trade_envy.models import CoWAmmOrderData
import json
import pandas as pd
from cow_amm_trade_envy.db_utils import upsert_data
from cow_amm_trade_envy.constants import DB_FILE
load_dotenv()

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
        df_insert = pd.DataFrame({
            "key": [cache_key],
            "response": [json.dumps(order_data)]
        })
        with duckdb.connect(database=DB_FILE) as conn:
            # doesnt need to be upsert but shouldnt hurt
            upsert_data("order_cache", df_insert, conn)
        return order_data


class BCoWHelper:
    """Helper class for interacting with the blockchain contract."""

    def __init__(self):
        self.address = "0x3FF0041A614A9E6Bf392cbB961C97DA214E9CB31"
        self.abi = BCOW_HELPER_ABI
        self.contract = w3.eth.contract(address=self.address, abi=self.abi)

    def order(self, pool: str, prices: list, block_num: int | None = None) -> CoWAmmOrderData:
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
        df_insert = pd.DataFrame({
            "key": [cache_key],
            "response": [logs]
        })
        with duckdb.connect(database=DB_FILE) as conn:
            # doesnt need to be upsert but shouldnt hurt
            upsert_data("receipt_cache", df_insert, conn)
        return json.loads(logs)

if __name__ == "__main__":
    bcow_helper = BCoWHelper()

    # Example usage
    tx_hash = "0x5381141986041a1b42931e5e37bdab3cce7672a830914390c6da18a477ca930b"
    logs = get_logs(tx_hash)
    print(logs)

import os
from dotenv import load_dotenv
from web3 import Web3
from web3.types import HexBytes
from web3.datastructures import AttributeDict
from cow_amm_trade_envy.constants import BCOW_HELPER_ABI
from cow_amm_trade_envy.models import CoWAmmOrderData, BCowPool, Tokens
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

backoff_blocks = {
    "ethereum": 1800  # dune price data is ingested hourly, for good measure 6h backoff
}


QUERY_NR_SETTLE = 4448838
QUERY_NR_PRICE = 4468197
INTERVAL_LENGTH_SETTLE = 10_000
INTERVAL_LENGTH_PRICE = 100_000
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


def query(pool: BCowPool, prices: list, block_num: int, contract) -> list:
    """Fetch data from blockchain."""
    fun = contract.functions.order(pool.checksum_address, prices)
    response = fun.call(block_identifier=block_num)
    order, preInteractions, postInteractions, sig = response
    return json_serializer(order)


def fetch_from_cache_or_query(
    pool: BCowPool, prices: list, block_num: int | None, contract
):
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
        self, pool: BCowPool, prices: list, block_num: int | None = None
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
    if network == "ethereum":
        highest_block = w3.eth.get_block_number() - backoff_blocks[network]
    else:
        raise ValueError(f"Network {network} not supported")

    highest_block = 20842716  # todo remove this
    return highest_block


def get_last_block_ingested(
    conn: duckdb.DuckDBPyConnection, table_name: str, block_col_name: str
) -> int:
    res = conn.query(f"SELECT MAX({block_col_name}) FROM {table_name}")
    final_block_ingested = res.fetchdf().iloc[0, 0]
    if pd.isna(final_block_ingested):
        final_block_ingested = 20842476  # todo define default minimum block somewhere
    else:
        final_block_ingested = int(final_block_ingested)

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
def query_dune_data(query_nr: int, parameters: dict) -> pl.DataFrame:
    df = spice.query(query_nr, parameters=parameters)
    return df


def populate_settlement_table(network: str):
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
        gas_price BIGINT,
        solver TEXT,
    );
    """

    with duckdb.connect(database=DB_FILE) as conn:
        conn.execute(create_table_query)
        current_block = get_highest_block(network)
        beginning_block = (
            get_last_block_ingested(conn, table_name, "call_block_number") + 1
        )

    if beginning_block > current_block:
        return

    splits = split_intervals(beginning_block, current_block, INTERVAL_LENGTH_SETTLE)
    dfs = []

    for left, right in tqdm(splits):
        print(f"Fetching {left} to {right}")
        params = {"start_block": left, "end_block": right, "network": network}
        df = query_dune_data(QUERY_NR_SETTLE, params)
        dfs.append(df)

    df = pl.concat(dfs)
    df = df.to_pandas()
    df["gas_price"] = df["gas_price"].astype(int)

    with duckdb.connect(database=DB_FILE) as conn:
        upsert_data(table_name, df, conn)


def populate_price_table(network: str, token_address: str):
    table_name = f"{network}_{token_address}_price"

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        block_number INTEGER PRIMARY KEY,
        price NUMERIC
    );
    """

    with duckdb.connect(database=DB_FILE) as conn:
        conn.execute(create_table_query)
        current_block = get_highest_block(network)
        beginning_block = get_last_block_ingested(conn, table_name, "block_number") + 1

    if beginning_block > current_block:
        return

    splits = split_intervals(beginning_block, current_block, INTERVAL_LENGTH_PRICE)
    dfs = []

    for left, right in tqdm(splits):
        print(f"Fetching {left} to {right}")
        params = {
            "contract_address": token_address,
            "network": network,
            "start_block": left,
            "end_block": right,
        }
        df = query_dune_data(QUERY_NR_PRICE, params)

        dfs.append(df)

    df = pl.concat(dfs)
    df = df.to_pandas()

    with duckdb.connect(database=DB_FILE) as conn:
        upsert_data(table_name, df, conn)

def get_token_to_native_rate(network: str, token_address: str, block_number: int) -> float | None:
    table_name = f"{network}_{token_address}_price"
    native = Tokens.native.address

    with duckdb.connect(database=DB_FILE) as conn:
        res = conn.query(f"SELECT price FROM {table_name} WHERE block_number <= {block_number} ORDER BY block_number DESC LIMIT 1").fetchone()
        native_price_res = conn.query(f"SELECT price FROM {network}_{native}_price WHERE block_number <= {block_number} ORDER BY block_number DESC LIMIT 1").fetchone()

    if res is None or native_price_res is None:
        return None

    price = float(res[0]) # usd/token
    native_price = float(native_price_res[0]) # usd/native

    # native/token = usd/token  * 1/(native/usd)
    token_to_native_rate = price / native_price

    return token_to_native_rate



def populate_price_tables(network: str):
    for token in tqdm(Tokens.tokens):
        populate_price_table(network, token.address)


if __name__ == "__main__":
    network_in_use = "ethereum"
    populate_settlement_table(network_in_use)
    populate_price_tables(network_in_use)

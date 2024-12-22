import spice
import dotenv
from tqdm import tqdm
import polars as pl
import sqlite3
from tenacity import retry, stop_after_attempt, wait_fixed

dotenv.load_dotenv()

query_nr = 4448838
interval = 10_000

dfs = []
supported_networks = ["ethereum"]

network = "ethereum"  # todo make this adjustable


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


current_block = get_highest_block(network)
beginning_block = get_last_block_ingested(network) + 1


def split_intervals(beginning_block: int, current_block: int, interval: int) -> list:
    splits = []
    for start in range(beginning_block, current_block + 1, interval):
        end = min(start + interval - 1, current_block)
        splits.append((start, end))
    return splits


splits = split_intervals(beginning_block, current_block, interval)


@retry(stop=stop_after_attempt(5), wait=wait_fixed(2))
def query_settle_data(query_nr: int, left: int, right: int):
    df = spice.query(query_nr, parameters={"start_block": left, "end_block": right})
    return df


for left, right in tqdm(splits):
    print(f"Fetching {left} to {right}")
    df = query_settle_data(query_nr, left, right)
    dfs.append(df)


df = pl.concat(dfs)

DB_FILE = "data.db"
conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

create_table_query = f"""
CREATE TABLE IF NOT EXISTS {network}_settle (
    call_tx_hash BLOB PRIMARY KEY,
    contract_address BLOB,
    call_success BOOLEAN,
    call_trace_address TEXT,
    call_block_time TIMESTAMP,
    call_block_number INTEGER,
    tokens TEXT,
    clearingPrices TEXT,
    trades TEXT,
    interactions TEXT,
    gas_price INTEGER
);
"""

upsert_query = f"""
INSERT OR REPLACE INTO {network}_settle (
    call_tx_hash,
    contract_address,
    call_success,
    call_trace_address,
    call_block_time,
    call_block_number,
    tokens,
    clearingPrices,
    trades,
    interactions,
    gas_price
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""

# make sure data to insert has the right column order
cursor.execute(create_table_query)
conn.commit()

columns = conn.execute(f"PRAGMA table_info({network}_settle)").fetchall()
columns = [col[1] for col in columns]

df = df.select(columns)

cursor.executemany(
    upsert_query,
    df.to_pandas().values,
)

conn.commit()

conn.close()

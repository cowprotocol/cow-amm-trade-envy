import spice
import dotenv
from tqdm import tqdm
import polars as pl
import duckdb
from tenacity import retry, stop_after_attempt, wait_fixed
from cow_amm_trade_envy.db_utils import upsert_data
from cow_amm_trade_envy.constants import DB_FILE

dotenv.load_dotenv()

QUERY_NR = 4448838
INTERVAL_LENGTH = 10_000

dfs = []
supported_networks = ["ethereum"]

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


def split_intervals(beginning_block: int, current_block: int, interval_len: int) -> list:
    splits = []
    for start in range(beginning_block, current_block + 1, interval_len):
        end = min(start + interval_len - 1, current_block)
        splits.append((start, end))
    return splits

@retry(stop=stop_after_attempt(5), wait=wait_fixed(2))
def query_settle_data(network: str, left: int, right: int) -> pl.DataFrame:
    df = spice.query(QUERY_NR, parameters={"start_block": left, "end_block": right,
                                           "network": network})
    return df


def populate_settlement_table(network: str):

    current_block = get_highest_block(network)
    beginning_block = get_last_block_ingested(network) + 1
    splits = split_intervals(beginning_block, current_block, INTERVAL_LENGTH)

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
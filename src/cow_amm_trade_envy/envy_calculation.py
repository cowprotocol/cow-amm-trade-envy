import json
import pandas as pd
from tqdm import tqdm
from cow_amm_trade_envy.models import UCP, Pools, SettlementTrades, Trade
from cow_amm_trade_envy.datasources import BCoWHelper
from typing import Optional
from cow_amm_trade_envy.db_utils import upsert_data
from cow_amm_trade_envy.constants import DB_FILE
import duckdb

# todo dockerize

helper = BCoWHelper()


def preprocess_row(row: pd.Series) -> pd.Series:
    row["tokens"] = row["tokens"].lower().strip("[]").split()
    row["clearingPrices"] = row["clearingPrices"].lower().strip("[]").split()
    row["trades"] = json.loads(row["trades"].replace(" ", ","))
    row["call_block_number"] = int(row["call_block_number"])
    row["gas_price"] = int(row["gas_price"])
    return row


def calc_surplus_per_trade(ucp: UCP, trade: Trade, block_num) -> Optional[dict]:
    pool = Pools().get_fitting_pool(trade)
    order = helper.order(
        pool=pool.checksum(),
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

    # actual calculation
    cow_amm_buy_amount = order.buyAmount
    max_cow_amm_buy_amount = min(trade.sellAmount, cow_amm_buy_amount)
    # todo lookup price if CoW is not fully filled
    max_cow_amm_sell_amount = (
        order.sellAmount * max_cow_amm_buy_amount / cow_amm_buy_amount
    )

    if trade.isOneToZero(pool):
        selling_token, buying_token = pool.TOKEN1, pool.TOKEN0
    elif trade.isZeroToOne(pool):
        selling_token, buying_token = pool.TOKEN0, pool.TOKEN1
    else:
        #  shouldnt even happen when we only use eligible trades
        raise ValueError("Trade not supported")

    executed_buy_amount = (
        max_cow_amm_buy_amount * ucp[selling_token] / ucp[buying_token]
    )
    surplus = max_cow_amm_sell_amount - executed_buy_amount

    if trade.isOneToZero(pool):
        # todo need an ETH pricelookup for more general pools
        surplus = surplus * ucp[selling_token] / ucp[buying_token]
    return {"surplus": surplus, "pool": pool.ADDRESS}


def calc_gas(gas_price: int):
    return gas_price * 100_000


def calc_envy_per_settlement(row):
    row = preprocess_row(row)

    ucp = UCP.from_lists(row["tokens"], row["clearingPrices"])

    eligible_settlement_trades = SettlementTrades.eligible_trades_from_lists(
        row["tokens"], row["clearingPrices"], row["trades"]
    )

    envy_list = []
    for trade in eligible_settlement_trades:
        surplus_data = calc_surplus_per_trade(ucp, trade, row["call_block_number"])
        if surplus_data:
            surplus = surplus_data["surplus"]
            pool = surplus_data["pool"]
            gas = calc_gas(row["gas_price"])
            trade_envy = (surplus - gas)*1e-18 # convert to ETH
            envy_list.append({"trade_envy": trade_envy, "pool": pool})

    return envy_list


def create_envy_data(network: str):
    # load table
    with duckdb.connect(database=DB_FILE) as conn:
        ucp_data = conn.execute(f"SELECT * FROM {network}_settle").fetchdf()

    ucp_data = ucp_data.sort_values("call_block_number", ascending=False)
    ucp_data.reset_index(drop=True, inplace=True)

    trade_envy_per_settlement = [
        calc_envy_per_settlement(row) for _, row in tqdm(ucp_data.iterrows())
    ]

    df_te = pd.DataFrame({"data": trade_envy_per_settlement})
    df_te = df_te.explode("data")
    df_te["pool"] = df_te["data"].apply(lambda x: None if pd.isna(x) else x["pool"])
    df_te["trade_envy"] = df_te["data"].apply(lambda x: None if pd.isna(x) else x["trade_envy"])
    ucp_data["trade_envy"] = df_te["trade_envy"]
    ucp_data["pool"] = df_te["pool"]

    envy_data = pd.DataFrame(
        {
            "call_tx_hash": ucp_data["call_tx_hash"],
            "trade_envy": ucp_data["trade_envy"],
            "pool": ["None" if pd.isna(x) else x for x in ucp_data["pool"]],
        }
    )

    query_recreate_table = f"""
    CREATE TABLE IF NOT EXISTS {network}_envy (
    call_tx_hash BLOB,
    pool TEXT,
    trade_envy NUMERIC,
    PRIMARY KEY (call_tx_hash, pool)
    );
    """

    with duckdb.connect(database=DB_FILE) as conn:
        conn.execute(query_recreate_table)
        upsert_data(f"{network}_envy", envy_data, conn)

    if network == "ethereum":
        outfile = "data/cow_amm_missed_surplus.csv"
        ucp_data.to_csv(outfile) # keep this for now to see changes in the diffs


if __name__ == "__main__":
    network = "ethereum"
    create_envy_data(network)

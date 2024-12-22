import json
import pandas as pd
from tqdm import tqdm
from cow_amm_trade_envy.models import UCP, Pools, SettlementTrades, Trade
from cow_amm_trade_envy.datasources import BCoWHelper
from typing import Optional
import sqlite3

# todo dockerize

helper = BCoWHelper()


DB_FILE = "data.db"
conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()


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
    # todo lookup price if not CoW is not fully filled
    max_cow_amm_sell_amount = (
        order.sellAmount * max_cow_amm_buy_amount / cow_amm_buy_amount
    )

    if trade.isOneToZero(pool):
        selling_token, buying_token = pool.TOKEN1, pool.TOKEN0
    elif trade.isZeroToOne(pool):
        selling_token, buying_token = pool.TOKEN0, pool.TOKEN1
    else:
        return None  #  shouldnt even happen when we only use eligible trades

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

    for trade in eligible_settlement_trades:
        surplus_data = calc_surplus_per_trade(ucp, trade, row["call_block_number"])
        if surplus_data:
            surplus = surplus_data["surplus"]
            pool = surplus_data["pool"]
            gas = calc_gas(row["gas_price"])
            trade_envy = surplus - gas
            return {"trade_envy": trade_envy, "pool": pool}

    return {"trade_envy": None, "pool": None}


def create_envy_data(network: str, outfile: str):
    # load table
    ucp_data = pd.read_sql_query(f"SELECT * FROM {network}_settle", conn)

    ucp_data = ucp_data.sort_values("call_block_number", ascending=False)
    ucp_data.reset_index(drop=True, inplace=True)

    trade_envy_per_settlement = [
        calc_envy_per_settlement(row) for _, row in tqdm(ucp_data.iterrows())
    ]
    ucp_data["trade_envy"] = [x["trade_envy"] for x in trade_envy_per_settlement]
    ucp_data["pool"] = [x["pool"] for x in trade_envy_per_settlement]

    ucp_data.to_csv(outfile)


if __name__ == "__main__":
    outfile = "data/cow_amm_missed_surplus.csv"

    network = "ethereum"
    create_envy_data(network, outfile)

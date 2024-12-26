import json
import pandas as pd
from tqdm import tqdm
from cow_amm_trade_envy.models import UCP, Pools, Tokens, Token, SettlementTrades, Trade, BCowPool, CoWAmmOrderData
from cow_amm_trade_envy.datasources import BCoWHelper
from typing import Optional
from cow_amm_trade_envy.db_utils import upsert_data
from cow_amm_trade_envy.constants import DB_FILE
import duckdb
from cow_amm_trade_envy.datasources import get_token_to_native_rate

network = "ethereum"
helper = BCoWHelper(network)


def preprocess_row(row: pd.Series) -> pd.Series:
    row["tokens"] = row["tokens"].lower().strip("[]").split()
    row["clearingPrices"] = row["clearingPrices"].lower().strip("[]").split()
    row["trades"] = json.loads(row["trades"].replace(" ", ","))
    row["call_block_number"] = int(row["call_block_number"])
    row["gas_price"] = int(row["gas_price"])
    return row

def calc_max_cow_sell_amount(
    pool: BCowPool, order: CoWAmmOrderData, block_num: int, selling_token: Token,
    max_cow_amm_buy_amount: int, cow_amm_buy_amount: int
) -> int:
    """
    Calculates the maximum amount of the selling token that the CoW AMM can buy.
    In case the CoW is not fully filled, the helper is used to get the partial order.
    """
    ratio_filled = max_cow_amm_buy_amount / cow_amm_buy_amount
    if ratio_filled == 1:
        max_cow_amm_sell_amount = order.sellAmount
    else:
        # from CoWAMMs perspective, the selling_token is bought
        partial_order = helper.order_from_buy_amount(
            pool, selling_token.address, max_cow_amm_buy_amount, block_num
        )

        # if the helper was not yet deployed, we can't get the partial order
        if partial_order is None:
            max_cow_amm_sell_amount = order.sellAmount * ratio_filled
        else:
            max_cow_amm_sell_amount = partial_order.sellAmount

    return max_cow_amm_sell_amount

def calc_surplus_per_trade(
    ucp: UCP, trade: Trade, block_num: int, network: str
) -> Optional[dict]:
    pool = Pools().get_fitting_pool(trade)
    order = helper.order(
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


    max_cow_amm_sell_amount = calc_max_cow_sell_amount(
        pool, order, block_num, selling_token, max_cow_amm_buy_amount, cow_amm_buy_amount
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
        rate_in_wrapped_native = get_token_to_native_rate(
            network, pool.TOKEN1.address, block_num
        )
        decimal_correction_factor = 10 ** (
            Tokens.native.decimals - pool.TOKEN1.decimals
        )
        surplus = surplus * rate_in_wrapped_native * decimal_correction_factor

    return {"surplus": surplus, "pool": pool.ADDRESS}


def calc_gas(gas_price: int):
    return gas_price * 100_000


def calc_envy_per_settlement(row, network: str):
    row = preprocess_row(row)

    ucp = UCP.from_lists(row["tokens"], row["clearingPrices"])

    eligible_settlement_trades = SettlementTrades.eligible_trades_from_lists(
        row["tokens"], row["clearingPrices"], row["trades"], row["call_block_number"]
    )

    envy_list = []
    for trade in eligible_settlement_trades:
        surplus_data = calc_surplus_per_trade(
            ucp, trade, row["call_block_number"], network
        )
        if surplus_data:
            surplus = surplus_data["surplus"]
            pool = surplus_data["pool"]
            gas = calc_gas(row["gas_price"])
            trade_envy = (surplus - gas) * 10 ** (-Tokens.native.decimals)
            envy_list.append({"trade_envy": trade_envy, "pool": pool})

    return envy_list


def create_envy_data(network: str):
    # load table
    with duckdb.connect(database=DB_FILE) as conn:
        ucp_data = conn.execute(f"SELECT * FROM {network}_settle").fetchdf()

    ucp_data = ucp_data.sort_values("call_block_number", ascending=False)
    ucp_data.reset_index(drop=True, inplace=True)

    trade_envy_per_settlement = [
        calc_envy_per_settlement(row, network) for _, row in tqdm(ucp_data.iterrows())
    ]

    df_te = pd.DataFrame({"data": trade_envy_per_settlement})
    df_te = df_te.explode("data")
    df_te["pool"] = df_te["data"].apply(lambda x: None if pd.isna(x) else x["pool"])
    df_te["trade_envy"] = df_te["data"].apply(
        lambda x: None if pd.isna(x) else x["trade_envy"]
    )
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
    call_tx_hash TEXT,
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
        ucp_data.to_csv(outfile)  # keep this for now to see changes in the diffs


if __name__ == "__main__":
    create_envy_data(network)

import json
import pandas as pd
from tqdm import tqdm
from cow_amm_trade_envy.models import UCP, Pools, SettlementTrades, Trade
from cow_amm_trade_envy.datasources import BCoWHelper
from typing import Optional

# todo add ingestion from dune
# todo set up dagster
# todo dockerize
# todo add notebook or similar to show results
# todo store dune and node response data in postgres


helper = BCoWHelper()


def preprocess_row(row: pd.Series) -> pd.Series:
    row["tokens"] = row["tokens"].lower().strip("[]").split()
    row["clearingPrices"] = row["clearingPrices"].lower().strip("[]").split()
    row["trades"] = json.loads(row["trades"].replace(" ", ","))
    row["call_block_number"] = int(row["call_block_number"])
    row["gas_price"] = int(row["gas_price"])
    return row


def calc_surplus_per_trade(ucp: UCP, trade: Trade, block_num) -> Optional[dict]:
    # todo test if direction of trades is actually valid

    pool = Pools().get_fitting_pool(trade)

    order = helper.order(
        pool=pool.checksum(),
        prices=[ucp[pool.TOKEN0], ucp[pool.TOKEN1]],
        block_num=block_num,
    )
    cow_amm_buy = order.sellAmount
    # todo prices should be better when we do not fully balance, probably negligible, assuming constant prices here
    max_buy = min(trade.sellAmount, cow_amm_buy)
    max_sell = order.buyAmount * max_buy / cow_amm_buy

    order_and_trade_aligned = (
        order.buyToken == trade.buyToken and order.sellToken == trade.sellToken
    )
    if not order_and_trade_aligned:
        return None

    if trade.isOneToZero(pool):
        selling_token, buying_token = pool.TOKEN1, pool.TOKEN0
    elif trade.isZeroToOne(pool):
        selling_token, buying_token = pool.TOKEN0, pool.TOKEN1
    else:
        return None  #  shouldnt even happen when we only use eligible trades

    executed_buy = max_sell * ucp[buying_token] / ucp[selling_token]
    surplus = executed_buy - max_buy
    if trade.isZeroToOne(pool):
        # todo need an ETH pricelookup for more general pools
        surplus = surplus * ucp[selling_token] / ucp[buying_token]
    return {
        "surplus": surplus,
        "pool": pool.ADDRESS
    }


def calc_gas(gas_price: int):
    return gas_price * 100_000


def calc_envy(row):
    row = preprocess_row(row)

    ucp = UCP.from_lists(row["tokens"], row["clearingPrices"])

    eligible_settlement_trades = SettlementTrades.eligible_trades_from_lists(
        row["tokens"], row["clearingPrices"], row["trades"]
    )


    for trade in eligible_settlement_trades:
        surplus_data = calc_surplus_per_trade(ucp, trade, row["call_block_number"])
        # todo decide what to do on multiple trades, currently takes the first
        if surplus_data:
            surplus = surplus_data["surplus"]
            pool = surplus_data["pool"]
            gas = calc_gas(row["gas_price"])
            trade_envy = surplus - gas
            return {
                "trade_envy": trade_envy,
                "pool": pool
            }

    return {
        "trade_envy": None,
        "pool": None
    }


def create_envy_data(infile: str, outfile: str):
    ucp_data = pd.read_csv(infile)
    trade_envy_per_settlement = [calc_envy(row) for _, row in tqdm(ucp_data.iterrows())]

    ucp_data["trade_envy"] = [x["trade_envy"] for x in trade_envy_per_settlement]
    ucp_data["pool"] = [x["pool"] for x in trade_envy_per_settlement]
    ucp_data.to_csv(outfile)


if __name__ == "__main__":
    infile = "data/cow_amm_ucp.csv"
    outfile = "data/cow_amm_missed_surplus.csv"

    create_envy_data(infile, outfile)

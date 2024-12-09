import web3
import json
import pandas as pd
from tqdm import tqdm
from models import Contracts, BCowPool, CoWAmmOrderData, UCP, Tokens, SettlementTrades
from bcow_helper import BCoWHelper

helper = BCoWHelper()
usdc_weth = BCowPool(Contracts.USDC_WETH_POOL)

def preprocess_row(row: pd.Series) -> pd.Series:
    row["tokens"] = row['tokens'].lower().strip('[]').split()
    row['clearingPrices'] = row['clearingPrices'].lower().strip('[]').split()
    row['trades'] = json.loads(row['trades'].replace(' ', ','))
    return row

def calc_surplus(ucp: UCP, order: CoWAmmOrderData, settlement_trades:SettlementTrades) -> float:
    # todo assumes we can fully balance
    # todo calc per trade, return None if pool not supported or helper disagrees with trade direction
    #   then decide what to do for multiple trades per settlement
    if order.buyToken == Tokens.USDC and settlement_trades.isWethUsdc:
        cow_amm_buy = order.sellAmount
        executed_buy = order.buyAmount * ucp[Tokens.USDC] / ucp[Tokens.WETH]
        surplus = executed_buy - cow_amm_buy
        return surplus
    elif order.buyToken == Tokens.WETH and settlement_trades.isUsdcWeth:
        cow_amm_buy = order.sellAmount
        executed_buy = order.buyAmount * ucp[Tokens.WETH] / ucp[Tokens.USDC]
        surplus_usdc = executed_buy - cow_amm_buy
        surplus = surplus_usdc * ucp[Tokens.USDC] / ucp[Tokens.WETH]
        return surplus

    return None

def calc_gas(gas_price: int):
    return gas_price * 100_000

def calc_envy(row):
    row = preprocess_row(row)

    ucp = UCP.from_lists(
        row['tokens'],
        row['clearingPrices']
    )

    settlement_trades = SettlementTrades.from_lists(row['tokens'],
                                                    row['clearingPrices'],
                                                    row['trades'])

    order = helper.order(
        pool=usdc_weth.checksum(),
        prices=[
            ucp[Tokens.USDC],
            ucp[Tokens.WETH]
        ],
        block_num=int(row['call_block_number']) + 1
    )

    surplus = calc_surplus(ucp, order, settlement_trades)

    if surplus:
        gas = calc_gas(int(row['gas_price']))
        trade_envy = surplus - gas
        return trade_envy

    return None


if __name__ == '__main__':
    infile = 'cow_amm_ucp.csv'
    outfile = 'cow_amm_missed_surplus.csv'

    ucp_data = pd.read_csv(infile)
    trade_envy_per_settlement = [calc_envy(row) for _, row in tqdm(ucp_data.iterrows())]

    ucp_data['trade_envy'] = trade_envy_per_settlement
    ucp_data.to_csv(outfile)

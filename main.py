import web3
import json
import pandas as pd
from tqdm import tqdm
from models import Contracts, BCowPool, CoWAmmOrderData, UCP, Tokens, SettlementTrades
from bcow_helper import BCoWHelper


def calc_surplus(ucp: UCP, order: CoWAmmOrderData, settlement_trades:SettlementTrades) -> float:
    # todo assumes we can fully balance
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


def calc_gas(gas_price: int):
    return gas_price * 100_000

if __name__ == '__main__':
    filepath = 'cow_amm_ucp.csv'

    ucp_file = pd.read_csv(filepath)
    helper = BCoWHelper()
    usdc_weth = BCowPool(Contracts.USDC_WETH_POOL)
    potential_surplus = list()
    for _, row in tqdm(ucp_file.iterrows()):
        ucp = UCP.from_lists(
                row['tokens'].lower().strip('[]').split(),
                row['clearingPrices'].lower().strip('[]').split()
        )

        settlement_trades = SettlementTrades.from_lists(row['tokens'], row['clearingPrices'], row['trades'])

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
            potential_surplus.append(trade_envy)
        else:
            potential_surplus.append(None)

    ucp_file['potential_surplus'] = potential_surplus
    ucp_file.to_csv("cow_amm_missed_surplus.csv")

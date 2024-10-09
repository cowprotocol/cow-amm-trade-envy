import web3
import json
import pandas as pd

from models import Contracts, BCowPool, CoWAmmOrderData, UCP, Tokens, Trades
from bcow_helper import BCoWHelper

def calc_surplus(ucp: UCP, order: CoWAmmOrderData, trades:Trades) -> float:
    if order.buyToken == Tokens.WETH and trades.isWethUsdc:
        cow_amm_buy = order.weth_amount
        executed_buy = order.usdc_amount * ucp[Tokens.USDC] / ucp[Tokens.WETH]
        surplus = executed_buy - cow_amm_buy
        return surplus
    elif order.buyToken == Tokens.USDC and trades.isUsdcWeth:
        cow_amm_buy = order.usdc_amount
        executed_buy = order.weth_amount * ucp[Tokens.WETH] / ucp[Tokens.USDC]
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
    for _, row in ucp_file.iterrows():
        ucp = UCP.from_lists(
                row['tokens'].lower().strip('[]').split(),
                row['clearingPrices'].lower().strip('[]').split()
        )
        order = helper.order(
                pool=usdc_weth.checksum(), 
                prices=[
                    ucp[Tokens.USDC],
                    ucp[Tokens.WETH]
                ],
                block_num=int(row['call_block_number'])+1
        )
        trades = Trades.from_lists(row['tokens'], row['clearingPrices'], row['trades'])
        if trades.isUsdcWeth or trades.isWethUsdc:
            try:
                surplus = calc_surplus(ucp, order, trades)
                if surplus:
                    gas = calc_gas(int(row['gas_price']))
                    potential_surplus.append(surplus - gas)
                else:
                    potential_surplus.append(None)
            except ValueError:
                potential_surplus.append(None)
        else:
            potential_surplus.append(None)

    ucp_file['potential_surplus'] = potential_surplus
    ucp_file.to_csv("cow_amm_missed_surplus.csv")

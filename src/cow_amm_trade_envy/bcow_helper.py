import os
from web3 import Web3
from typing import List
from dotenv import load_dotenv

from cow_amm_trade_envy.constants import BCOW_HELPER_ABI
from cow_amm_trade_envy.models import Contracts, CoWAmmOrderData

load_dotenv()
node_url = os.getenv("NODE_URL")


class BCoWHelper:
    def __init__(self):
        self.address = Contracts.HELPER_CONTRACT
        self.abi = BCOW_HELPER_ABI
        self.w3 = Web3(Web3.HTTPProvider(node_url, request_kwargs={"timeout": 60}))
        self.contract = self.w3.eth.contract(address=self.address, abi=self.abi)

    def order(
        self, pool: str, prices: List[int], block_num: int | None = None
    ) -> CoWAmmOrderData:
        fun = self.contract.functions.order(pool, prices)
        response = fun.call(block_identifier=block_num)
        order, preInteractions, postInteractions, sig = response
        return CoWAmmOrderData.from_order_response(order)

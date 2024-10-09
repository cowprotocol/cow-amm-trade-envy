import os
from web3 import Web3
from typing import List
from dotenv import load_dotenv

from constants import BCOW_HELPER_ABI
from models import Contracts, CoWAmmOrderData

load_dotenv()
node_url = os.getenv("NODE_URL")

class BCoWHelper:
    def __init__(self):
        self.address = Contracts.HELPER_CONTRACT
        self.abi = BCOW_HELPER_ABI
        self.w3 = Web3(Web3.HTTPProvider(
            node_url,
            request_kwargs={'timeout': 60}
        ))
        self.contract = self.w3.eth.contract(address=self.address, abi=self.abi)

    def order(self, 
              pool:str, 
              prices: List[str], 
              block_num: int | None = None) -> CoWAmmOrderData:
        _resp = self.contract.functions.order(
                pool,
                prices
        )
        if block_num:
            _resp = _resp.call(block_identifier=block_num)
        else:
            _resp = _resp.call()
        return CoWAmmOrderData.from_order_response(prices, _resp[0])

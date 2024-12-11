import os
import sqlite3
import json
from web3 import Web3
from typing import List
from dotenv import load_dotenv
from cow_amm_trade_envy.constants import BCOW_HELPER_ABI
from cow_amm_trade_envy.models import Contracts, CoWAmmOrderData

load_dotenv()
node_url = os.getenv("NODE_URL")
w3 = Web3(Web3.HTTPProvider(node_url, request_kwargs={"timeout": 60}))

# Setup SQLite
DB_FILE = "cache.db"
conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

# Create table if it doesn't exist
cursor.execute('''
CREATE TABLE IF NOT EXISTS order_cache (
    key TEXT PRIMARY KEY,
    response TEXT
)
''')
cursor.execute('''
CREATE TABLE IF NOT EXISTS receipt_cache (
    key TEXT PRIMARY KEY,
    response TEXT
)
''')
conn.commit()


def json_serializer(obj):
    if isinstance(obj, (HexBytes, bytes)):
        return obj.hex()

    if isinstance(obj, AttributeDict):
        return dict(obj)

    if isinstance(obj, dict):
        return {k: json_serializer(v) for k, v in obj.items()}

    if isinstance(obj, (list, tuple)):
        return [json_serializer(item) for item in obj]

    if isinstance(obj, (int, float, str, bool, type(None))):
        return obj

    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

def query(pool: str, prices: List[int], block_num: int, contract) -> List:
    fun = contract.functions.order(pool, prices)
    response = fun.call(block_identifier=block_num)
    order, preInteractions, postInteractions, sig = response
    order_data = json_serializer(order)
    return order_data

def fetch_from_cache_or_query(pool: str, prices: List[int], block_num: int | None, contract):
    cache_key = f"{pool}_{prices}_{block_num}"

    cursor.execute('SELECT response FROM order_cache WHERE key = ?', (cache_key,))
    row = cursor.fetchone()
    if row:
        order_data = json.loads(row[0])
    else:
        order_data = query(pool, prices, block_num, contract)
        cursor.execute(
            'INSERT INTO order_cache (key, response) VALUES (?, ?)',
            (cache_key, json.dumps(order_data))
        )
        conn.commit()
    return order_data

class BCoWHelper:
    def __init__(self):
        self.address = Contracts.HELPER_CONTRACT
        self.abi = BCOW_HELPER_ABI
        self.contract = w3.eth.contract(address=self.address, abi=self.abi)

    def order(
        self, pool: str, prices: List[int], block_num: int | None = None
    ) -> CoWAmmOrderData:
        order = fetch_from_cache_or_query(pool, prices, block_num, self.contract)
        return CoWAmmOrderData.from_order_response(order)


import json
from web3 import Web3
from web3.types import HexBytes
from web3.datastructures import AttributeDict



def get_logs(tx_hash: str):
    cache_key = f"{tx_hash}"

    cursor.execute('SELECT response FROM receipt_cache WHERE key = ?', (cache_key,))
    row = cursor.fetchone()

    if row:
        logs = json.loads(row[0])
    else:
        receipt = w3.eth.get_transaction_receipt(tx_hash)

        # Use the custom serializer to convert logs
        logs = json.loads(json.dumps(receipt["logs"], default=json_serializer))

        cursor.execute(
            'INSERT INTO receipt_cache (key, response) VALUES (?, ?)',
            (cache_key, json.dumps(logs))
        )
        conn.commit()

    return logs


if __name__ == "__main__":
    a = get_logs("0x5381141986041a1b42931e5e37bdab3cce7672a830914390c6da18a477ca930b")
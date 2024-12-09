import os
import pandas as pd
from web3 import Web3
from dotenv import load_dotenv

load_dotenv()
node_url = os.getenv("NODE_URL")

# topics are hexbytes
def isin_bondingpool(topic: bytes, pool_address: str) -> bool:
    return pool_address.lower() in topic.hex()

def log_isused(log, pool_address: str) -> bool:
    return any([isin_bondingpool(topic, weth_usdc_pool_addr) for topic in log.topics])

def logs_isused(logs, pool_address: str) -> bool:
    return any([log_isused(log, pool_address) for log in logs])

if __name__ == '__main__':
    filepath = 'cow_amm_missed_surplus.csv'

    weth_usdc_pool_addr = "f08d4dea369c456d26a3168ff0024b904f2d8b91"

    file = pd.read_csv(filepath)
    w3 = Web3(Web3.HTTPProvider(
        node_url,
        request_kwargs={'timeout': 60}
    ))
    print(f"Potential surplus: {file['potential_surplus'][file['potential_surplus'] > 0].sum()}")
    print(f"Block range: {file['call_block_number'].min()} - {file['call_block_number'].max()}")

    total_surplus = 0
    used_surplus = 0
    for _, row in file.iterrows():
        if row['potential_surplus'] and row['potential_surplus'] > 0:
            txs = w3.eth.get_transaction_receipt(row['call_tx_hash'])
            isUsed = logs_isused(txs.logs, weth_usdc_pool_addr)
            if not isUsed:
                total_surplus += row['potential_surplus']
            else:
                used_surplus += row['potential_surplus']

    print(f"Total unused surplus: {total_surplus/10**18}")
    print(f"Total used surplus: {used_surplus/10**18}")
    

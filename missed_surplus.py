import os
import pandas as pd
from web3 import Web3
from dotenv import load_dotenv

load_dotenv()

node_url = os.getenv("NODE_URL")

def analyze_surplus(file_path, node_url, pool_address):
    w3 = Web3(Web3.HTTPProvider(node_url, request_kwargs={'timeout': 60}))
    pool_address = pool_address.lower()

    def isin_pool(topic):
        return pool_address in topic.hex()

    def log_is_used(log):
        return any(isin_pool(topic) for topic in log.topics)

    def logs_are_used(logs):
        return any(log_is_used(log) for log in logs)

    file = pd.read_csv(file_path)
    total_surplus, used_surplus = 0, 0

    for _, row in file.iterrows():
        if row['potential_surplus'] > 0:
            tx_receipt = w3.eth.get_transaction_receipt(row['call_tx_hash'])
            is_used = logs_are_used(tx_receipt.logs)

            if not is_used:
                total_surplus += row['potential_surplus']
            else:
                used_surplus += row['potential_surplus']

    return {
        "total_unused_surplus": total_surplus / 10**18,
        "total_used_surplus": used_surplus / 10**18,
    }

if __name__ == '__main__':
    file_path = 'cow_amm_missed_surplus.csv'
    pool_address = "f08d4dea369c456d26a3168ff0024b904f2d8b91"

    surplus_results = analyze_surplus(file_path, node_url, pool_address)
    print(f"Total unused surplus: {surplus_results['total_unused_surplus']}")
    print(f"Total used surplus: {surplus_results['total_used_surplus']}")

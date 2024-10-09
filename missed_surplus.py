import os
import pandas as pd
from web3 import Web3
from dotenv import load_dotenv

load_dotenv()
node_url = os.getenv("NODE_URL")

if __name__ == '__main__':
    filepath = 'cow_amm_missed_surplus.csv'
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
            isUsed = False
            txs = w3.eth.get_transaction_receipt(row['call_tx_hash'])
            for log in txs.logs:
                for topic in log.topics:
                    # Search for tx to the usdc-weth cow bonding pool
                    if "f08d4dea369c456d26a3168ff0024b904f2d8b91".lower() in topic.hex():
                        isUsed = True
            if not isUsed:
                total_surplus += row['potential_surplus']
            else:
                used_surplus += row['potential_surplus']
    print(f"Total unused surplus: {total_surplus}")
    print(f"Total used surplus: {used_surplus}")
    

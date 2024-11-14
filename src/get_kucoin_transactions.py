import asyncio
import sys
from kucoin_data.transactions import main

# Parse command-line arguments
args = dict(arg.split('=', 1) for arg in sys.argv[1:])
DB_NAME = args.get('db_name', 'testing')
STATS_COLLECTION = args.get('stats_collection', 'binance_transactions')
BIG_TRANSACTIONS_COLLECTION = args.get('big_transactions_collection', 'binance_big_transactions')
PRICES_COLLECTION = args.get('prices_collection', 'prices_collection')

# Parse usdt_pairs from command-line argument
usdt_pairs_str = args.get('pairs', '')
PAIRS = [pair.strip() for pair in usdt_pairs_str.split(',')] if usdt_pairs_str else ['BTC-USDT']

if __name__ == "__main__":
    asyncio.run(main(DB_NAME, STATS_COLLECTION, BIG_TRANSACTIONS_COLLECTION, PRICES_COLLECTION,PAIRS))
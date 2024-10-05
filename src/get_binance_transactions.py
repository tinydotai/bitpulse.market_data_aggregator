import asyncio
import sys
from binance.transactions import main
from binance.pairs import usdt_pairs

# Parse command-line arguments
args = dict(arg.split('=') for arg in sys.argv[1:])
DB_NAME = args.get('db_name', 'bitpulse')
STATS_COLLECTION = args.get('stats_collection', 'binance_transactions')
BIG_TRANSACTIONS_COLLECTION = args.get('big_transactions_collection', 'binance_big_transactions')

if __name__ == "__main__":
    asyncio.run(main(DB_NAME, STATS_COLLECTION, BIG_TRANSACTIONS_COLLECTION, usdt_pairs[:2]))
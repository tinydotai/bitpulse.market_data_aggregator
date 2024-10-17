import asyncio
import sys
from coingecko.fetch_coingecko_data import main

# Parse command-line arguments
args = dict(arg.split('=', 1) for arg in sys.argv[1:])
DB_NAME = args.get('db_name', 'testing')
DATA_COLLECTION = args.get('data_collection', 'coingecko_data')

# Parse usdt_pairs from command-line argument

if __name__ == "__main__":
    asyncio.run(main(DB_NAME, DATA_COLLECTION))
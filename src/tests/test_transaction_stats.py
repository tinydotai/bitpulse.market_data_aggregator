import asyncio
from datetime import datetime, timedelta
from service.async_mongo import AsyncMongoDBHelper
from binance.pairs import usdt_pairs
from bson import CodecOptions

# ANSI color codes
GREEN = "\033[92m"
RED = "\033[91m"
RESET = "\033[0m"

async def analyze_pair_transactions(mongo_helper, pair, start_time: datetime, end_time: datetime):
    try:
        query = {
            "symbol": pair,
            "timestamp": {
                "$gte": start_time,
                "$lte": end_time
            }
        }
        
        transactions = await mongo_helper.find_many(query, limit=0)
        transactions.sort(key=lambda x: x["timestamp"])

        if not transactions:
            print(f"{RED}No {pair} transactions found in the specified time range.{RESET}")
            return False

        expected_timestamp = transactions[0]['timestamp']
        missing_intervals = 0

        for transaction in transactions[1:]:
            while expected_timestamp + timedelta(seconds=10) < transaction['timestamp']:
                missing_intervals += 1
                expected_timestamp += timedelta(seconds=10)
            expected_timestamp = transaction['timestamp']

        if missing_intervals == 0:
            print(f"{GREEN}{pair}: All expected 10-second intervals are present. "
                  f"({len(transactions)} transactions){RESET}")
            return True
        else:
            print(f"{RED}{pair}: Found {missing_intervals} missing 10-second intervals. "
                  f"({len(transactions)} transactions){RESET}")
            return False

    except Exception as e:
        print(f"{RED}Error analyzing {pair}: {str(e)}{RESET}")
        return False

async def analyze_all_pairs(start_time: datetime, end_time: datetime):
    mongo_helper = AsyncMongoDBHelper("bitpulse")
    codec_options = CodecOptions(tz_aware=True)
    mongo_helper.set_codec_options(codec_options)
    mongo_helper.set_collection("transactions_stats")

    try:
        results = []
        for pair in usdt_pairs:
            result = await analyze_pair_transactions(mongo_helper, pair, start_time, end_time)
            results.append(result)

        total_pairs = len(usdt_pairs)
        successful_pairs = sum(results)
        print(f"\nAnalysis complete. {successful_pairs}/{total_pairs} pairs passed the test.")

    finally:
        await mongo_helper.close_connection()

async def main():
    start_time = datetime(2024, 10, 6, 15, 0, 0)
    end_time = datetime(2024, 10, 6, 19, 0, 0)

    print(f"Analyzing USDT pairs from {start_time} to {end_time}")
    await analyze_all_pairs(start_time, end_time)

if __name__ == "__main__":
    asyncio.run(main())
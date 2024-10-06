import asyncio
from datetime import datetime, timedelta
from service.async_mongo import AsyncMongoDBHelper
from bson import CodecOptions

async def analyze_cvxusdt_transactions(start_time: datetime, end_time: datetime):
    # Initialize MongoDB connection
    mongo_helper = AsyncMongoDBHelper("bitpulse")
    
    # Set codec options to use timezone-aware datetimes
    codec_options = CodecOptions(tz_aware=True)
    mongo_helper.set_codec_options(codec_options)
    
    # Set the collection
    mongo_helper.set_collection("transactions_stats")

    try:
        # Query for CVXUSDT transactions within the specified time range
        query = {
            "symbol": "CVXUSDT",
            "timestamp": {
                "$gte": start_time,
                "$lte": end_time
            }
        }
        
        # Sort by timestamp to ensure chronological order
        transactions = await mongo_helper.find_many(query, limit=0)
        transactions.sort(key=lambda x: x["timestamp"])

        # Analyze the transactions
        if not transactions:
            print("No CVXUSDT transactions found in the specified time range.")
            return

        print(f"Found {len(transactions)} CVXUSDT transactions.")
        print(f"First transaction: {transactions[0]['timestamp']}")
        print(f"Last transaction: {transactions[-1]['timestamp']}")

        # Check if there's a document every 10 seconds
        expected_timestamp = transactions[0]['timestamp']
        missing_intervals = 0

        for transaction in transactions[1:]:
            while expected_timestamp + timedelta(seconds=10) < transaction['timestamp']:
                print(f"Missing data at {expected_timestamp + timedelta(seconds=10)}")
                missing_intervals += 1
                expected_timestamp += timedelta(seconds=10)
            expected_timestamp = transaction['timestamp']

        if missing_intervals == 0:
            print("All expected 10-second intervals are present.")
        else:
            print(f"Found {missing_intervals} missing 10-second intervals.")

    finally:
        # Close the MongoDB connection
        await mongo_helper.close_connection()

# Example usage
async def main():
    # Set your desired time range here
    start_time = datetime(2024, 10, 6, 15, 0, 0)  # October 6, 2024, 00:00:00
    end_time = datetime(2024, 10, 6, 19, 0, 0)    # October 6, 2024, 01:00:00

    await analyze_cvxusdt_transactions(start_time, end_time)

if __name__ == "__main__":
    asyncio.run(main())
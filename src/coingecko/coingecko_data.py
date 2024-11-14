import asyncio
import aiohttp
import time
from datetime import datetime, timezone
from typing import List, Dict
from service.async_mongo import AsyncMongoDBHelper
from bson import CodecOptions

class CoinGeckoDataFetcher:
    def __init__(self, mongo_helper: AsyncMongoDBHelper, collection_name: str):
        self.mongo_helper = mongo_helper
        self.collection_name = collection_name
        self.active_cryptos = []
        
        # Rate limiting
        self.RATE_LIMIT = 5  # Calls per minute
        self.CALL_INTERVAL = 60 / self.RATE_LIMIT
        self.last_call_time = 0

    async def load_active_cryptos(self):
        """Load active cryptocurrencies from MongoDB target_pairs collection"""
        try:
            # Temporarily set collection to target_pairs to fetch the list
            self.mongo_helper.set_collection('target_pairs')
            
            # Fetch all active pairs from target_pairs collection
            pairs = await self.mongo_helper.find_many({})
            
            # Transform the data into the expected format
            self.active_cryptos = [{
                'id': pair.get('id'),
                'symbol': pair.get('symbol'),
                'name': pair.get('base_asset', pair.get('symbol', '').upper()),
                'source': pair.get('source', [])  # Get source array from target_pairs
            } for pair in pairs if pair.get('id')]

            # Reset collection back to original
            self.mongo_helper.set_collection(self.collection_name)
            
            print(f"Loaded {len(self.active_cryptos)} active cryptocurrencies from MongoDB")
            
        except Exception as e:
            print(f"Error loading active cryptos from MongoDB: {e}")
            raise

    async def fetch_and_store_crypto_data(self, session, crypto: Dict):
        url = f"https://api.coingecko.com/api/v3/coins/{crypto['id']}"
        
        # Ensure we don't exceed rate limit
        current_time = time.time()
        time_since_last_call = current_time - self.last_call_time
        if time_since_last_call < self.CALL_INTERVAL:
            await asyncio.sleep(self.CALL_INTERVAL - time_since_last_call)
        
        try:
            async with session.get(url) as response:
                self.last_call_time = time.time()
                
                if response.status == 200:
                    data = await response.json()
                    await self.process_and_store_data(data, crypto.get('source', []))
                    return True
                elif response.status == 429:
                    print(f"Rate limit exceeded for {crypto['id']}. Waiting before retry.")
                    await asyncio.sleep(self.CALL_INTERVAL)
                    return False
                else:
                    print(f"Error fetching data for {crypto['id']}: HTTP {response.status}")
                    return False
        except aiohttp.ClientError as e:
            print(f"Request exception for {crypto['id']}: {e}")
            return False

    async def process_and_store_data(self, data: Dict, source: List[str]):
        processed_data = {
            "id": data.get('id'),
            "symbol": data.get('symbol'),
            "name": data.get('name'),
            "market_cap": data.get('market_data', {}).get('market_cap', {}).get('usd'),
            "market_cap_rank": data.get('market_cap_rank'),
            "total_volume": data.get('market_data', {}).get('total_volume', {}).get('usd'),
            "circulating_supply": data.get('market_data', {}).get('circulating_supply'),
            "total_supply": data.get('market_data', {}).get('total_supply'),
            "max_supply": data.get('market_data', {}).get('max_supply'),
            "last_updated_on_coingecko": data.get('last_updated'),
            "image": data.get('image', {}).get('large'),
            "timestamp": datetime.now(timezone.utc),
            "source": source,  # Add source array from target_pairs
            "data_source": {    # Add detailed data source information
                "name": "coingecko",
                "url": f"https://api.coingecko.com/api/v3/coins/{data.get('id')}",
                "fetched_at": datetime.now(timezone.utc).isoformat()
            }
        }

        try:
            self.mongo_helper.set_collection(self.collection_name)
            result = await self.mongo_helper.update_one_upsert(
                {"id": processed_data["id"]},
                {"$set": processed_data}
            )
            if result.get("upserted_id"):
                print(f"Inserted new data for {processed_data['name']} ({processed_data['symbol']})")
            else:
                print(f"Updated existing data for {processed_data['name']} ({processed_data['symbol']})")
        except Exception as e:
            print(f"Error inserting/updating data for {processed_data['name']}: {e}")

    async def main_loop(self):
        while True:
            print(f"\nStarting data fetch cycle at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Reload active cryptos at the start of each cycle
            await self.load_active_cryptos()
            
            async with aiohttp.ClientSession() as session:
                for crypto in self.active_cryptos:
                    success = await self.fetch_and_store_crypto_data(session, crypto)
                    if not success:
                        print(f"Failed to fetch/store data for {crypto.get('name', 'Unknown')} ({crypto['id']}).")
            
            print(f"\nFinished fetching and storing data for {len(self.active_cryptos)} cryptocurrencies.")
            print(f"Waiting for {len(self.active_cryptos) * self.CALL_INTERVAL} seconds before next cycle...")
            await asyncio.sleep(len(self.active_cryptos) * self.CALL_INTERVAL)

async def main(db_name, collection_name):
    try:
        mongo_helper = AsyncMongoDBHelper(db_name)
        
        # Set codec options to use timezone-aware datetimes
        codec_options = CodecOptions(tz_aware=True, tzinfo=timezone.utc)
        mongo_helper.set_codec_options(codec_options)
        
        fetcher = CoinGeckoDataFetcher(mongo_helper, collection_name)
        
        print("Starting CoinGecko data fetching")
        await fetcher.main_loop()
    except Exception as e:
        print(f"Fatal error in main: {e}")
    finally:
        if 'mongo_helper' in locals():
            await mongo_helper.close_connection()
        print("CoinGecko data fetching stopped")

if __name__ == "__main__":
    asyncio.run(main("test", "test"))
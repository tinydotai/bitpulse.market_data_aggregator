import asyncio
import aiohttp
import time
from datetime import datetime, timezone
from typing import List, Dict
from service.async_mongo import AsyncMongoDBHelper
from bson import CodecOptions

# Assume these are imported from your existing files
from binance.top_list import active_cryptos as binance_cryptos
from kucoin_data.top_list import active_cryptos as kucoin_cryptos

class CoinGeckoDataFetcher:
    def __init__(self, mongo_helper: AsyncMongoDBHelper, collection_name: str):
        self.mongo_helper = mongo_helper
        self.collection_name = collection_name
        self.active_cryptos = self.merge_unique_coin_lists(binance_cryptos, kucoin_cryptos)
        
        # Rate limiting
        self.RATE_LIMIT = 5  # Calls per minute
        self.CALL_INTERVAL = 60 / self.RATE_LIMIT
        self.last_call_time = 0

    @staticmethod
    def merge_unique_coin_lists(list1: List[Dict[str, str]], list2: List[Dict[str, str]]) -> List[Dict[str, str]]:
        combined_list = list1 + list2
        unique_coins = {coin['id']: coin for coin in combined_list}
        return list(unique_coins.values())

    async def fetch_and_store_crypto_data(self, session, crypto_id: str):
        url = f"https://api.coingecko.com/api/v3/coins/{crypto_id}"
        
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
                    await self.process_and_store_data(data)
                    return True
                elif response.status == 429:
                    print(f"Rate limit exceeded for {crypto_id}. Waiting before retry.")
                    await asyncio.sleep(self.CALL_INTERVAL)
                    return False
                else:
                    print(f"Error fetching data for {crypto_id}: HTTP {response.status}")
                    return False
        except aiohttp.ClientError as e:
            print(f"Request exception for {crypto_id}: {e}")
            return False

    async def process_and_store_data(self, data: Dict):
        processed_data = {
            "id": data.get('id'),
            "symbol": data.get('symbol'),
            "name": data.get('name'),
            "current_price": data.get('market_data', {}).get('current_price', {}).get('usd'),
            "market_cap": data.get('market_data', {}).get('market_cap', {}).get('usd'),
            "market_cap_rank": data.get('market_cap_rank'),
            "total_volume": data.get('market_data', {}).get('total_volume', {}).get('usd'),
            "high_24h": data.get('market_data', {}).get('high_24h', {}).get('usd'),
            "low_24h": data.get('market_data', {}).get('low_24h', {}).get('usd'),
            "price_change_24h": data.get('market_data', {}).get('price_change_24h'),
            "price_change_percentage_24h": data.get('market_data', {}).get('price_change_percentage_24h'),
            "circulating_supply": data.get('market_data', {}).get('circulating_supply'),
            "total_supply": data.get('market_data', {}).get('total_supply'),
            "max_supply": data.get('market_data', {}).get('max_supply'),
            "ath": data.get('market_data', {}).get('ath', {}).get('usd'),
            "ath_date": data.get('market_data', {}).get('ath_date', {}).get('usd'),
            "atl": data.get('market_data', {}).get('atl', {}).get('usd'),
            "atl_date": data.get('market_data', {}).get('atl_date', {}).get('usd'),
            "last_updated": data.get('last_updated'),
            "image": data.get('image', {}).get('large'),
            "timestamp": datetime.now(timezone.utc)
        }

        try:
            self.mongo_helper.set_collection(self.collection_name)
            result = await self.mongo_helper.update_one_upsert(
                {"id": processed_data["id"]},
                {"$set": processed_data}
            )
            if result["upserted_id"]:
                print(f"Inserted new data for {processed_data['name']} ({processed_data['symbol']})")
            else:
                print(f"Updated existing data for {processed_data['name']} ({processed_data['symbol']})")
        except Exception as e:
            print(f"Error inserting/updating data for {processed_data['name']}: {e}")

    async def main_loop(self):
        while True:
            print(f"\nStarting data fetch cycle at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
            async with aiohttp.ClientSession() as session:
                for crypto in self.active_cryptos:
                    success = await self.fetch_and_store_crypto_data(session, crypto['id'])
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
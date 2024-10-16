import asyncio
import json
import os
from datetime import datetime, timezone
from kucoin.client import Client
from service.async_mongo import AsyncMongoDBHelper
from service.logger import get_logger
from bson import CodecOptions
import websockets

logger = get_logger('KucoinWebSocket')

class KucoinWebSocket:
    def __init__(self, pairs, mongo_helper: AsyncMongoDBHelper, output_dir='kucoin_data'):
        self.pairs = pairs
        self.output_dir = output_dir
        self.transactions = {}
        self.big_transactions = {}
        self.current_interval = None
        self.interval_seconds = 1  # 1 second interval
        self.mongo_helper = mongo_helper
        self.max_retries = 10
        self.initial_retry_delay = 5
        self.max_retry_delay = 300  # 5 minutes
        self.BIG_TRANSACTION_THRESHOLD = 10000  # $10,000 threshold for big transactions

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # KuCoin client setup
        api_key = os.environ.get('KUCOIN_API_KEY')
        api_secret = os.environ.get('KUCOIN_API_SECRET')
        api_passphrase = os.environ.get('KUCOIN_API_PASSPHRASE')
        self.client = Client(api_key, api_secret, api_passphrase)

    async def connect(self):
        retry_count = 0
        retry_delay = self.initial_retry_delay

        while retry_count < self.max_retries:
            try:
                # Get WebSocket token
                ws_details = self.client.get_ws_endpoint(private=True)
                
                # Extract WebSocket endpoint and token
                ws_endpoint = ws_details['instanceServers'][0]['endpoint']
                ws_token = ws_details['token']

                # Connect to WebSocket
                async with websockets.connect(f"{ws_endpoint}?token={ws_token}") as websocket:
                    logger.info(f"Connected to KuCoin WebSocket for {len(self.pairs)} pairs")
                    logger.info(self.pairs)

                    # Subscribe to WebSocket feed
                    for symbol in self.pairs:
                        subscribe_message = {
                            "id": symbol,
                            "type": "subscribe",
                            "topic": f"/market/match:{symbol}",
                            "privateChannel": False,
                            "response": True
                        }
                        await websocket.send(json.dumps(subscribe_message))

                    # Reset retry counters on successful connection
                    retry_count = 0
                    retry_delay = self.initial_retry_delay

                    while True:
                        try:
                            message = await asyncio.wait_for(websocket.recv(), timeout=30)
                            await self.handle_message(json.loads(message))
                        except asyncio.TimeoutError:
                            # Send ping to keep connection alive
                            await websocket.ping()

            except Exception as e:
                retry_count += 1
                logger.error(f"WebSocket error (attempt {retry_count}/{self.max_retries}): {e}")

                if retry_count >= self.max_retries:
                    logger.critical("Max retries reached. Exiting.")
                    return

                logger.info(f"Attempting to reconnect in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, self.max_retry_delay)  # Exponential backoff

        logger.critical("Failed to establish a stable connection. Exiting.")

    async def handle_message(self, message):
        try:
            if message['type'] == 'message' and message['subject'] == 'trade.l3match':
                data = message['data']
                symbol = data['symbol']
                price = float(data['price'])
                quantity = float(data['size'])
                timestamp = int(data['time'])
                trade_side = data['side'].lower()

                transaction_value = price * quantity

                transaction_time = datetime.fromtimestamp(timestamp / 1000000000, tz=timezone.utc)
                interval_start = transaction_time.replace(microsecond=0)

                if self.current_interval is None or interval_start > self.current_interval:
                    if self.current_interval is not None:
                        await self.process_and_store_data()
                    self.current_interval = interval_start
                    self.transactions = {pair: {'buy': [], 'sell': []} for pair in self.pairs}
                    self.big_transactions = {pair: {'buy': [], 'sell': []} for pair in self.pairs}

                self.transactions[symbol][trade_side].append({'price': price, 'quantity': quantity})

                # Check if it's a big transaction
                if transaction_value >= self.BIG_TRANSACTION_THRESHOLD:
                    self.big_transactions[symbol][trade_side].append({
                        'price': price,
                        'quantity': quantity,
                        'value': transaction_value,
                        'timestamp': transaction_time
                    })

        except KeyError as e:
            logger.error(f"KeyError in handle_message: {e}. Message: {message}")
        except Exception as e:
            logger.error(f"Unexpected error in handle_message: {e}")

    async def process_and_store_data(self):
        try:
            timestamp = self.current_interval.replace(tzinfo=timezone.utc)

            documents = []
            big_transaction_documents = []

            for symbol, data in self.transactions.items():
                output_data = {
                    "timestamp": timestamp,
                    "symbol": symbol,
                }

                for side in ['buy', 'sell']:
                    trades = data[side]
                    if trades:
                        total_quantity = sum(trade['quantity'] for trade in trades)
                        total_value = sum(trade['price'] * trade['quantity'] for trade in trades)
                        output_data.update({
                            f"{side}_count": len(trades),
                            f"{side}_total_quantity": total_quantity,
                            f"{side}_total_value": total_value,
                            f"{side}_min_price": min(trade['price'] for trade in trades),
                            f"{side}_max_price": max(trade['price'] for trade in trades),
                            f"{side}_avg_price": total_value / total_quantity
                        })
                    else:
                        output_data.update({
                            f"{side}_count": 0,
                            f"{side}_total_quantity": 0,
                            f"{side}_total_value": 0,
                            f"{side}_min_price": None,
                            f"{side}_max_price": None,
                            f"{side}_avg_price": None
                        })

                documents.append(output_data)

                # Process big transactions
                big_trades = self.big_transactions[symbol]
                for side in ['buy', 'sell']:
                    for trade in big_trades[side]:
                        big_transaction_documents.append({
                            "timestamp": trade['timestamp'],
                            "symbol": symbol,
                            "side": side,
                            "price": trade['price'],
                            "quantity": trade['quantity'],
                            "value": trade['value']
                        })

            # Insert regular transactions
            self.mongo_helper.set_collection("kucoin_test")
            await self.bulk_insert(documents)

            # Insert big transactions
            if big_transaction_documents:
                await self.bulk_insert_big_transactions(big_transaction_documents)

        except Exception as e:
            logger.error(f"Error in process_and_store_data: {e}")

    async def bulk_insert(self, documents):
        try:
            result = await self.mongo_helper.insert_many(documents)
            logger.info(f"Bulk inserted {len(documents)} documents into MongoDB")
            return result
        except Exception as e:
            logger.error(f"Error bulk inserting data into MongoDB: {e}")
            # Attempt to insert documents one by one
            for doc in documents:
                try:
                    await self.mongo_helper.insert_one(doc)
                except Exception as inner_e:
                    logger.error(f"Error inserting single document: {inner_e}")

    async def bulk_insert_big_transactions(self, documents):
        try:
            self.mongo_helper.set_collection('big_transactions')
            result = await self.mongo_helper.insert_many(documents)
            logger.info(f"Bulk inserted {len(documents)} big transactions into MongoDB")
            return result
        except Exception as e:
            logger.error(f"Error bulk inserting big transactions into MongoDB: {e}")
            # Attempt to insert documents one by one
            for doc in documents:
                try:
                    await self.mongo_helper.insert_one(doc)
                except Exception as inner_e:
                    logger.error(f"Error inserting single big transaction document: {inner_e}")

    async def close(self):
        # Process any remaining data
        if self.transactions:
            await self.process_and_store_data()
        # Add any other cleanup code here

async def main():
    try:
        # You'll need to define your pairs here
        pairs = ["BTC-USDT", "ETH-USDT"]  # Add more pairs as needed
        mongo_helper = AsyncMongoDBHelper("bitpulse")

        # Set codec options to use timezone-aware datetimes
        codec_options = CodecOptions(tz_aware=True, tzinfo=timezone.utc)
        mongo_helper.set_codec_options(codec_options)
        
        # Set the main collection
        mongo_helper.set_collection("kucoin_test")

        kucoin_ws = KucoinWebSocket(pairs, mongo_helper, output_dir='kucoin_data/transactions')
        
        await kucoin_ws.connect()
    except Exception as e:
        logger.critical(f"Fatal error in main: {e}")
    finally:
        logger.info("Shutting down...")
        if 'kucoin_ws' in locals():
            await kucoin_ws.close()
        if 'mongo_helper' in locals():
            await mongo_helper.close_connection()

if __name__ == "__main__":
    asyncio.run(main())
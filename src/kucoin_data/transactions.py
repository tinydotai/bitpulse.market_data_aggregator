import asyncio
import json
import os
from datetime import datetime, timezone
from kucoin.client import Client
from service.async_mongo import AsyncMongoDBHelper
from bson import CodecOptions
import websockets
from prometheus_client import start_http_server, Counter, Gauge

# Prometheus metrics
TRANSACTIONS_TOTAL = Counter('kucoin_transactions_total', 'Total number of transactions', ['symbol', 'side'])
TRANSACTION_VALUE = Counter('kucoin_transaction_value_total', 'Total value of transactions', ['symbol', 'side'])
PRICE_GAUGE = Gauge('kucoin_price', 'Current price', ['symbol'])
BIG_TRANSACTIONS = Counter('kucoin_big_transactions_total', 'Number of big transactions', ['symbol', 'side'])

class KucoinWebSocket:
    def __init__(self, pairs, mongo_helper: AsyncMongoDBHelper, stats_collection, big_transactions_collection):
        self.pairs = pairs
        self.transactions = {}
        self.big_transactions = {}
        self.current_interval = None
        self.interval_seconds = 1  # 1 second interval
        self.mongo_helper = mongo_helper
        self.max_retries = 10
        self.initial_retry_delay = 5
        self.max_retry_delay = 60  # 1 minute
        self.BIG_TRANSACTION_THRESHOLD = 10000  # $10,000 threshold for big transactions
        self.stats_collection = stats_collection
        self.big_transactions_collection = big_transactions_collection
        start_http_server(8001)  # Prometheus will scrape metrics from this port

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

                print(f"Attempting to connect to KuCoin WebSocket: {ws_endpoint}")
                async with websockets.connect(f"{ws_endpoint}?token={ws_token}") as websocket:
                    print("Successfully connected to KuCoin WebSocket")
                    print(f"Connected for {len(self.pairs)} pairs")
                    print(self.pairs)

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
                        print(f"Sent subscription request for {symbol}")

                    # Reset retry counters on successful connection
                    retry_count = 0
                    retry_delay = self.initial_retry_delay

                    while True:
                        try:
                            response = await asyncio.wait_for(websocket.recv(), timeout=30)
                            message = json.loads(response)
                            await self.handle_message(message)
                        except asyncio.TimeoutError:
                            print("No data received in 30 seconds, sending ping")
                            await websocket.ping()
                            print("Sent ping, waiting for pong")
                        except json.JSONDecodeError as e:
                            print(f"JSON decode error: {e}. Response: {response}")
                        except Exception as e:
                            print(f"Error handling message: {e}")
                            raise  # Re-raise to trigger reconnection

            except (websockets.exceptions.ConnectionClosed, 
                    websockets.exceptions.WebSocketException, 
                    asyncio.TimeoutError) as e:
                retry_count += 1
                print(f"WebSocket error (attempt {retry_count}/{self.max_retries}): {e}")

                if retry_count >= self.max_retries:
                    print("Max retries reached. Exiting.")
                    return

                print(f"Retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, self.max_retry_delay)  # Exponential backoff

            except Exception as e:
                print(f"Unexpected error in WebSocket connection: {e}")
                return

        print("Failed to establish a stable connection. Exiting.")

    async def handle_message(self, message):
        try:
            if message['type'] == 'message' and message['subject'] == 'trade.l3match':
                data = message['data']
                symbol = data['symbol']
                price = float(data['price'])
                quantity = float(data['size'])
                timestamp = int(data['time'])
                trade_side = data['side'].lower()

                # Extract base and quote currencies
                base_currency, quote_currency = symbol.split('-')

                transaction_value = price * quantity

                transaction_time = datetime.fromtimestamp(timestamp / 1000000000, tz=timezone.utc)
                interval_start = transaction_time.replace(microsecond=0)

                if self.current_interval is None or interval_start > self.current_interval:
                    if self.current_interval is not None:
                        await self.process_and_store_data()
                    self.current_interval = interval_start
                    self.transactions = {pair: {'buy': [], 'sell': []} for pair in self.pairs}
                    self.big_transactions = {pair: {'buy': [], 'sell': []} for pair in self.pairs}

                self.transactions[symbol][trade_side].append({
                    'price': price,
                    'quantity': quantity,
                    'baseCurrency': base_currency,
                    'quoteCurrency': quote_currency
                })

                # Update Prometheus metrics
                TRANSACTIONS_TOTAL.labels(symbol=symbol, side=trade_side).inc()
                TRANSACTION_VALUE.labels(symbol=symbol, side=trade_side).inc(transaction_value)
                PRICE_GAUGE.labels(symbol=symbol).set(price)

                # Check if it's a big transaction
                if transaction_value >= self.BIG_TRANSACTION_THRESHOLD:
                    BIG_TRANSACTIONS.labels(symbol=symbol, side=trade_side).inc()
                    self.big_transactions[symbol][trade_side].append({
                        'price': price,
                        'quantity': quantity,
                        'value': transaction_value,
                        'timestamp': transaction_time,
                        'baseCurrency': base_currency,
                        'quoteCurrency': quote_currency
                    })

        except KeyError as e:
            print(f"KeyError in handle_message: {e}. Message: {message}")
        except Exception as e:
            print(f"Unexpected error in handle_message: {e}")

    async def process_and_store_data(self):
        try:
            timestamp = self.current_interval.replace(tzinfo=timezone.utc)
            print("-"*50)
            print(f"Processing data for interval: {timestamp}")

            documents = []
            big_transaction_documents = []

            for symbol, data in self.transactions.items():
                output_data = {
                    "timestamp": timestamp,
                    "symbol": symbol,
                    "source": "kucoin",
                    "baseCurrency": symbol.split('-')[0],
                    "quoteCurrency": symbol.split('-')[1]
                }

                has_trades = False
                for side in ['buy', 'sell']:
                    trades = data[side]
                    if trades:
                        has_trades = True
                        total_quantity = sum(trade['quantity'] for trade in trades)
                        total_value = sum(trade['price'] * trade['quantity'] for trade in trades)
                        output_data.update({
                            f"{side}_count": len(trades),
                            f"{side}_total_quantity": total_quantity,
                            f"{side}_total_value": total_value,
                            f"{side}_min_price": min(trade['price'] for trade in trades),
                            f"{side}_max_price": max(trade['price'] for trade in trades),
                            f"{side}_avg_price": total_value / total_quantity,
                        })
                        print(f"{symbol} {side}: {len(trades)} trades, total value: {total_value}")
                    else:
                        print(f"{symbol} {side}: No trades")

                if has_trades:
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
                            "value": trade['value'],
                            "source": "kucoin",
                            "baseCurrency": trade['baseCurrency'],
                            "quoteCurrency": trade['quoteCurrency']
                        })

            # Insert regular transactions
            if documents:
                self.mongo_helper.set_collection(self.stats_collection)
                regular_insert_result = await self.bulk_insert(documents)
                if regular_insert_result:
                    print(f"Successfully inserted {len(documents)} documents into {self.stats_collection}")
                else:
                    print(f"No result returned from bulk insert into {self.stats_collection}")
            else:
                print("No trades to insert for this interval")

            # Insert big transactions
            if big_transaction_documents:
                big_insert_result = await self.bulk_insert_big_transactions(big_transaction_documents)
                if big_insert_result:
                    print(f"Successfully inserted {len(big_transaction_documents)} big transaction documents into {self.big_transactions_collection}")
                else:
                    print(f"No result returned from bulk insert into {self.big_transactions_collection}")
            else:
                print("No big transactions to insert")

            print("Data processing and storage completed successfully")
        except Exception as e:
            print(f"Error in process_and_store_data: {e}")
        finally:
            print("-"*50)

    async def bulk_insert(self, documents):
        try:
            result = await self.mongo_helper.insert_many(documents)
            return result
        except Exception as e:
            print(f"Error bulk inserting data into MongoDB: {e}")
            # Attempt to insert documents one by one
            for doc in documents:
                try:
                    await self.mongo_helper.insert_one(doc)
                except Exception as inner_e:
                    print(f"Error inserting single document: {inner_e}")

    async def bulk_insert_big_transactions(self, documents):
        try:
            self.mongo_helper.set_collection(self.big_transactions_collection)
            result = await self.mongo_helper.insert_many(documents)
            return result
        except Exception as e:
            print(f"Error bulk inserting big transactions into MongoDB: {e}")
            # Attempt to insert documents one by one
            for doc in documents:
                try:
                    await self.mongo_helper.insert_one(doc)
                except Exception as inner_e:
                    print(f"Error inserting single big transaction document: {inner_e}")

    async def close(self):
        # Process any remaining data
        if self.transactions:
            await self.process_and_store_data()
        # Add any other cleanup code here

async def main(db_name, stats_collection, big_transactions_collection, pairs):
    try:
        mongo_helper = AsyncMongoDBHelper(db_name)

        # Set codec options to use timezone-aware datetimes
        codec_options = CodecOptions(tz_aware=True, tzinfo=timezone.utc)
        mongo_helper.set_codec_options(codec_options)
        
        # Set the main collection
        mongo_helper.set_collection(stats_collection)

        kucoin_ws = KucoinWebSocket(pairs, mongo_helper, stats_collection, big_transactions_collection)
        
        print("Starting KuCoin WebSocket connection")
        await kucoin_ws.connect()
    except Exception as e:
        print(f"Fatal error in main: {e}")
    finally:
        if 'kucoin_ws' in locals():
            await kucoin_ws.close()
        if 'mongo_helper' in locals():
            await mongo_helper.close_connection()
        print("KuCoin WebSocket connection closed")

if __name__ == "__main__":
    # You should define db_name, stats_collection, big_transactions_collection, and pairs here
    # or pass them as command-line arguments
    db_name = "your_db_name"
    stats_collection = "your_stats_collection"
    big_transactions_collection = "your_big_transactions_collection"
    pairs = ["BTC-USDT", "ETH-USDT"]  # Add more pairs as needed
    
    asyncio.run(main(db_name, stats_collection, big_transactions_collection, pairs))
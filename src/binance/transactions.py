import asyncio
import websockets
import json
import os
from datetime import datetime, timezone
from service.async_mongo import AsyncMongoDBHelper
from bson import CodecOptions
from prometheus_client import start_http_server, Counter, Gauge

# Prometheus metrics
TRANSACTIONS_TOTAL = Counter('binance_transactions_total', 'Total number of transactions', ['symbol', 'side'])
TRANSACTION_VALUE = Counter('binance_transaction_value_total', 'Total value of transactions', ['symbol', 'side'])
PRICE_GAUGE = Gauge('binance_price', 'Current price', ['symbol'])
BIG_TRANSACTIONS = Counter('binance_big_transactions_total', 'Number of big transactions', ['symbol', 'side'])

class BinanceWebSocket:
    def __init__(self, pairs, mongo_helper: AsyncMongoDBHelper, stats_collection, big_transactions_collection, prices_collection):
        self.pairs = pairs
        self.base_url = "wss://stream.binance.com:9443/ws"
        self.transactions = {}
        self.big_transactions = {}
        self.current_interval = None
        self.interval_seconds = 1  # Changed from 10 to 1 second
        self.mongo_helper = mongo_helper
        self.max_retries = 10
        self.initial_retry_delay = 5
        self.max_retry_delay = 60  # 1 minute
        self.BIG_TRANSACTION_THRESHOLD = 10000  # $10,000 threshold for big transactions
        self.stats_collection = stats_collection
        self.big_transactions_collection = big_transactions_collection
        self.prices_collection = prices_collection
        start_http_server(8000)  # Prometheus will scrape metrics from this port

    async def connect(self):
        stream_names = [f"{pair.lower()}@trade" for pair in self.pairs]
        ws_url = f"{self.base_url}/{'/'.join(stream_names)}"

        retry_count = 0
        retry_delay = self.initial_retry_delay

        while retry_count < self.max_retries:
            try:
                print(f"Attempting to connect to Binance WebSocket: {ws_url}")
                async with websockets.connect(ws_url) as websocket:
                    print("Successfully connected to Binance WebSocket")
                    
                    subscribe_msg = {
                        "method": "SUBSCRIBE",
                        "params": stream_names,
                        "id": 1
                    }
                    await websocket.send(json.dumps(subscribe_msg))
                    print(f"Sent subscription request for {len(stream_names)} pairs")

                    # Wait for subscription confirmation
                    subscription_response = await websocket.recv()
                    subscription_data = json.loads(subscription_response)
                    if subscription_data.get('result') is None:
                        print(f"Successfully subscribed to all streams: {stream_names}")
                    else:
                        print(f"Unexpected subscription response: {subscription_data}")

                    retry_count = 0  # Reset retry count on successful connection
                    retry_delay = self.initial_retry_delay  # Reset retry delay

                    while True:
                        try:
                            response = await asyncio.wait_for(websocket.recv(), timeout=30)  # 30 second timeout
                            transaction = json.loads(response)
                            await self.handle_message(transaction)
                        except asyncio.TimeoutError:
                            print("No data received in 30 seconds, sending ping")
                            pong_waiter = await websocket.ping()
                            await asyncio.wait_for(pong_waiter, timeout=10)
                            print("Received pong, connection still alive")
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
            if 'e' in message and message['e'] == 'trade':
                symbol = message['s']
                price = float(message['p'])
                quantity = float(message['q'])
                timestamp = int(message['T'])
                is_buyer_maker = message['m']

                # Extract base and quote currencies
                base_currency = symbol[:-4]  # Assuming USDT pairs, adjust if needed
                quote_currency = symbol[-4:]

                trade_side = "sell" if is_buyer_maker else "buy"
                transaction_value = price * quantity

                transaction_time = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
                interval_start = transaction_time.replace(microsecond=0)  # Round to the nearest second

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
            price_documents = []
            price_updates = {}  # Store latest prices for DATA collection

            for symbol, data in self.transactions.items():
                output_data = {
                    "timestamp": timestamp,
                    "symbol": symbol,
                    "source": "binance",
                    "baseCurrency": symbol[:-4],
                    "quoteCurrency": symbol[-4:]
                }

                has_trades = False
                total_value = 0
                total_quantity = 0

                for side in ['buy', 'sell']:
                    trades = data[side]
                    if trades:
                        has_trades = True
                        side_quantity = sum(trade['quantity'] for trade in trades)
                        side_value = sum(trade['price'] * trade['quantity'] for trade in trades)
                        total_quantity += side_quantity
                        total_value += side_value
                        output_data.update({
                            f"{side}_count": len(trades),
                            f"{side}_total_quantity": side_quantity,
                            f"{side}_total_value": side_value,
                            f"{side}_min_price": min(trade['price'] for trade in trades),
                            f"{side}_max_price": max(trade['price'] for trade in trades),
                            f"{side}_avg_price": side_value / side_quantity,
                        })
                        print(f"{symbol} {side}: {len(trades)} trades, total value: {side_value}")
                    else:
                        print(f"{symbol} {side}: No trades")

                if has_trades:
                    documents.append(output_data)

                    # Calculate weighted average price
                    weighted_avg_price = total_value / total_quantity

                    # Create price document
                    price_document = {
                        "timestamp": timestamp,
                        "symbol": symbol,
                        "source": "binance",
                        "price": weighted_avg_price,
                        "baseCurrency": symbol[:-4],
                        "quoteCurrency": symbol[-4:]
                    }
                    price_documents.append(price_document)

                    # Store price for DATA collection update
                    token_symbol = symbol[:-4].lower()  # Remove USDT and convert to lowercase
                    if token_symbol not in price_updates:
                        price_updates[token_symbol] = weighted_avg_price

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
                            "source": "binance",
                            "baseCurrency": trade['baseCurrency'],
                            "quoteCurrency": trade['quoteCurrency']
                        })

            # Regular data inserts
            if documents:
                self.mongo_helper.set_collection(self.stats_collection)
                regular_insert_result = await self.bulk_insert(documents)
                if regular_insert_result:
                    print(f"Successfully inserted {len(documents)} documents into {self.stats_collection}")
                else:
                    print(f"No result returned from bulk insert into {self.stats_collection}")
            else:
                print("No trades to insert for this interval")

            # Insert price documents
            if price_documents:
                self.mongo_helper.set_collection(self.prices_collection)
                price_insert_result = await self.bulk_insert(price_documents)
                if price_insert_result:
                    print(f"Successfully inserted {len(price_documents)} documents into {self.prices_collection}")
                else:
                    print(f"No result returned from bulk insert into {self.prices_collection}")

            # Update DATA collection with latest prices
            if price_updates:
                self.mongo_helper.set_collection("coingecko_data")
                for token_symbol, price in price_updates.items():
                    try:
                        update_result = await self.mongo_helper.collection.update_one(
                            {"symbol": token_symbol},
                            {
                                "$set": {
                                    "prices.binance": price,
                                    "last_updated_binance": timestamp
                                }
                            }
                        )
                        if update_result.modified_count > 0:
                            print(f"Updated price for {token_symbol} in DATA collection: {price}")
                        else:
                            print(f"No document found for {token_symbol} in DATA collection")
                    except Exception as e:
                        print(f"Error updating DATA collection for {token_symbol}: {e}")

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

# main.py
async def main(db_name, stats_collection, big_transactions_collection, prices_collection, pairs):
    try:
        mongo_helper = AsyncMongoDBHelper(db_name)

        # Set codec options to use timezone-aware datetimes
        codec_options = CodecOptions(tz_aware=True, tzinfo=timezone.utc)
        mongo_helper.set_codec_options(codec_options)
        
        # Set the main collection
        mongo_helper.set_collection(stats_collection)

        binance_ws = BinanceWebSocket(pairs, mongo_helper, stats_collection, big_transactions_collection, prices_collection)
        
        print("Starting Binance WebSocket connection")
        await binance_ws.connect()
    except Exception as e:
        print(f"Fatal error in main: {e}")
    finally:
        if 'binance_ws' in locals():
            await binance_ws.close()
        if 'mongo_helper' in locals():
            await mongo_helper.close_connection()
        print("Binance WebSocket connection closed")

if __name__ == "__main__":
    # Configuration
    db_name = "testing"  # Your database name
    stats_collection = "binance_trades"  # Collection for regular trade statistics
    big_transactions_collection = "big_transactions"  # Collection for large trades
    prices_collection = "prices"  # New collection for price data
    pairs = ["BTCUSDT", "ETHUSDT"]  # Add more trading pairs as needed
    
    # Run the application
    asyncio.run(main(db_name, stats_collection, big_transactions_collection))
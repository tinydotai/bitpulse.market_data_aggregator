import asyncio
import websockets
import json
import os
from service.logger import get_logger  
from datetime import datetime, timezone
from service.async_mongo import AsyncMongoDBHelper
from bson import CodecOptions
from prometheus_client import start_http_server, Counter, Gauge

logger = get_logger('BinanceWebSocket')

# Prometheus metrics
TRANSACTIONS_TOTAL = Counter('binance_transactions_total', 'Total number of transactions', ['symbol', 'side'])
TRANSACTION_VALUE = Counter('binance_transaction_value_total', 'Total value of transactions', ['symbol', 'side'])
PRICE_GAUGE = Gauge('binance_price', 'Current price', ['symbol'])
BIG_TRANSACTIONS = Counter('binance_big_transactions_total', 'Number of big transactions', ['symbol', 'side'])

class BinanceWebSocket:
    def __init__(self, pairs, mongo_helper: AsyncMongoDBHelper, stats_collection, big_transactions_collection):
        self.pairs = pairs
        self.base_url = "wss://stream.binance.com:9443/ws"
        self.transactions = {}
        self.big_transactions = {}
        self.current_interval = None
        self.interval_seconds = 10
        self.mongo_helper = mongo_helper
        self.max_retries = 10
        self.initial_retry_delay = 5
        self.max_retry_delay = 60  # 1 minute
        self.BIG_TRANSACTION_THRESHOLD = 10000  # $10,000 threshold for big transactions
        self.stats_collection = stats_collection
        self.big_transactions_collection = big_transactions_collection 
        start_http_server(8000)  # Prometheus will scrape metrics from this port

    async def connect(self):
        stream_names = [f"{pair.lower()}@trade" for pair in self.pairs]
        ws_url = f"{self.base_url}/{'/'.join(stream_names)}"

        retry_count = 0
        retry_delay = self.initial_retry_delay

        while retry_count < self.max_retries:
            try:
                logger.info(f"Attempting to connect to Binance WebSocket: {ws_url}")
                async with websockets.connect(ws_url) as websocket:
                    logger.info("Successfully connected to Binance WebSocket")
                    
                    subscribe_msg = {
                        "method": "SUBSCRIBE",
                        "params": stream_names,
                        "id": 1
                    }
                    await websocket.send(json.dumps(subscribe_msg))
                    logger.info(f"Sent subscription request for {len(stream_names)} pairs")

                    # Wait for subscription confirmation
                    subscription_response = await websocket.recv()
                    subscription_data = json.loads(subscription_response)
                    if subscription_data.get('result') is None:
                        logger.info(f"Successfully subscribed to all streams: {stream_names}")
                    else:
                        logger.warning(f"Unexpected subscription response: {subscription_data}")

                    retry_count = 0  # Reset retry count on successful connection
                    retry_delay = self.initial_retry_delay  # Reset retry delay

                    while True:
                        try:
                            response = await asyncio.wait_for(websocket.recv(), timeout=30)  # 30 second timeout
                            transaction = json.loads(response)
                            await self.handle_message(transaction)
                        except asyncio.TimeoutError:
                            logger.debug("No data received in 30 seconds, sending ping")
                            pong_waiter = await websocket.ping()
                            await asyncio.wait_for(pong_waiter, timeout=10)
                            logger.debug("Received pong, connection still alive")
                        except json.JSONDecodeError as e:
                            logger.error(f"JSON decode error: {e}. Response: {response}")
                        except Exception as e:
                            logger.error(f"Error handling message: {e}")
                            raise  # Re-raise to trigger reconnection

            except (websockets.exceptions.ConnectionClosed, 
                    websockets.exceptions.WebSocketException, 
                    asyncio.TimeoutError) as e:
                retry_count += 1
                logger.error(f"WebSocket error (attempt {retry_count}/{self.max_retries}): {e}")

                if retry_count >= self.max_retries:
                    logger.error("Max retries reached. Exiting.")
                    return

                logger.info(f"Retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, self.max_retry_delay)  # Exponential backoff

            except Exception as e:
                logger.error(f"Unexpected error in WebSocket connection: {e}")
                return

        logger.error("Failed to establish a stable connection. Exiting.")

    async def handle_message(self, message):
        try:
            if 'e' in message and message['e'] == 'trade':
                symbol = message['s']
                price = float(message['p'])
                quantity = float(message['q'])
                timestamp = int(message['T'])
                is_buyer_maker = message['m']

                trade_side = "sell" if is_buyer_maker else "buy"
                transaction_value = price * quantity

                transaction_time = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
                interval_start = transaction_time.replace(second=transaction_time.second // self.interval_seconds * self.interval_seconds, microsecond=0)

                if self.current_interval is None or interval_start > self.current_interval:
                    if self.current_interval is not None:
                        await self.process_and_store_data()
                    self.current_interval = interval_start
                    self.transactions = {pair: {'buy': [], 'sell': []} for pair in self.pairs}
                    self.big_transactions = {pair: {'buy': [], 'sell': []} for pair in self.pairs}

                self.transactions[symbol][trade_side].append({'price': price, 'quantity': quantity})

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
                        'timestamp': transaction_time
                    })


        except KeyError as e:
            logger.error(f"KeyError in handle_message: {e}. Message: {message}")
        except Exception as e:
            logger.error(f"Unexpected error in handle_message: {e}")

    async def process_and_store_data(self):
        try:
            timestamp = self.current_interval.replace(tzinfo=timezone.utc)
            logger.info("-"*50)
            logger.info(f"Processing data for interval: {timestamp}")

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
                            f"{side}_avg_price": total_value / total_quantity,
                            "source": "binance"
                        })
                        logger.debug(f"{symbol} {side}: {len(trades)} trades, total value: {total_value}")
                    else:
                        output_data.update({
                            f"{side}_count": 0,
                            f"{side}_total_quantity": 0,
                            f"{side}_total_value": 0,
                            f"{side}_min_price": None,
                            f"{side}_max_price": None,
                            f"{side}_avg_price": None,
                            "source": "binance"
                        })
                        logger.debug(f"{symbol} {side}: No trades")

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
                            "source": "binance"
                        })
                    logger.debug(f"{symbol} big {side} transactions: {len(big_trades[side])}")


            # Insert regular transactions
            self.mongo_helper.set_collection(self.stats_collection)
            regular_insert_result = await self.bulk_insert(documents)
            if regular_insert_result:
                logger.info(f"Successfully inserted {len(documents)} documents into {self.stats_collection}")
            else:
                logger.warning(f"No result returned from bulk insert into {self.stats_collection}")

            # Insert big transactions
            if big_transaction_documents:
                big_insert_result = await self.bulk_insert_big_transactions(big_transaction_documents)
                if big_insert_result:
                    logger.info(f"Successfully inserted {len(big_transaction_documents)} big transaction documents into {self.big_transactions_collection}")
                else:
                    logger.warning(f"No result returned from bulk insert into {self.big_transactions_collection}")
            else:
                logger.info("No big transactions to insert")

            logger.info("Data processing and storage completed successfully")
        except Exception as e:
            logger.error(f"Error in process_and_store_data: {e}", exc_info=True)
        finally:
            logger.info("-"*50)

    async def bulk_insert(self, documents):
        try:
            result = await self.mongo_helper.insert_many(documents)
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
            self.mongo_helper.set_collection(self.big_transactions_collection)
            result = await self.mongo_helper.insert_many(documents)
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

async def main(db_name, stats_collection, big_transactions_collection, pairs):
    try:
        mongo_helper = AsyncMongoDBHelper(db_name)

        # Set codec options to use timezone-aware datetimes
        codec_options = CodecOptions(tz_aware=True, tzinfo=timezone.utc)
        mongo_helper.set_codec_options(codec_options)
        
        # Set the main collection
        mongo_helper.set_collection(stats_collection)

        binance_ws = BinanceWebSocket(pairs, mongo_helper, stats_collection, big_transactions_collection)
        
        logger.info("Starting Binance WebSocket connection")
        await binance_ws.connect()
    except Exception as e:
        logger.error(f"Fatal error in main: {e}")
    finally:
        if 'binance_ws' in locals():
            await binance_ws.close()
        if 'mongo_helper' in locals():
            await mongo_helper.close_connection()
        logger.info("Binance WebSocket connection closed")

if __name__ == "__main__":
    # You should define db_name, stats_collection, big_transactions_collection, and pairs here
    # or pass them as command-line arguments
    db_name = "your_db_name"
    stats_collection = "your_stats_collection"
    big_transactions_collection = "your_big_transactions_collection"
    pairs = ["BTCUSDT"]  # Add more pairs as needed
    
    asyncio.run(main(db_name, stats_collection, big_transactions_collection, pairs))
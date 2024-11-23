import asyncio
import websockets
import json
from datetime import datetime, timezone
from redis import Redis

class RedisHelper:
    def __init__(self, host='localhost', port=6379, db=0):
        self.redis_client = Redis(
            host=host,
            port=port,
            db=db,
            decode_responses=True
        )

    def store_trade_stats(self, key, data):
        self.redis_client.hset(key, mapping=self._prepare_data_for_redis(data))

    def store_big_transaction(self, transaction):
        # Store big transactions in a sorted set, scored by timestamp
        score = int(transaction['timestamp'].timestamp())
        self.redis_client.zadd(
            f"big_transactions:{transaction['symbol']}",
            {json.dumps(self._prepare_data_for_redis(transaction)): score}
        )

    def get_latest_price(self, symbol):
        return self.redis_client.get(f"price:{symbol}")

    def close(self):
        self.redis_client.close()

    def _prepare_data_for_redis(self, data):
        # Convert datetime objects to timestamps and ensure all values are strings
        prepared_data = {}
        for key, value in data.items():
            if isinstance(value, datetime):
                prepared_data[key] = int(value.timestamp())
            elif isinstance(value, (int, float)):
                prepared_data[key] = str(value)
            else:
                prepared_data[key] = str(value)
        return prepared_data

class BinanceWebSocket:
    def __init__(self, pairs, redis_helper: RedisHelper):
        self.pairs = pairs
        self.base_url = "wss://stream.binance.com:9443/ws"
        self.transactions = {}
        self.big_transactions = {}
        self.current_interval = None
        self.interval_seconds = 1
        self.redis_helper = redis_helper
        self.max_retries = 10
        self.initial_retry_delay = 5
        self.max_retry_delay = 60
        self.BIG_TRANSACTION_THRESHOLD = 10000  # $10,000 threshold

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

                    subscription_response = await websocket.recv()
                    subscription_data = json.loads(subscription_response)
                    if subscription_data.get('result') is None:
                        print(f"Successfully subscribed to all streams: {stream_names}")
                    else:
                        print(f"Unexpected subscription response: {subscription_data}")

                    retry_count = 0
                    retry_delay = self.initial_retry_delay

                    while True:
                        try:
                            response = await asyncio.wait_for(websocket.recv(), timeout=30)
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
                            raise

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
                retry_delay = min(retry_delay * 2, self.max_retry_delay)

            except Exception as e:
                print(f"Unexpected error in WebSocket connection: {e}")
                return

    async def handle_message(self, message):
        try:
            if 'e' in message and message['e'] == 'trade':
                symbol = message['s']
                price = float(message['p'])
                quantity = float(message['q'])
                timestamp = int(message['T'])
                is_buyer_maker = message['m']

                base_currency = symbol[:-4]
                quote_currency = symbol[-4:]

                trade_side = "sell" if is_buyer_maker else "buy"
                transaction_value = price * quantity

                transaction_time = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
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

                # Store latest price in Redis
                self.redis_helper.redis_client.set(f"price:{symbol}", str(price))

                # Track big transactions
                if transaction_value >= self.BIG_TRANSACTION_THRESHOLD:
                    self.big_transactions[symbol][trade_side].append({
                        'price': price,
                        'quantity': quantity,
                        'value': transaction_value,
                        'timestamp': transaction_time,
                        'baseCurrency': base_currency,
                        'quoteCurrency': quote_currency
                    })

        except Exception as e:
            print(f"Error in handle_message: {e}")

    async def process_and_store_data(self):
        try:
            timestamp = self.current_interval.replace(tzinfo=timezone.utc)
            print("-"*50)
            print(f"Processing data for interval: {timestamp}")

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
                    # Store trade stats in Redis
                    key = f"trade_stats:{symbol}:{int(timestamp.timestamp())}"
                    self.redis_helper.store_trade_stats(key, output_data)

                    # Store big transactions
                    for side in ['buy', 'sell']:
                        for trade in self.big_transactions[symbol][side]:
                            self.redis_helper.store_big_transaction({
                                **trade,
                                'side': side,
                                'symbol': symbol,
                                'source': 'binance'
                            })

            print("Data processing and storage completed successfully")
        except Exception as e:
            print(f"Error in process_and_store_data: {e}")
        finally:
            print("-"*50)

    async def close(self):
        if self.transactions:
            await self.process_and_store_data()

async def main():
    try:
        # Initialize Redis helper
        redis_helper = RedisHelper(
            host='localhost',  # Change this if Redis is on different host
            port=6379,        # Default Redis port
            db=0             # Default Redis database
        )
        
        # Trading pairs to monitor
        pairs = ["BTCUSDT", "ETHUSDT"]  # Add more pairs as needed
        
        # Initialize and start WebSocket client
        binance_ws = BinanceWebSocket(pairs, redis_helper)
        print("Starting Binance WebSocket connection")
        await binance_ws.connect()
        
    except Exception as e:
        print(f"Fatal error in main: {e}")
    finally:
        if 'binance_ws' in locals():
            await binance_ws.close()
        if 'redis_helper' in locals():
            redis_helper.close()
        print("Binance WebSocket connection closed")

if __name__ == "__main__":
    asyncio.run(main())
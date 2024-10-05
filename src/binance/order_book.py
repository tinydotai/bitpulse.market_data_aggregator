import asyncio
import json
import aiohttp
import websockets
import logging
import datetime
from pprint import pformat
import argparse
from service.mongo import MongoConnector

# MongoDB Collections
CRYPTO_COLL = "cryptos"
ORDER_BOOK_STATS = "order_book_stats"

class CustomFormatter(logging.Formatter):
    reset = "\x1b[0m"
    FORMATS = {
        logging.DEBUG: "%(asctime)s - %(levelname)s - %(message)s",
        logging.INFO: "%(asctime)s - %(levelname)s - %(message)s",
        logging.WARNING: "\x1b[43m%(asctime)s - %(levelname)s - %(message)s" + reset,
        logging.ERROR: "\x1b[41m%(asctime)s - %(levelname)s - %(message)s" + reset,
        logging.CRITICAL: "\x1b[45m%(asctime)s - %(levelname)s - %(message)s" + reset,
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)

class OrderBookManager:
    def __init__(self, crypto, mongo_client):
        self.crypto = crypto
        self.mongo_client = mongo_client
        self.symbol = None
        self.min_stats = {}
        self.current_minute = datetime.datetime.utcnow().minute

    async def initialize(self):
        crypto_entity = self.mongo_client.find_one(CRYPTO_COLL, {"name": self.crypto.lower()})
        if not crypto_entity:
            raise ValueError(f"Cryptocurrency {self.crypto} not found in database")
        self.symbol = crypto_entity["symbol"] + "usdt"
        self.order_book = {
            'bids': {},
            'asks': {}
        }

    async def fetch_depth_snapshot(self):
        url = f"https://api.binance.com/api/v3/depth?symbol={self.symbol.upper()}&limit=1000"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status != 200:
                        logging.error(f"Failed to fetch snapshot: {response.status} - {await response.text()}")
                        return None
                    snapshot = await response.json()
                    if 'lastUpdateId' not in snapshot:
                        logging.error("Snapshot missing 'lastUpdateId'")
                        return None
                    return snapshot
        except Exception as e:
            logging.error(f"Exception during fetch: {e}")
            return None

    def apply_update(self, update):
        for bid in update.get('b', []):
            price, qty = bid
            if float(qty) == 0:
                self.order_book['bids'].pop(price, None)
            else:
                self.order_book['bids'][price] = qty

        for ask in update.get('a', []):
            price, qty = ask
            if float(qty) == 0:
                self.order_book['asks'].pop(price, None)
            else:
                self.order_book['asks'][price] = qty

    def calculate_order_book_stats(self):
        best_bid = max(self.order_book['bids'], key=lambda x: float(x), default=None)
        best_ask = min(self.order_book['asks'], key=lambda x: float(x), default=None)
        total_bid_volume = sum(float(qty) for qty in self.order_book['bids'].values())
        total_ask_volume = sum(float(qty) for qty in self.order_book['asks'].values())
        num_bids = len(self.order_book['bids'])
        num_asks = len(self.order_book['asks'])
        current_time = datetime.datetime.now().replace(second=0, microsecond=0)
        return {
            'crypto_name': self.crypto,
            'symbol': self.symbol,
            'best_bid': float(best_bid) if best_bid else None,
            'best_ask': float(best_ask) if best_ask else None,
            'total_bid_volume': total_bid_volume,
            'total_ask_volume': total_ask_volume,
            'num_bids': num_bids,
            'num_asks': num_asks,
            'time': current_time
        }

    async def update_database_periodically(self, interval):
        while True:
            if self.min_stats:
                for stat in self.min_stats.values():
                    # Uncomment and complete this section as needed
                    # self.mongo_client.find_one_and_replace_or_insert(
                    #     ORDER_BOOK_STATS,
                    #     {"crypto_name": stat["crypto_name"], "time": stat["time"], "symbol": stat["symbol"]},
                    #     stat, upsert=True
                    # )
                    logging.info(f"Updated database with the latest stats: {pformat(stat)}")
            await asyncio.sleep(interval)

    async def manage_order_book(self):
        await self.initialize()

        while True:
            try:
                snapshot = await self.fetch_depth_snapshot()
                if not snapshot:
                    logging.error("Failed to fetch initial snapshot.")
                    await asyncio.sleep(5)
                    continue

                last_update_id = snapshot['lastUpdateId']
                self.order_book['bids'] = {price: qty for price, qty in snapshot['bids']}
                self.order_book['asks'] = {price: qty for price, qty in snapshot['asks']}
                logging.info('Fetched initial order book snapshot')

                ws_url = f"wss://stream.binance.com:9443/ws/{self.symbol.lower()}@depth"
                async with websockets.connect(ws_url) as websocket:
                    logging.info('Connected to WebSocket')
                    first_update = True

                    db_task = asyncio.create_task(self.update_database_periodically(10))

                    while True:
                        data = await websocket.recv()
                        update = json.loads(data)

                        if first_update and (update['U'] <= last_update_id + 1 and update['u'] >= last_update_id + 1):
                            first_update = False
                        if not first_update and update['U'] != last_update_id + 1:
                            continue

                        self.apply_update(update)
                        last_update_id = update['u']
                        stats = self.calculate_order_book_stats()

                        if stats["time"] in self.min_stats:
                            min_stat = self.min_stats[stats["time"]]
                            min_stat["total_bid_volume"] += stats["total_bid_volume"]
                            min_stat["total_ask_volume"] += stats["total_ask_volume"]
                            min_stat["num_bids"] += stats["num_bids"]
                            min_stat["num_asks"] += stats["num_asks"]

                            if min_stat["best_bid"] < stats["best_bid"]:
                                min_stat["best_bid"] = stats["best_bid"]

                            if min_stat["best_ask"] > stats["best_ask"]:
                                min_stat["best_ask"] = stats["best_ask"]
                        else:
                            self.min_stats[stats["time"]] = stats

                        new_minute = datetime.datetime.utcnow().minute
                        if new_minute != self.current_minute:
                            logging.info(f"{pformat(self.min_stats)}")
                            if len(self.min_stats) > 5:
                                sorted_keys = sorted(self.min_stats)
                                for key in sorted_keys[:-2]:
                                    del self.min_stats[key]
                                logging.warning("Cleaning service has finished")

                            self.current_minute = new_minute

            except websockets.ConnectionClosed as e:
                logging.error(f"WebSocket connection closed: {e}")
            except Exception as e:
                logging.error(f"Error: {e}")
            finally:
                db_task.cancel()
                logging.info("Attempting to reconnect in 3 seconds...")
                await asyncio.sleep(3)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--crypto", type=str, required=True, help="Provide the cryptocurrency symbol")
    args = parser.parse_args()

    # MongoDB Client
    mongo_client = MongoConnector()
    
    # Logger setup
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    ch.setFormatter(CustomFormatter())
    logger.addHandler(ch)

    order_book_manager = OrderBookManager(args.crypto, mongo_client)
    asyncio.run(order_book_manager.manage_order_book())


# REQUIRES MODIFICATIONS
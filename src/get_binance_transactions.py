import asyncio
from binance.transactions import main
from binance.pairs import usdt_pairs

DB_NAME = "testing"
COLLECTION_NAME = "TEST_BINANCE"


if __name__ == "__main__":
    asyncio.run(main(DB_NAME, COLLECTION_NAME, usdt_pairs[:2]))

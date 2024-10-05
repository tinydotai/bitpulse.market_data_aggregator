# market_data_aggregator

sudo docker run -v /home/luka/binance_data:/app/binance_data -v /home/luka/logs:/app/logs -d l0rtk/binance_transactions

off-chain data aggregator

# Binance WebSocket Transactions System

This system collects real-time trade data from Binance using WebSocket connections and stores aggregated data in MongoDB. Here's an overview of how it works:

## 1. Connection and Subscription

- The system connects to Binance's WebSocket API.
- It subscribes to trade streams for multiple cryptocurrency pairs (e.g., BTCUSDT, ETHUSDT).
- The connection is maintained with automatic reconnection and exponential backoff in case of disconnections.

## 2. Data Reception

- The system receives real-time trade data for each subscribed pair.
- Each trade message contains information such as:
  - Symbol (e.g., BTCUSDT)
  - Price
  - Quantity
  - Timestamp
  - Trade side (buy or sell)

## 3. Data Aggregation

- Trades are aggregated in 10-second intervals.
- For each interval and each symbol, the system tracks:
  - Number of trades (buy and sell separately)
  - Total quantity traded (buy and sell)
  - Total value traded (buy and sell)
  - Minimum price
  - Maximum price
  - Average price

## 4. Data Storage

- At the end of each 10-second interval, the aggregated data is prepared for storage.
- A document is created for each symbol, containing all the aggregated metrics.
- These documents are bulk inserted into MongoDB.

## 5. Error Handling

- The system includes robust error handling:
  - WebSocket connection issues are managed with retries and backoff.
  - JSON parsing errors are logged.
  - Database insertion errors are handled, with fallback to individual inserts if bulk insert fails.

## 6. Logging

- The system logs important events and errors, including:
  - Connection status
  - Data insertion results
  - Any errors or exceptions that occur

## 7. Graceful Shutdown

- On shutdown, any remaining data in the current interval is processed and stored.

## Usage

To use this system:

1. Ensure all dependencies are installed.
2. Configure the MongoDB connection.
3. Specify the cryptocurrency pairs you want to track.
4. Run the main script to start collecting and storing data.

This system provides a robust way to collect and store real-time trading data from Binance, allowing for further analysis and processing of market activities.

# Docker Setup Guide for Binance Transactions Project

This guide will walk you through the process of setting up and running the Binance Transactions project using Docker.

## Prerequisites

- Docker installed on your machine. If you haven't installed Docker yet, please follow the official [Docker installation guide](https://docs.docker.com/get-docker/).

## Setup Steps

1. **Prepare Your Project Directory**
   Ensure your project directory structure matches the following:

   ```
   project_root/
   ├── src/
   │   ├── binance/
   │   └── service/
   │       └── get_binance_transactions.py
   ├── .env
   ├── requirements.txt
   └── binance.transactions.Dockerfile
   ```

2. **Create the Dockerfile**
   Create a file named `binance.transactions.Dockerfile` in the root directory of your project with the following content:

   ```dockerfile
   FROM python:3.9-slim

   WORKDIR /app

   COPY src /app/src
   COPY .env /app/.env
   COPY requirements.txt /app/requirements.txt

   RUN pip install --no-cache-dir -r requirements.txt

   ENV PYTHONPATH=/app/src

   CMD ["python", "/app/src/get_binance_transactions.py"]
   ```

3. **Build the Docker Image**
   Run the following command in the terminal from your project's root directory:

   ```
   docker build -t binance-transactions -f binance.transactions.Dockerfile .
   ```

   This command builds a Docker image named `binance-transactions` using the `binance.transactions.Dockerfile`.

4. **Run the Docker Container**
   To run the container, use the following command:

   ```
   docker run --name binance-transactions -v $(pwd)/logs:/app/logs binance-transactions python /app/src/get_binance_transactions.py db_name=your_db_name stats_collection=your_stats_collection big_transactions_collection=your_big_transactions_collection pairs=BTCUSDT,ETHUSDT,SOLUSDT
   ```

   for real

   ```
   docker run --name binance-transactions -v $(pwd)/logs:/app/logs -d l0rtk/bitpulse_binance_transactions:0.1.2 python /app/src/get_binance_transactions.py db_name=bitpulse stats_collection=transactions_stats big_transactions_collection=big_transactions pairs=BTCUSDT,ETHUSDT,SOLUSDT
   ```

   Replace `your_db_name`, `your_stats_collection`, `your_big_transactions_collection`, and `PAIR1,PAIR2,PAIR3` with your desired values.

   For example:

   ```
   docker run --name binance-transactions -v $(pwd)/logs:/app/logs  binance-transactions python /app/src/get_binance_transactions.py db_name=test stats_collection=test_stats big_transactions_collection=test_big_transactions pairs=1000SATSUSDT,1INCHUSDT,ACAUSDT
   ```

5. **Running with Host Network** (if needed)
   If your application needs to connect to services running on your host machine (like a local MongoDB instance), use the `--network host` option:
   ```
   docker run --name binance-transactions -v $(pwd)/logs:/app/logs --network host binance-transactions python /app/src/service/get_binance_transactions.py db_name=test stats_collection=test_stats big_transactions_collection=test_big_transactions pairs=1000SATSUSDT,1INCHUSDT,ACAUSDT
   ```

## Troubleshooting

- If you encounter any "module not found" errors, ensure that your Python files are importing modules correctly, considering the `src` directory as the root.
- Make sure your `.env` file is present in the project root and contains all necessary environment variables.
- If you make changes to your code, remember to rebuild the Docker image before running a new container.

## Additional Notes

- The `PYTHONPATH` is set to `/app/src` in the Dockerfile to ensure Python can find your modules.
- Adjust the pairs in the run command as needed for different cryptocurrency pairs.
- For any changes in dependencies, update your `requirements.txt` file and rebuild the Docker image.

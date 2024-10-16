import requests
import json

def get_kucoin_top_50_symbols():
    # KuCoin API endpoint for market data
    url = "https://api.kucoin.com/api/v1/market/allTickers"

    try:
        # Send GET request to the API
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for bad status codes

        # Parse the JSON response
        data = response.json()

        # Extract the ticker data
        tickers = data['data']['ticker']

        # Sort tickers by 24h volume in descending order
        sorted_tickers = sorted(tickers, key=lambda x: float(x['volValue']), reverse=True)

        # Get the top 50 symbols
        top_50_symbols = [ticker['symbol'] for ticker in sorted_tickers[:50]]

        return top_50_symbols

    except requests.RequestException as e:
        print(f"An error occurred while fetching data: {e}")
        return []

# Fetch and print the top 50 crypto symbols
top_50 = get_kucoin_top_50_symbols()
pairs = ''
print("Top 50 KuCoin crypto symbols by 24h volume:")
for i, symbol in enumerate(top_50, 1):
    pairs += symbol + ','
print(pairs[:-1])

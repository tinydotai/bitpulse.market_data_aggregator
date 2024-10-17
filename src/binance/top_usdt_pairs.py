import requests
from typing import List, Dict
from datetime import datetime

def get_binance_top_50_usdt_pairs() -> List[Dict]:
    url = "https://api.coingecko.com/api/v3/exchanges/binance/tickers"
    params = {
        "order": "volume_desc",
        "depth": "false"
    }
    
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        usdt_pairs = [pair for pair in data['tickers'] if pair['target'] == 'USDT']
        return usdt_pairs[:50]
    else:
        raise Exception(f"Failed to fetch data: HTTP {response.status_code}")

def format_datetime(dt_string: str) -> str:
    dt = datetime.strptime(dt_string, "%Y-%m-%dT%H:%M:%S%z")
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def main():
    try:
        top_50_usdt_pairs = get_binance_top_50_usdt_pairs()
        top_list = []
        
        print("Top 50 USDT Trading Pairs on Binance by Volume:")
        for idx, pair in enumerate(top_50_usdt_pairs, 1):
            top_list.append({
                "symbol" : pair['base'],
                "id": pair['coin_id']
            })
    
        print(top_list)
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()
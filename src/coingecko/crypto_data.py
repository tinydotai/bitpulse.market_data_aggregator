import requests
import json
import time
from collections import defaultdict

def fetch_all_crypto_data():
    all_cryptos = []
    url = "https://api.coingecko.com/api/v3/coins/markets"
    page = 1
    
    while True:
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 250,
            "page": page,
            "sparkline": False
        }
        
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            if not data:  # If the response is empty, we've reached the end
                break
            
            all_cryptos.extend(data)
            
            print(f"Fetched page {page}, total cryptos: {len(all_cryptos)}")
            page += 1
            
            # Sleep for a short time to avoid hitting rate limits
            time.sleep(1)
        else:
            print(f"Error fetching data: {response.status_code}")
            break
    
    return all_cryptos

def group_cryptos(data):
    by_name = defaultdict(list)
    by_symbol = defaultdict(list)

    for crypto in data:
        by_name[crypto['name']].append(crypto)
        by_symbol[crypto['symbol'].upper()].append(crypto)

    return dict(by_name), dict(by_symbol)

def save_to_json(data, filename):
    with open(filename, 'w', encoding='utf-8') as jsonfile:
        json.dump(data, jsonfile, indent=2, ensure_ascii=False)

def main():
    print("Fetching all cryptocurrency data from CoinGecko...")
    all_crypto_data = fetch_all_crypto_data()

    print("Grouping cryptocurrencies by name and symbol...")
    by_name, by_symbol = group_cryptos(all_crypto_data)

    print(f"Saving {len(by_name)} name groups to 'cryptos_by_name.json'...")
    save_to_json(by_name, 'cryptos_by_name.json')

    print(f"Saving {len(by_symbol)} symbol groups to 'cryptos_by_symbol.json'...")
    save_to_json(by_symbol, 'cryptos_by_symbol.json')

    print("Done! Check 'cryptos_by_name.json' and 'cryptos_by_symbol.json' for the results.")

if __name__ == "__main__":
    main()
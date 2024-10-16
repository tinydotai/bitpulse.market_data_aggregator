import requests
import time
from collections import defaultdict
import pprint

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

def save_to_python_file(by_name, by_symbol, filename):
    with open(filename, 'w', encoding='utf-8') as pyfile:
        pyfile.write("# This file contains cryptocurrency data grouped by name and symbol\n\n")
        
        pyfile.write("cryptos_by_name = ")
        pyfile.write(pprint.pformat(by_name, width=120, sort_dicts=False))
        
        pyfile.write("\n\ncryptos_by_symbol = ")
        pyfile.write(pprint.pformat(by_symbol, width=120, sort_dicts=False))

def main():
    print("Fetching all cryptocurrency data from CoinGecko...")
    all_crypto_data = fetch_all_crypto_data()

    print("Grouping cryptocurrencies by name and symbol...")
    by_name, by_symbol = group_cryptos(all_crypto_data)

    print("Saving grouped data to 'crypto_data.py'...")
    save_to_python_file(by_name, by_symbol, 'crypto_data.py')

    print("Done! Check 'crypto_data.py' for the results.")

if __name__ == "__main__":
    main()
import requests
import json
import time

def fetch_all_crypto_data():
    all_cryptos = {}
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
            
            for crypto in data:
                all_cryptos[crypto['id']] = crypto
            
            print(f"Fetched page {page}, total cryptos: {len(all_cryptos)}")
            page += 1
            
            # Sleep for a short time to avoid hitting rate limits
            time.sleep(1)
        else:
            print(f"Error fetching data: {response.status_code}")
            break
    
    return all_cryptos

def save_to_json(data, filename):
    with open(filename, 'w', encoding='utf-8') as jsonfile:
        json.dump(data, jsonfile, indent=2, ensure_ascii=False)

def main():
    print("Fetching all cryptocurrency data from CoinGecko...")
    all_crypto_data = fetch_all_crypto_data()

    print(f"Saving {len(all_crypto_data)} cryptocurrencies to JSON file...")
    save_to_json(all_crypto_data, 'all_cryptocurrencies.json')

    print("Done! Check 'all_cryptocurrencies.json' for the results.")

if __name__ == "__main__":
    main()
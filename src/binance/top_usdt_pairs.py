import requests
from typing import List, Dict
from operator import itemgetter

def fetch_data(url: str) -> Dict:
    response = requests.get(url)
    response.raise_for_status()  # Raises an HTTPError for bad responses
    return response.json()

def filter_usdt_pairs(tickers: List[Dict]) -> List[Dict]:
    return [ticker for ticker in tickers if ticker['target'] == 'USDT']

def sort_by_volume(tickers: List[Dict]) -> List[Dict]:
    return sorted(tickers, key=lambda x: float(x['converted_volume']['usd']), reverse=True)

def format_ticker(ticker: Dict) -> Dict:
    return {
        'pair': f"{ticker['base']}/USDT",
        'price': float(ticker['last']),
        'volume_usd': float(ticker['converted_volume']['usd']),
        '24h_change': ticker.get('converted_last', {}).get('usd_24h_change', 'N/A')
    }

def main():
    url = "https://api.coingecko.com/api/v3/exchanges/binance/tickers"
    
    try:
        # Fetch the data
        data = fetch_data(url)
        
        # Filter USDT pairs
        usdt_pairs = filter_usdt_pairs(data['tickers'])
        
        # Sort by volume
        sorted_pairs = sort_by_volume(usdt_pairs)
        
        # Get top 50
        top_50 = sorted_pairs[:50]
        
        # Format and print results
        print(f"{'Pair':<15} {'Price':<10} {'Volume (USD)':<15} {'24h Change':<10}")
        print("-" * 50)
        pairs = ''
        for ticker in top_50:
            formatted = format_ticker(ticker)
            print(f"{formatted['pair']:<15} {formatted['price']:<10.4f} {formatted['volume_usd']:<15,.0f} {formatted['24h_change']:<10}")
            pairs += formatted['pair'].replace('/','') + ','
        
        print(pairs[:-1])

    except requests.RequestException as e:
        print(f"An error occurred while fetching data: {e}")
    except KeyError as e:
        print(f"Error: Expected data not found in the API response. {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()
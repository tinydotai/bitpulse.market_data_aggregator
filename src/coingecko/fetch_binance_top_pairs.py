import requests
import pandas as pd
from datetime import datetime
import time

def get_coingecko_ids():
    """Fetch CoinGecko IDs and market caps for cryptocurrencies"""
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        'vs_currency': 'usd',
        'order': 'market_cap_desc',
        'per_page': 250,  # Get top 250 coins by market cap
        'page': 1,
        'sparkline': False
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        # Create mapping of symbol to ID, keeping only highest market cap version
        symbol_to_id = {}
        for coin in data:
            symbol = coin['symbol'].upper()
            market_cap = coin.get('market_cap', 0) or 0
            
            # If symbol already exists, only replace if new market cap is higher
            if symbol not in symbol_to_id or market_cap > symbol_to_id[symbol]['market_cap']:
                symbol_to_id[symbol] = {
                    'id': coin['id'],
                    'market_cap': market_cap
                }
        
        return symbol_to_id
    except requests.exceptions.RequestException as e:
        print(f"Error fetching CoinGecko IDs: {e}")
        return None

def get_exchange_info():
    """Fetch exchange information"""
    url = "https://api.binance.com/api/v3/exchangeInfo"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        symbol_info = {}
        for symbol_data in data['symbols']:
            if symbol_data['status'] == 'TRADING':  # Only include active trading pairs
                symbol_info[symbol_data['symbol']] = {
                    'baseAsset': symbol_data['baseAsset'],
                    'quoteAsset': symbol_data['quoteAsset']
                }
        return symbol_info
    except requests.exceptions.RequestException as e:
        print(f"Error fetching exchange info: {e}")
        return None

def fetch_top_usdt_pairs():
    # Fetch symbol info and CoinGecko IDs
    print("Fetching CoinGecko IDs with market caps...")
    symbol_info = get_exchange_info()
    coingecko_data = get_coingecko_ids()
    
    if not symbol_info or not coingecko_data:
        return None
    
    # Binance API endpoint for 24hr ticker
    url = "https://api.binance.com/api/v3/ticker/24hr"
    
    try:
        print("Fetching Binance trading data...")
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        usdt_pairs = []
        processed_count = 0
        
        for item in data:
            # Only process if it's a USDT pair and exists in symbol_info
            if item['symbol'].endswith('USDT') and item['symbol'] in symbol_info:
                try:
                    base_asset = symbol_info[item['symbol']]['baseAsset']
                    coin_data = coingecko_data.get(base_asset, {'id': 'N/A', 'market_cap': 0})
                    coingecko_id = coin_data['id']
                    market_cap = coin_data['market_cap']
                    
                    pair_info = {
                        'symbol': item['symbol'].replace('USDT', '/USDT'),
                        'base_asset': base_asset,
                        'coingecko_id': coingecko_id,
                        'market_cap_usd': market_cap,
                        'volume_usd': float(item['quoteVolume']),
                        'price': float(item['lastPrice']),
                        'price_change_pct': float(item['priceChangePercent']),
                        'high_24h': float(item['highPrice']),
                        'low_24h': float(item['lowPrice'])
                    }
                    usdt_pairs.append(pair_info)
                    processed_count += 1
                    
                    if processed_count % 10 == 0:
                        print(f"Processed {processed_count} pairs...")
                        
                except Exception as e:
                    print(f"Error processing pair {item['symbol']}: {e}")
                    continue
        
        if not usdt_pairs:
            print("No valid USDT pairs found")
            return None
            
        # Create DataFrame and sort by volume
        df = pd.DataFrame(usdt_pairs)
        df = df.sort_values('volume_usd', ascending=False).head(100)
        
        # Format the numbers for better readability
        df['volume_usd'] = df['volume_usd'].apply(lambda x: f"${x:,.2f}")
        df['market_cap_usd'] = df['market_cap_usd'].apply(lambda x: f"${x:,.2f}" if x > 0 else 'N/A')
        df['price'] = df['price'].apply(lambda x: f"${x:,.8f}")
        df['price_change_pct'] = df['price_change_pct'].apply(lambda x: f"{x:.2f}%")
        df['high_24h'] = df['high_24h'].apply(lambda x: f"${x:,.8f}")
        df['low_24h'] = df['low_24h'].apply(lambda x: f"${x:,.8f}")
        
        # Reorder columns
        columns_order = ['symbol', 'base_asset', 'coingecko_id', 'market_cap_usd', 'volume_usd', 
                        'price', 'price_change_pct', 'high_24h', 'low_24h']
        df = df[columns_order]
        
        # Reset index starting from 1
        df.index = range(1, len(df) + 1)
        
        return df
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

if __name__ == "__main__":
    print(f"Fetching top 100 USDT pairs from Binance at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    df = fetch_top_usdt_pairs()
    
    if df is not None:
        print("\nTop 100 USDT Trading Pairs by 24h Volume:")
        print(df.to_string())
        df.to_csv("binance_top_coins.csv")
import os
import requests
from concurrent.futures import ThreadPoolExecutor

# List of symbols
symbols = [
    "POL", "RENDER", "ETC", "XLM", "STX", "IMX", "AAVE", "OP", "FIL", "INJ",
    "WBETH", "FET", "FDUSD", "WIF", "WBTC", "LINK", "BCH", "DOT", "SUI", "NEAR",
    "DAI", "APT", "LTC", "TAO", "UNI", "PEPE", "ICP", "BTC", "ETH", "USDT",
    "BNB", "SOL", "USDC", "XRP", "DOGE", "TRX", "TON", "ADA", "AVAX", "SHIB",
    "TIA", "AR", "OM", "MKR", "PYTH", "JUP", "WLD", "ALGO", "JASMY", "FTM",
    "ARB", "HBAR", "VET", "ATOM", "SEI", "RUNE", "GRT", "BONK", "FLOKI", "THETA"
]

# Base URL for downloading images
base_url = "https://bin.bnbstatic.com/static/assets/logos/{}.png"

# Create the 'pairs' directory if it doesn't exist
os.makedirs("pairs", exist_ok=True)

def download_image(symbol):
    url = base_url.format(symbol)
    response = requests.get(url)
    if response.status_code == 200:
        with open(f"pairs/{symbol}.png", "wb") as f:
            f.write(response.content)
        print(f"Downloaded {symbol}.png")
    else:
        print(f"Failed to download {symbol}.png")

# Use ThreadPoolExecutor to download images concurrently
with ThreadPoolExecutor(max_workers=10) as executor:
    executor.map(download_image, symbols)

print("All downloads completed.")
# from dydx3.constants import API_HOST_MAINNET, API_HOST_GOERLI
from decouple import config

# I have money in binance...
EXCHANGE= "binance"
API_KEY = config(f"{EXCHANGE.upper()}_API_KEY")
API_SECRET = config(f"{EXCHANGE.upper()}_API_SECRET")

# Close all open positions and orders
ABORT_ALL_POSITIONS = False

# Find Cointegrated Pairs
FIND_COINTEGRATED = False

# Manage Exits
MANAGE_EXITS = False

# Place Trades
PLACE_TRADES = False

# Resolution - ccxt timeframe
RESOLUTION = "1h"


# Stats Window
WINDOW = 21

# Thresholds - Opening
MAX_HALF_LIFE = 24
ZSCORE_THRESH = 1.5
USD_PER_TRADE = 10
USD_MIN_COLLATERAL = 100

# Thresholds - Closing - this appears to be safest.
CLOSE_AT_ZSCORE_CROSS = True

QUOTE_CURRENCY = "BTC"
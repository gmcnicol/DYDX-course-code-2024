import ccxt

from constants import EXCHANGE, API_KEY, API_SECRET


# Client Class
class Client:
  def __init__(self, indexer, indexer_account, node, wallet):
    self.indexer = indexer
    self.indexer_account = indexer_account
    self.node = node
    self.wallet = wallet


# Connect to configured exchange
async def connect_exchange():
  # Create ccxt client
  ccxt_client = getattr(ccxt, EXCHANGE)({
    'apiKey': API_KEY,
    'secret': API_SECRET,
  })
  # Determine market data endpoint
  return ccxt_client

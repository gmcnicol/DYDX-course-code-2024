import ccxt.async_support as ccxt

from constants import EXCHANGE, API_KEY, API_SECRET, IS_TESTING


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
    'enableRateLimit': True,
  })
  # Determine market data endpoint
  ccxt_client.options["warnOnFetchOpenOrdersWithoutSymbol"] = False
  ccxt_client.set_sandbox_mode(IS_TESTING)
  return ccxt_client

async def close_client(client):
  await client.close()
  return
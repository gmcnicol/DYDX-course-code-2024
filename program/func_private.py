# Cancel Order
from pprint import pprint

from func_connections import close_client


async def cancel_order(client, order_id):
  pass
# Get Account
async def get_account(client):
  pass

# Get Balances
async def get_balances(client):
  balances = await client.fetch_balance()
  pprint(balances)
  return balances
# Get Open Positions
async def get_open_positions(client):
  pass


# Get Existing Order
async def get_order(client, order_id):
  pass


# Get existing open positions
async def is_open_positions(client, market):
  return False


# Check order status
async def check_order_status(client, order_id):
  return "FAILED"


# Place market order
async def place_market_order(client, market, side, size, price, reduce_only):
  client.
  pass
# Get Open Orders
async def cancel_all_orders(client, symbol):
  await client.cancel_all_orders(symbol=symbol)

# Abort all open positions
async def abort_all_positions(client):
  open_orders = await client.fetch_open_orders()
  pprint(open_orders)
  await close_client(client)
  exit(0)
  await cancel_all_orders(client)

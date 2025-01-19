# Cancel Order
async def cancel_order(client, order_id):
  pass
# Get Account
async def get_account(client):
  pass

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
  pass
# Get Open Orders
async def cancel_all_orders(client):
  pass

# Abort all open positions
async def abort_all_positions(client):
  pass

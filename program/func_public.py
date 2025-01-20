import time
from datetime import datetime, timedelta, timezone
from pprint import pprint

import numpy as np
import pandas as pd

from constants import RESOLUTION, QUOTE_CURRENCY
from func_utils import get_iso_times

# Get relevant time periods for ISO from and to
ISO_TIMES = get_iso_times()

# Get Recent Candles
async def get_candles_recent(client, market):

  # Define output
  close_prices = []

  # Protect API
  time.sleep(0.2)

  # Get Prices from DYDX V4
  response = await client.indexer.markets.get_perpetual_market_candles(
    market = market, 
    resolution = RESOLUTION
  )

  # Candles
  candles = response

  # Structure data
  for candle in candles["candles"]:
    close_prices.append(candle["close"])

  # Construct and return close price series
  close_prices.reverse()
  prices_result = np.array(close_prices).astype(np.float64)
  return prices_result


# Get Historical Candles
async def get_candles_historical(client, market):


  # Define output
  close_prices = []

# Get the timestamp 400 hours ago
  from_time = await get_from_time_for_candlesticks()

# Fetch OHLCV data using pagination
  candles = await client.fetch_ohlcv(market, timeframe=RESOLUTION, since=int(from_time.timestamp()) * 1000, limit=1000)
 # Structure data
  for candle in candles:
    close_prices.append({"datetime": candle[0], market: candle[4] })

  return close_prices


async def get_from_time_for_candlesticks():
  # get the timestamp at 400*RESOLUTION ago
  resolution_value = int(RESOLUTION[:-1])
  resolution_unit = RESOLUTION[-1]
  timestamp_ago = datetime.now(tz=timezone.utc)
  if resolution_unit == "m":
    timestamp_ago = datetime.now(tz=timezone.utc) - timedelta(minutes=400 * resolution_value)
  elif resolution_unit == "h":
    timestamp_ago = datetime.now(tz=timezone.utc) - timedelta(hours=400 * resolution_value)
  elif resolution_unit == "d":
    timestamp_ago = datetime.now(tz=timezone.utc) - timedelta(days=400 * resolution_value)
  elif resolution_unit == "M":
    timestamp_ago = datetime.now(tz=timezone.utc) - timedelta(
      days=400 * resolution_value * 30)  # Approximate month as 30 days
  elif resolution_unit == "y":
    timestamp_ago = datetime.now(tz=timezone.utc) - timedelta(
      days=400 * resolution_value * 365)  # Approximate year as 365 days
  return timestamp_ago


# Get Markets
async def get_markets(client):
  markets = await client.load_markets()
  spot_markets = []
  # pprint(markets)
  for market in markets.keys():
    market_info = markets[market]
    if ("status" in market_info["info"] and market_info["info"]["status"] == "TRADING") and market_info[
      "type"] == "spot" and market_info["quote"] == QUOTE_CURRENCY:
      spot_markets.append(market)
  pprint(spot_markets)
  return spot_markets


# Construct market prices
async def construct_market_prices(client):
  active_markets = await get_markets(client)
  # Set initial DateFrame
  close_prices = await get_candles_historical(client, active_markets[0])

  df = pd.DataFrame(close_prices)
  df.set_index("datetime", inplace=True)

  # Append other prices to DataFrame
  # You can limit the amount to loop though here to save time in development
  for (i, market) in enumerate(active_markets[1:]):
    print(f"Extracting prices for {i + 1} of {len(active_markets)} tokens for {market}")
    time.sleep(client.rateLimit / 1000)
    close_prices_add = await get_candles_historical(client, market)
    df_add = pd.DataFrame(close_prices_add)
    try:
      df_add.set_index("datetime", inplace=True)
      df = pd.merge(df, df_add, how="outer", on="datetime", copy=False)
      # print(df.head())
    except Exception as e:
      print(f"Failed to add {market} - {e}")
    del df_add

  # Check any columns with NaNs
  nans = df.columns[df.isna().any()].tolist()
  if len(nans) > 0:
    print("Dropping columns: ")
    print(nans)
    df.drop(columns=nans, inplace=True)

  # Return result
  return df

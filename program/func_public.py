import time
from datetime import datetime, timedelta, timezone
from pprint import pprint

import pandas as pd

from constants import RESOLUTION, QUOTE_CURRENCY


# Get Recent Candles
async def get_candles_recent(client, market):
    close_prices = []
    candles = await client.fetch_ohlcv(market=market, timeframe=RESOLUTION)
    return await extract_close_prices_from_candles(candles, close_prices, market)


# Get Historical Candles
async def get_candles_historical(client, market):
    close_prices = []
    from_time = await get_from_time_for_candlesticks()
    candles = await client.fetch_ohlcv(market, timeframe=RESOLUTION, since=int(from_time.timestamp()) * 1000,
                                       limit=1000)
    return await extract_close_prices_from_candles(candles, close_prices, market)


async def extract_close_prices_from_candles(candles, close_prices, market):
    for candle in candles:
        close_prices.append({"datetime": candle[0], market: candle[4]})
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
        timestamp_ago = datetime.now(tz=timezone.utc) - timedelta(days=400 * resolution_value * 30)
    elif resolution_unit == "y":
        timestamp_ago = datetime.now(tz=timezone.utc) - timedelta(days=400 * resolution_value * 365)
    return timestamp_ago


# Get Markets
async def get_markets(client):
    markets = await client.load_markets()
    spot_markets = []
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
    close_prices = await get_candles_historical(client, active_markets[0])
    df = pd.DataFrame(close_prices)
    df.set_index("datetime", inplace=True)
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
    return df

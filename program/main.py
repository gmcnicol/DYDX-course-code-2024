import time

import asyncio

from basecommander import run_migrations
from constants import ABORT_ALL_POSITIONS, FIND_COINTEGRATED, PLACE_TRADES, MANAGE_EXITS, MIGRATION_PATH, DATABASE_PATH
from func_cointegration import find_cointegrated_markets_from_all_markets
from func_connections import connect_exchange, close_client
from func_database import store_cointegrated_markets
from func_entry_pairs import open_positions
from func_exit_pairs import manage_trade_exits
from func_messaging import send_message
from func_private import abort_all_positions
from func_public import get_historical_prices_for_all_markets


# MAIN FUNCTION
async def main():
    exchange = None
    try:
        try:
            run_migrations(DATABASE_PATH, MIGRATION_PATH)

        except Exception as e:
            print("Error running migrations: ", e)
            send_message(f"Error running migrations {e}")
            exit(1)
        send_message("Bot launch successful")

        try:
            exchange = await connect_exchange()

        except Exception as e:
            print("Error connecting to exchange: ", e)
            send_message(f"Failed to connect to exchange {e}")
            await close_client(exchange)
            exit(1)

        if ABORT_ALL_POSITIONS:
            try:
                await abort_all_positions(exchange)
            except Exception as e:
                print("Error closing all positions: ", e)
                send_message(f"Error closing all positions {e}")
                exit(1)

        if FIND_COINTEGRATED:
            try:
                try:
                    df_all_market_prices = await get_historical_prices_for_all_markets(exchange)
                except Exception as e:
                    print("Error constructing market prices: ", e)
                    send_message(f"Error constructing market prices {e}")
                    exit(1)

                try:
                    df_cointegrated_markets = find_cointegrated_markets_from_all_markets(df_all_market_prices)
                    store_cointegrated_markets(df_cointegrated_markets)
                except Exception as e:
                    print("Error saving cointegrated pairs: ", e)
                    send_message(f"Error saving cointegrated pairs {e}")
                    exit(1)
            finally:
                del df_cointegrated_markets
                del df_all_market_prices

        while MANAGE_EXITS or PLACE_TRADES:
            if MANAGE_EXITS:
                try:
                    await manage_trade_exits(exchange)
                    time.sleep(1)
                except Exception as e:
                    print("Error managing exiting positions: ", e)
                    send_message(f"Error managing exiting positions {e}")
                    exit(1)

            # Place trades for opening positions
            if PLACE_TRADES:
                try:
                    await open_positions(exchange)
                except Exception as e:
                    print("Error trading pairs: ", e)
                    send_message(f"Error opening trades {e}")
                    exit(1)
    finally:
        if exchange:
            await close_client(exchange)


asyncio.run(main())

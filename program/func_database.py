import sqlite3
from datetime import datetime, timezone

from constants import DATABASE_PATH


def _connect_to_database():
    # Connect to the SQLite database
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    return conn, cursor


def open_position(exchange, pair_1, pair_2, open_position_amount):
    """
    Opens a new position in the database based on CCXT pairs.

    :param exchange: ccxt exchange object.
    :param pair_1: The first trading pair (e.g., 'ETH/BTC').
    :param pair_2: The second trading pair (e.g., 'USDT/BTC').
    :param open_position_amount: The amount of the open position.
    """
    db_path = DATABASE_PATH
    conn = None
    try:

        # Fetch market info for each pair
        market_info_1 = exchange.market(pair_1)
        market_info_2 = exchange.market(pair_2)

        # Extract base and quote currencies from market info
        base_currency_1 = market_info_1['base']
        quote_currency_1 = market_info_1['quote']
        base_currency_2 = market_info_2['base']
        quote_currency_2 = market_info_2['quote']
        # Extract base and quote currencies from CCXT pairs

        # The common quote currency is the one present in both pairs
        # In this case, BTC is the common quote currency between ETH/BTC and USDT/BTC
        if quote_currency_1 != quote_currency_2:
            raise ValueError(f"Quote currencies do not match: {quote_currency_1} vs {quote_currency_2}")

        secondary_currency = base_currency_2  # USDT is secondary when the pairs are ETH/BTC and USDT/BTC
        common_quote_currency = quote_currency_1  # BTC in this example

        # Get current UTC time

        # Connect to the SQLite database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Insert the open position into the position table
        cursor.execute("""
            INSERT INTO position (base_currency, quote_currency, secondary_currency, open_position)
            VALUES (?, ?, ?, ?, ?)
        """, (base_currency_1, common_quote_currency, secondary_currency, open_position_amount))

        # Commit the transaction
        conn.commit()

        print(
            f"Position opened: {base_currency_1}/{common_quote_currency} with secondary currency {secondary_currency}.")

    except sqlite3.Error as e:
        print(f"Error opening position: {e}")
    except ValueError as e:
        print(f"Error: {e}")
    finally:
        if conn:
            conn.close()


def close_position_by_id(db_path, position_id, closed_position_amount):
    """
    Closes an open position in the database by updating the closed position and timestamp using its ID.

    :param db_path: Path to the SQLite database file.
    :param position_id: The ID of the position to close.
    :param closed_position_amount: The amount of the closed position.
    """
    conn = None
    try:
        conn, cursor = _connect_to_database()

        # Get current UTC time
        close_timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

        # Fetch the open position by ID
        cursor.execute("""
            SELECT open_position, close_timestamp FROM position
            WHERE id = ?
        """, (position_id,))
        result = cursor.fetchone()

        if not result:
            print(f"No position found with ID {position_id}.")
            return

        open_position, existing_close_timestamp = result

        # Check if the position is already closed
        if existing_close_timestamp is not None:
            print(f"Position with ID {position_id} is already closed.")
            return

        # Ensure the closed position amount does not exceed the open position
        if closed_position_amount > open_position:
            print(f"Error: Closed position amount ({closed_position_amount}) exceeds open position ({open_position}).")
            return

        # Update the position to close it
        cursor.execute("""
            UPDATE position
            SET closed_position = ?, close_timestamp = ?
            WHERE id = ?
        """, (closed_position_amount, close_timestamp, position_id))

        # Commit the transaction
        conn.commit()

        print(f"Position with ID {position_id} successfully closed.")

    except sqlite3.Error as e:
        print(f"Error closing position: {e}")

    finally:
        # Close the database connection
        if conn:
            conn.close()

def store_cointegrated_markets(pairs):
    conn, cursor = None, None
    try:
        conn, cursor = _connect_to_database()
        pairs.to_sql('cointegrated_pairs', conn, if_exists='replace', index=False)
        conn.commit()
    finally:
        if conn:
            conn.close()

def get_cointegrated_markets():
    conn, cursor = None, None
    try:
        conn, cursor = _connect_to_database()
        cursor.execute("SELECT * FROM cointegrated_pairs")
        pairs = cursor.fetchall()
        return pairs
    finally:
        if conn:
            conn.close()
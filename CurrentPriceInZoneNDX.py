import sqlite3
import yfinance as yf

# Function to get the current price of a stock using yfinance
def get_current_price(stock_symbol):
    try:
        stock = yf.Ticker(stock_symbol)
        current_price = stock.history(period="1d")['Close'].iloc[-1]
        return current_price
    except Exception as e:
        print(f"Error fetching current price for {stock_symbol}: {e}")
        return None

# Function to fetch demand and supply zones from the database (handles both daily and hourly data)
def fetch_zones_from_db(database_name, table_name, timeframe):
    # Connect to the SQLite database
    conn = sqlite3.connect(database_name)
    cursor = conn.cursor()

    # Query to fetch demand and supply zone details (no need for timeframe filter)
    query = f"""
    SELECT symbol, start_date, end_date, price_range_high, price_range_low
    FROM { table_name }
    WHERE zone_status = 'Active';
    """ 

    zones_data = []
    try:
        # Execute the query to fetch the zones
        cursor.execute(query)

        # Fetch all results and store them
        rows = cursor.fetchall()

        # Process each row and append to zones_data
        for row in rows:
            zone = {
                'symbol': row[0],
                'start_date': row[1],
                'end_date': row[2],
                'price_range_high': row[3],
                'price_range_low': row[4],
                'timeframe': timeframe
            }
            zones_data.append(zone)

        # Close the cursor and connection
        cursor.close()
        conn.close()  # Ensure to close the connection after fetching data
        return zones_data

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        cursor.close()  # Ensure cursor is closed in case of error
        conn.close()  # Ensure connection is closed in case of error
        return []

# Function to insert the results into the StockDZSZ.db database
# Function to insert the results into the StockDZSZ.db database
def insert_results_into_db(results, database_name):
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect(database_name)
        cursor = conn.cursor()
        cursor.execute('''DROP TABLE IF EXISTS stock_price_results;''')
        # Create the table for results if it doesn't exist
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_price_results (
            symbol TEXT,
            current_price REAL,
            price_range_low REAL,
            price_range_high REAL,
            nearest_range TEXT,
            nearest_diff REAL,
            start_date TEXT,
            end_date TEXT,
            timeframe TEXT
        )
        ''')

        # Insert the results into the table
        for result in results:
            cursor.execute('''
            INSERT INTO stock_price_results (symbol, current_price, price_range_low, price_range_high, nearest_range, nearest_diff, start_date, end_date, timeframe)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                result['symbol'], result['current_price'], result['price_range_low'], result['price_range_high'], 
                result['nearest_range'], result['nearest_diff'], result['start_date'], result['end_date'], result['timeframe']
            ))

        # Commit the transaction and close the connection
        conn.commit()
        cursor.close()
        conn.close()
        print("Results successfully inserted into the database.")

    except sqlite3.Error as e:
        print(f"SQLite error during insertion: {e}")

# Function to check which stocks have their current price within the range
def check_stocks_in_range():
    # Fetch active zones from the daily database (StockDZSZ.db) and hourly database (StockTest.db)
    daily_zones = fetch_zones_from_db('../StockDZSZNDX.db', "demand_supply_zones", '1d')  # Daily data
    # hourly_zones = fetch_zones_from_db('StockTest.db', "demand_supply_zones_1hr", '1hr')  # Hourly data
    # hourly_2_zones = fetch_zones_from_db('StockTest.db', "demand_supply_zones", '2hr')  # Hourly data

    if not daily_zones :
        print("No active zones found in the database.")
        return

    # Combine daily and hourly zones
    zones_data = daily_zones 

    # Create a list to store stocks that are closest to the price range
    closest_stocks = []

    for zone in zones_data:
        symbol = zone['symbol']
        price_range_high = zone['price_range_high']
        price_range_low = zone['price_range_low']

        # Get current price of the stock
        current_price = get_current_price(symbol)

        if current_price is None:
            print(f"Skipping {symbol} as current price couldn't be fetched.")
            continue

        # Calculate the difference between the current price and both price range boundaries
        diff_low = abs(current_price - price_range_low)
        diff_high = abs(current_price - price_range_high)

        # Determine the nearest price range (low or high)
        nearest_range = 'Low' if diff_low < diff_high else 'High'
        nearest_diff = diff_low if nearest_range == 'Low' else diff_high

        # Add stock to the closest_stocks list with the nearest price difference
        closest_stocks.append({
            'symbol': symbol,
            'current_price': current_price,
            'price_range_low': price_range_low,
            'price_range_high': price_range_high,
            'nearest_range': nearest_range,
            'nearest_diff': nearest_diff,
            'start_date': zone['start_date'],
            'end_date': zone['end_date'],
            'timeframe': zone['timeframe']
        })

    # Sort the stocks by the nearest price difference (smallest difference first)
    closest_stocks.sort(key=lambda x: x['nearest_diff'])
    
    # Insert the results into the StockDZSZ.db database
    if closest_stocks:
        insert_results_into_db(closest_stocks, '../StockDZSZNDX.db')
    else:
        print("No stocks found with valid price data.")

# Main entry point
if __name__ == "__main__":
    check_stocks_in_range()

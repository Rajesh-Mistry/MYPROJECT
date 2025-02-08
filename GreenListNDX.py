import sqlite3
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta

# Function to fetch active demand/supply zones from the SQLite database
def fetch_active_zones(db_file, timeframe):
    connection = sqlite3.connect(db_file)
    
    today = datetime.today()
    
    if timeframe == '1d':
        date_limit = today - timedelta(days=15)
    elif timeframe == '2h':
        date_limit = today - timedelta(days=10)
    elif timeframe == '1h':
        date_limit = today - timedelta(days=5)
    else:
        raise ValueError("Invalid timeframe. Use '1d', '2h', or '1h'.")
    
    date_limit_str = date_limit.strftime('%Y-%m-%d')
    df = pd.DataFrame()
    
    if db_file == '../StockDZSZ.db':
        query = f"""
        SELECT symbol, zone_type, price_range_high, price_range_low, zone_status, start_date, tested_date
        FROM demand_supply_zones
        WHERE zone_status = 'Tested' AND start_date >= '{date_limit_str}';
        """
        # Use pandas to load the SQL query results into a DataFrame
        df = pd.read_sql(query, connection)
    else:
        query = f"""
        SELECT symbol, zone_type, price_range_high, price_range_low, zone_status, start_date, tested_date
        FROM demand_supply_zones
        WHERE zone_status = 'Tested' AND start_date >= '{date_limit_str}';
        """
        query1 = f"""
        SELECT symbol, zone_type, price_range_high, price_range_low, zone_status, start_date, tested_date
        FROM demand_supply_zones_1h
        WHERE zone_status = 'Tested' AND start_date >= '{date_limit_str}';
        """
        # Use pandas to load the SQL query results into DataFrames
        df = pd.read_sql(query, connection)
        df1 = pd.read_sql(query1, connection)
        
        # Concatenate the two DataFrames
        df = pd.concat([df, df1], ignore_index=True)
    
    # Close the connection
    # connection.close()
    connection.close()
    return df

# Function to fetch the current price of a stock using yfinance
def get_current_price(stock_name):
    try:
        stock = yf.Ticker(stock_name)
        current_price = stock.history(period="1d")['Close'].iloc[-1]
        return current_price
    except Exception as e:
        print(f"Error fetching price for {stock_name}: {e}")
        return None

# Function to create the GreenRedList table in the database
def create_green_red_list_table(db_file):
    if db_file == "../StockDZSZNDX.db":
        connection = sqlite3.connect(db_file)
        cursor = connection.cursor()
        cursor.execute('DROP TABLE IF EXISTS GreenRedList')
        # Create the GreenRedList table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS GreenRedList (
                Sr INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                start_date TEXT,
                end_date TEXT,
                price_range_high REAL,
                price_range_low REAL,
                tested_date TEXT,
                timeframe TEXT,
                List TEXT
            );
        ''')
        connection.commit()
        connection.close()

# Function to insert data into the GreenRedList table
def insert_into_green_red_list(db_file, data):
    if db_file == "../StockDZSZNDX.db":
        connection = sqlite3.connect(db_file)
        cursor = connection.cursor()
        
        cursor.execute('''
            INSERT INTO GreenRedList (symbol, start_date, end_date, price_range_high, price_range_low, tested_date, timeframe, List)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (data['symbol'], data['start_date'], data['end_date'], data['price_range_high'], data['price_range_low'], data['tested_date'], data['timeframe'], data['List']))
        
        connection.commit()
        connection.close()

# Function to calculate the body and wick of the last candle
def calculate_wick_and_body(open_price, close_price, high_price, low_price):
    # Calculate the body (difference between open and close)
    body = abs(open_price - close_price)
    
    # Calculate the wick (difference between high and low)
    wick_top = high_price - max(open_price, close_price)  # Top wick
    wick_bottom = min(open_price, close_price) - low_price  # Bottom wick
    wick = wick_top + wick_bottom  # Total wick (top + bottom)
    
    return body, wick
def is_valid_candle(open_price, close_price, high_price, low_price):
    body, wick = calculate_wick_and_body(open_price, close_price, high_price, low_price)
    
    # If wick is greater than 10% of body, return False (skip zone)
    if body > 0 and wick > 0.1 * body:
        return False
    return True
# Function to check if the price is within the zone's range and meets the tested date condition
def check_price_in_zone(db_file):
    # Determine the appropriate timeframe based on the database file
    if db_file == "../StockDZSZNDX.db":
        timeframe = "1d"
    elif db_file == "../StockTest.db":
        timeframe = "2h"  # default for "demand_supply_zones"
        # Also, we'll check for "demand_supply_zones_1hr" separately with 1h timeframe
        check_price_in_zone_with_timeframe(db_file, timeframe, "demand_supply_zones")
        timeframe = "1h"  # switch to 1h for "demand_supply_zones_1hr"
    
    check_price_in_zone_with_timeframe(db_file, timeframe, "demand_supply_zones")

# Helper function to handle checking price in the zone based on the specific timeframe
def check_price_in_zone_with_timeframe(db_file, timeframe, table_name):
    active_zones = fetch_active_zones(db_file, timeframe)
    
    current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    print(f"Checking zones at {current_datetime} with {timeframe} timeframe for table {table_name}")

    for index, row in active_zones.iterrows():
        stock_name = row['symbol']
        current_price = get_current_price(stock_name)
        start_date = row['start_date']
        tested_date = row['tested_date']
        
        # Check if the 'tested_date' format is valid (handle errors gracefully)
        try:
            if len(tested_date) == 10:  # Date format without time, e.g., '2025-01-24'
                tested_date = tested_date + " 00:00:00"  # Add default time if no time is present
            elif len(tested_date) == 16:  # Date format with minutes but no seconds (e.g., '2025-01-16 09:15')
                tested_date = tested_date + ":00"  # Add ':00' for seconds
            datetime_object = datetime.strptime(tested_date, '%Y-%m-%d %H:%M:%S')
            days_since_tested = (datetime.now() - datetime_object).days
        except ValueError as e:
            print(f"Error parsing tested date {tested_date}: {e}")
            days_since_tested = None

        if current_price is not None and days_since_tested is not None and days_since_tested < 3:
            zone_list = None
            # Determine if it's a Demand or Supply Zone
            if row['zone_type'] == 'Demand Zone':
                if row['price_range_low'] <= current_price:
                    zone_list = "Green List"  # Price is within the Demand Zone
                    print(f"Stock {stock_name} ({start_date}) is within the active Demand Zone: "
                          f"Price: {current_price} is above or equal to zone low {row['price_range_low']} "
                          f"and tested on {tested_date}")
                else:
                    zone_list = "Red List"  # Price has not crossed the zone low
                    print(f"Stock {stock_name} ({start_date}) price hasn't crossed the zone low for Demand Zone.")
            
            elif row['zone_type'] == 'Supply Zone':
                if row['price_range_high'] >= current_price:
                    zone_list = "Red List"  # Price is within the Supply Zone
                    print(f"Stock {stock_name} ({start_date}) is within the active Supply Zone: "
                          f"Price: {current_price} is below or equal to zone high {row['price_range_high']} "
                          f"and tested on {tested_date}")
                else:
                    zone_list = "Green List"  # Price has not crossed the zone high
                    print(f"Stock {stock_name} ({start_date}) price hasn't crossed the zone high for Supply Zone.")
            
            # Prepare the data to be inserted into the GreenRedList table
            if zone_list:
                data = {
                    'symbol': stock_name,
                    'start_date': start_date,
                    'end_date': tested_date,
                    'price_range_high': row['price_range_high'],
                    'price_range_low': row['price_range_low'],
                    'tested_date': tested_date,
                    'timeframe': timeframe,
                    'List': zone_list
                }
                insert_into_green_red_list("../StockDZSZNDX.db", data)

# Example usage
db_file1 = '../StockDZSZNDX.db'  # Path to your SQLite database file
# db_file2 = '../StockTest.db'  # Path to your other SQLite database file

# Create the GreenRedList table if not exists
create_green_red_list_table(db_file1)

# Check prices in zone for both databases
check_price_in_zone(db_file1)
# check_price_in_zone(db_file2)

import sqlite3
import pandas as pd
import yfinance as yf

# Function to fetch active demand/supply zones from the SQLite database
def fetch_active_zones(db_file):
    # Establish a connection to the SQLite database
    connection = sqlite3.connect(db_file)
    if  db_file == '../StockDZSZ.db':
    # Define the query to fetch active zones (stock names, price ranges, etc.)
        query = """
        SELECT symbol, zone_type, price_range_high, price_range_low, zone_status
        FROM demand_supply_zones
        WHERE zone_status = 'Active';
        """
        # Use pandas to load the SQL query results into a DataFrame
        df = pd.read_sql(query, connection)
    else:
        query = """
        SELECT symbol, zone_type, price_range_high, price_range_low, zone_status
        FROM demand_supply_zones
        WHERE zone_status = 'Active';
        """
        query1 = """
        SELECT symbol, zone_type, price_range_high, price_range_low, zone_status
        FROM demand_supply_zones_1hr
        WHERE zone_status = 'Active';
        """
        # Use pandas to load the SQL query results into a DataFrame
        df = pd.read_sql(query, connection)
        # Use pandas to load the SQL query results into a DataFrame
        df = pd.read_sql(query1, connection)
        

    
    # Close the connection
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

# Function to check if the price is within the zone's range
def check_price_in_zone(db_file):
    # Fetch active zones data from the SQLite database
    active_zones = fetch_active_zones(db_file)
    
    # Loop through each row in the DataFrame
    for index, row in active_zones.iterrows():
        stock_name = row['symbol']
        current_price = get_current_price(stock_name)
        
        if current_price is not None:
            # Check if the current price is within the zone's price range
            if row['price_range_low'] <= current_price <= row['price_range_high']:
                print(f"Stock {stock_name} is within the active zone: {row['zone_type']} between {row['price_range_low']} and {row['price_range_high']}")
        else:
            print(f"Skipping {stock_name} due to error in fetching price.")

# Example usage
db_file = ['../StockDZSZ.db', '../StockTest.db']  # Path to your SQLite database file
for i in db_file:
    check_price_in_zone(i)

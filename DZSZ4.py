import sqlite3
import yfinance as yf
import pandas as pd
import pytz
from fyers_apiv3 import fyersModel
import configparser
import datetime
from dateutil.relativedelta import relativedelta
import time
import os
# List of Nifty 50 stock symbols
nifty_200_symbols = [
    "ABB", "ABFRL", "ABCAPITAL", "ADANIENT", "ADANIGREEN", "ADANIPOWER", "ADANIPORTS", "ALKE", "APOLLOHOSP",
    "APOLLOTYRE", "APLAPOLLO", "ASTRAL", "ATGL", "AUBANK", "AUROPHARMA", "AXISBANK", "Bajaj-AUTO", "BAJAJFINSV",
    "BAJAJHLDNG", "BANDHANBNK", "BANKBARODA", "BANKINDIA", "BDL", "BEL", "BHARATFORG", "BHARTIARTL", "BHARTIHEXA",
    "BIOCON", "BHEL", "BSE", "BRITANNIA", "CANBK", "CGPOWER", "CIPLA", "COALINDIA", "COCHINSHIP", "COFORGE",
    "CONCOR", "CUMMINSIND", "DABUR", "DELHIVERY", "DIVISLAB", "DLF", "DIXON", "DRREDDY", "DMART", "EICHERMOT",
    "ESCORTS", "EXIDEIND", "FACT", "FEDERALBNK", "GAIL", "GMRINFRA", "GODREJCP", "GODREJPROP", "GRASIM", 
    "HAL", "HAVELLS", "HDFCAMC", "HDFCBANK", "HDFCLIFE", "HINDALCO", "HINDPETRO", "HINDUNILVR", "HINDZINC", 
    "HUDCO", "ICICIBANK", "ICICIGI", "ICICIPRULI", "IDBI", "IDFCFIRSTB", "IGL", "IRB", "IRFC", "IRCTC",
    "JSWENERGY", "JSWSTEEL", "JSWINFRA", "JUBLFOOD", "JIOFIN", "KALYANKJIL", "KPITTECH", "KOTAKBANK", 
    "LTF", "LICI", "LICHSGFIN", "LT", "LTIM", "MAHABANK", "MAZDOCK", "MARICO", "MARUTI", "MANKIND", 
    "MOTHERSON", "MUTHOOTFIN", "NLCINDIA", "NESTLEIND", "NTPC", "NYKAA", "OIL", "ONGC", "OFSS", "PAGEIND", 
    "PATANJALI", "PFC", "PHOENIXLTD", "PIDILITIND", "POLICYBZR", "POLYCAB", "POONAWALLA", "POWERGRID", 
    "PRESTIGE", "RECLTD", "RELIANCE", "RVNL", "SBICARD", "SBILIFE", "SAIL", "SHREECEM", "SHRIRAMFIN", 
    "SIEMENS", "SJVN", "SONACOMS", "SUZLON", "SUNPHARMA", "SUNDARMFIN", "TATACOMM", "TATACONSUM", 
    "TATAELXSI", "TATAMOTORS", "TATAPOWER", "TATASTEEL", "TATATECH", "TITAN", "TORNTPHARM", "TORNTPOWER", 
    "TRENT", "UPL", "UNITDSPR", "ULTRACEMCO", "VEDL", "VOLATAS", "WIPRO", "YESBANK", "ZOMATO", "ZYDUSLIFE"
]

nifty_200_symbols = [i + ".NS" for i in nifty_200_symbols]

# Connect to SQLite database
conn = sqlite3.connect('../StockTest.db')
cursor = conn.cursor()

# Drop existing table and create a new one
cursor.execute("DROP TABLE IF EXISTS demand_supply_zones;")
cursor.execute("""
CREATE TABLE IF NOT EXISTS demand_supply_zones (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT,
    start_date TEXT,
    end_date TEXT,
    base_candles_count INTEGER,
    zone_type TEXT,
    zone_classification TEXT,
    price_range_high REAL,
    price_range_low REAL,
    zone_status TEXT,
    tested_date TEXT
)
""")
conn.commit()

# Clean the table before inserting new data
cursor.execute("DELETE FROM demand_supply_zones;")

def update_zone_status(stock_data, price_range_high, price_range_low, zone_type, start_date, end_date):
    """
    Determines the status of the zone (Active, Tested, or Violated) based on stock data.
    Args:
        stock_data: DataFrame containing historical stock data.
        price_range_high: High value of the zone (supply zone or demand zone).
        price_range_low: Low value of the zone (supply zone or demand zone).
        zone_type: The type of zone ('Supply Zone' or 'Demand Zone').
        start_date: The start date of the zone.
        end_date: The end date of the zone.
    Returns:
        zone_status: One of "Active", "Tested", or "Violated".
    """
    tested_date = None
    zone_status = "Active"  # Default to Active
    

    # Find the indices for the start_date and end_date
    start_index = stock_data.index.get_loc(start_date)
    end_index = stock_data.index.get_loc(end_date)
    
    # Get the candles immediately after the zone (next candles)
    next_candles = stock_data.iloc[end_index + 1:]  # Next 2 or 3 candles after the zone

    # Print the zone boundaries for debugging
    print(f"Checking zone status from {start_date} to {end_date}. Zone Price range: High = {price_range_high}, Low = {price_range_low}")

    for i in range(len(next_candles)):
        current_high = next_candles['High'].iloc[i]
        current_low = next_candles['Low'].iloc[i]
        current_close = next_candles['Close'].iloc[i]

        # Print the current price levels for debugging
        print(f"Checking next candle at index {i}. Current High: {current_high}, Current Low: {current_low}, Current Close: {current_close}")

        # Check if the price violates the zone
        if zone_type == "Supply Zone":
            if current_close > price_range_high:
                zone_status = "Violated"
                print(f"Supply Zone violated: Close {current_close} > High {price_range_high}")
                break
            elif price_range_low <= current_high <= price_range_high or price_range_low <= current_low <= price_range_high:
                zone_status = "Tested"
                print(f"Supply Zone tested: Price in range [{price_range_low}, {price_range_high}]")
                tested_date = next_candles.index[i]  # Set the tested date when the zone is tested
                break
        elif zone_type == "Demand Zone":
            if current_close < price_range_low:
                zone_status = "Violated"
                print(f"Demand Zone violated: Close {current_close} < Low {price_range_low}")
                break
            elif price_range_low <= current_high <= price_range_high or price_range_low <= current_low <= price_range_high:
                zone_status = "Tested"
                print(f"Demand Zone tested: Price in range [{price_range_low}, {price_range_high}]")
                tested_date = next_candles.index[i]  # Set the tested date when the zone is tested
                break
    return zone_status, tested_date

def calculate_zone_price_range(classified_candles, stock_data, zone_type, start_date, end_date):
    start_candle = classified_candles[start_date]
    start_index = stock_data.index.get_loc(start_date)
    end_index = stock_data.index.get_loc(end_date)

    base_candles = [
        classified_candles[stock_data.index[i]] 
        for i in range(start_index + 1, end_index) 
        if classified_candles[stock_data.index[i]]["candle_type"] == "Base"
    ]

    combined_base_candle = combine_multiple_base_candles(base_candles)
    
    if combined_base_candle and combined_base_candle['open'] is None:
        print(f"Warning: Invalid combined base candle for {start_date} to {end_date}. Returning None.")
        return None  # Or return default value
    
    if zone_type == 'DBD':  # Double Bottom
        highest_wick = round(combined_base_candle['high'], 2)
        lowest_body = round(combined_base_candle["lowest_body"], 2)
        return (start_date, end_date, highest_wick, lowest_body)
    
    elif zone_type == 'RBR':  # Reversal Bottom
        lowest_wick = round(combined_base_candle['low'], 2)
        highest_body = round(combined_base_candle["highest_body"], 2)
        return (start_date, end_date, lowest_wick, highest_body)
    
    elif zone_type == 'RBD':  # Reversal Double
        highest_wick = round(max(combined_base_candle['high'], start_candle["high"]), 2)
        lowest_body = round(combined_base_candle["lowest_body"], 2)
        return (start_date, end_date, highest_wick, lowest_body)
    
    elif zone_type == 'DBR':  # Double Bottom Reversal
        lowest_wick = round(min(combined_base_candle['low'], start_candle["low"]), 2)
        highest_body = round(combined_base_candle["highest_body"], 2)
        return (start_date, end_date, lowest_wick, highest_body)
    
    else:
        return None



def combine_multiple_base_candles(candles):
    if not candles:
        return {
            'open': None,
            'close': None,
            'high': None,
            'low': None,
            'color': None,
            'lowest_body': None,
            'highest_body': None,
            'combined': False
        }

    new_high = candles[0]['high']
    new_low = candles[0]['low']
    new_open = candles[0]['open']
    new_close = candles[0]['close']
    new_color = candles[0]['color']
    
    lowest_body = min(new_open, new_close)
    highest_body = max(new_open, new_close)

    for candle in candles[1:]:
        new_high = max(new_high, candle['high'])
        new_low = min(new_low, candle['low'])

        if candle['color'] == 'g':
            candle_low_body = candle['open']
            candle_high_body = candle['close']
        elif candle['color'] == 'r':
            candle_low_body = candle['close']
            candle_high_body = candle['open']
        else:
            candle_low_body = min(candle['open'], candle['close'])
            candle_high_body = max(candle['open'], candle['close'])

        lowest_body = min(lowest_body, candle_low_body)
        highest_body = max(highest_body, candle_high_body)

        if new_close > new_open:
            new_color = 'g'
        elif new_close < new_open:
            new_color = 'r'
        else:
            new_color = None

    return {
        'open': new_open,
        'close': new_close,
        'high': new_high,
        'low': new_low,
        'color': new_color,
        'lowest_body': lowest_body,
        'highest_body': highest_body,
        'combined': True
    }

def is_last_long_candle_valid(zone_start_index, zone_end_index, stock_data, long_candle_threshold):
    # Initialize variable to store the last long candle
    last_long_candle = None
    
    # Iterate backwards from the zone end to find the last long candle
    for i in range(zone_end_index, zone_start_index, -1):
        candle = stock_data.iloc[i]
        candle_size = abs(candle['Close'] - candle['Open'])
        
        if candle_size >= long_candle_threshold:  # It's a long candle
            last_long_candle = candle
            break  # We found the last long candle, exit the loop
    
    if last_long_candle is not None:
        # Calculate the body and wick of the last long candle
        body_size = abs(last_long_candle['Close'] - last_long_candle['Open'])
        wick_size = max(last_long_candle['High'], last_long_candle['Low']) - max(last_long_candle['Open'], last_long_candle['Close'])

        # Check if the wick size is no more than 10% of the body size
        wick_percentage = wick_size / body_size if body_size != 0 else 0
        if wick_percentage <= 0.10:
            print(f"The last long candle is valid: Wick size is {wick_size} and Body size is {body_size}. Wick is within 10% of the body.")
            return True
        else:
            print(f"The last long candle is invalid: Wick size is {wick_size} and Body size is {body_size}. Wick exceeds 10% of the body.")
            return False
    else:
        print("No long candle found in the zone.")
        return False

def candles_2hr(confile, symbol):
    config = configparser.ConfigParser()

    # Verify the config file exists
    if not os.path.exists(confile):
        print(f"Config file not found: {confile}")
        return None
    
    config.read(confile)  # Read the configuration file

    # Check if 'FyersAPI' section exists
    if 'FyersAPI' not in config:
        print("FyersAPI section not found in config file.")
        return None

    # Fetch client_id and access_token from config
    client_id = config['FyersAPI']['client_id']
    access_token = config['FyersAPI']['access_token']

    # Initialize the FyersModel instance with your client_id, access_token, and enable async mode
    fyers = fyersModel.FyersModel(client_id=client_id, is_async=False, token=access_token, log_path="")

    # Get today's date and the date 1 month before today
    today = datetime.datetime.now()
    month1bef = today - relativedelta(months=1)

    # Convert the dates to epoch timestamps
    range_from_epoch = int(time.mktime(month1bef.timetuple()))  # 1 month ago
    range_to_epoch = int(time.mktime(today.timetuple()))  # Today

    # Prepare the data dictionary
    data = {
        "symbol": symbol,
        "resolution": "120",
        "date_format": "0",  # 0 means using epoch timestamps
        "range_from": range_from_epoch,
        "range_to": range_to_epoch,
        "cont_flag": "1"
    }

    # Call the API
    response = fyers.history(data=data)
    # print(response)

    # Sample data
    candles_data = response.get('candles', [])

    if not candles_data:
        print(f"No candles data found for symbol: {symbol}")
        return None

    # Convert the candles data
    converted_data = []
    for candle in candles_data:
        date = datetime.datetime.fromtimestamp(candle[0]).strftime('%Y-%m-%d %H:%M:%S')
        open_price = candle[1]
        high_price = candle[2]
        low_price = candle[3]
        close_price = candle[4]
        volume = candle[5]
        
        # Append the converted values to the list
        converted_data.append([date, open_price, high_price, low_price, close_price, volume])

    # Create the DataFrame
    two_hour_df = pd.DataFrame(converted_data, columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])

    # Set the 'Date' column as the index
    two_hour_df.set_index('Date', inplace=True)
    return two_hour_df

def convert_to_nse_symbol(symbol):
    # Remove any suffix like '.NS' and prepend 'NSE:'
    if symbol.endswith('.NS'):
        return 'NSE:' + symbol.split('.')[0] + '-EQ'
    else:
        return 'NSE:' + symbol + '-EQ'
# The analyze_zones function needs slight modification to accommodate the 1-hour data
def analyze_zones(stock_symbol):
    try:
        stock_symbol2 = convert_to_nse_symbol(stock_symbol)
        stock_data = candles_2hr('config.ini', stock_symbol2)

        # Check if stock data is None or empty
        if stock_data is None or stock_data.empty:
            print(f"Warning: No data found for {stock_symbol}. Skipping.")
            return []

        # Calculate candle sizes and round them to 1 decimal
        candle_sizes = [abs(stock_data['Close'][i] - stock_data['Close'][i-1]) for i in range(1, len(stock_data))]
        rounded_candle_sizes = [round(size, 1) for size in candle_sizes]
        
        # Calculate average candle size
        avg_candle_size = round(sum(rounded_candle_sizes) / len(rounded_candle_sizes), 1)
        long_candle_threshold = avg_candle_size * 1.5

        # Track long candle dates
        long_candle_dates = [
            stock_data.index[i] 
            for i in range(1, len(stock_data))  
            if rounded_candle_sizes[i-1] >= long_candle_threshold
        ]

        # Function to classify candles as long or base
        def classify_candle_color(i):
            if stock_data['Close'][i] > stock_data['Open'][i]:
                return 'g'  # Green
            elif stock_data['Close'][i] < stock_data['Open'][i]:
                return 'r'  # Red
            else:
                return None  # Neutral

        # Create a dictionary to classify candles
        classified_candles = {}
        for i in range(1, len(stock_data)):  # Start from 1
            candle_color = classify_candle_color(i)
            candle_type = "Long" if rounded_candle_sizes[i-1] >= long_candle_threshold else "Base"
            classified_candles[stock_data.index[i]] = {
            "close": round(stock_data['Close'][i], 1),
            "candle_size": rounded_candle_sizes[i-1],
            "candle_type": candle_type,
            "color": candle_color,
            "open": stock_data['Open'][i],
            "high": stock_data['High'][i],
            "low": stock_data['Low'][i],
            "date": stock_data.index[i]  # Explicitly add the date
        }



        # Loop through long candle dates and analyze zones
        for i in range(1, len(long_candle_dates)):
            start_date = long_candle_dates[i - 1]
            end_date = long_candle_dates[i]
            
            # Determine zone type and classification
            first_candle_color = classified_candles[start_date]["color"]
            second_candle_color = classified_candles[end_date]["color"]
            # del_HL = abs(classified_candles[end_date]["high"] - classified_candles[end_date]["low"])
            del_OC = abs(classified_candles[end_date]["open"] - classified_candles[end_date]["close"])
            # wick = abs(del_HL-del_OC)
            if first_candle_color == 'r' and second_candle_color == 'r':
                zone_type = 'Supply Zone'  # Reversal Bottom
                xa = min(classified_candles[end_date]["open"], classified_candles[end_date]["close"])
                wick = classified_candles[end_date]["low"] - xa
                zone_classification = 'DBD'
            elif first_candle_color == 'g' and second_candle_color == 'g':
                zone_type = 'Demand Zone'  # Reversal Double
                zone_classification = 'RBR'
                xa = max(classified_candles[end_date]["open"], classified_candles[end_date]["close"])
                wick = classified_candles[end_date]["high"] - xa
            if first_candle_color == 'r' and second_candle_color == 'g':
                zone_type = 'Demand Zone'  # Reversal Bottom
                zone_classification = 'DBR'
                xa = max(classified_candles[end_date]["open"], classified_candles[end_date]["close"])
                wick = classified_candles[end_date]["high"] - xa
            elif first_candle_color == 'g' and second_candle_color == 'r':
                zone_type = 'Supply Zone'  # Reversal Double
                zone_classification = 'RBD'
                xa = min(classified_candles[end_date]["open"], classified_candles[end_date]["close"])
                wick = classified_candles[end_date]["low"] - xa
            else:
                zone_type = 'Neutral'
                zone_classification = 'None'
                
            start_index = stock_data.index.get_loc(start_date)
            end_index = stock_data.index.get_loc(end_date)

            base_candles = [
                classified_candles[stock_data.index[i]] 
                for i in range(start_index + 1, end_index)  # Exclude long candles themselves
                if classified_candles[stock_data.index[i]]["candle_type"] == "Base"
            ]
            
            # Count the base candles correctly
            base_candle_count = len(base_candles)
            

            result = calculate_zone_price_range(
                classified_candles, stock_data, zone_classification, start_date, end_date
            )
            if result is not None and len(base_candles) < 7 and len(base_candles) != 0:
                # Calculate zone price range
                start_date, end_date, price_range_high, price_range_low = result
                # Determine zone status
                zone_status, tested_date = update_zone_status(stock_data, price_range_high, price_range_low, zone_type, start_date, end_date)
                if wick > (0.1*del_OC):
                    zone_status = "Bad"
                print(f"Zone found for {stock_symbol}: {zone_type} from {start_date} to {end_date} "
                    f"Price Range: {price_range_high} - {price_range_low} Status: {zone_status}")
                
                cursor.execute("""
                    INSERT INTO demand_supply_zones (symbol, start_date, end_date, base_candles_count, zone_type, 
                    zone_classification, price_range_high, price_range_low, zone_status,tested_date)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                            (stock_symbol, start_date, end_date, base_candle_count, zone_type,
                                zone_classification, price_range_high, price_range_low, zone_status,tested_date))
                conn.commit()

    except Exception as e:
        print(f"Error processing {stock_symbol}: {str(e)}")

# Analyze zones for all Nifty 50 symbols
for symbol in nifty_200_symbols:
    analyze_zones(symbol)

# Close the database connection
conn.close()

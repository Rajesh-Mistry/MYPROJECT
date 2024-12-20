import sqlite3
import yfinance as yf
import pandas as pd

# List of Nifty 200 stock symbols
nifty_200_symbols = [
    "COFORGE", "PERSISTENT", "MFSL", "PAYTM", "MRPL", "VBL", "IOB", "BANDHANBNK", "BAJFINANCE", "MPHASIS",
    "BSE", "DMART", "MAHABANK", "ZOMATO", "OFSS", "JSWENERGY", "SRF", "NAUKRI", "DIXON", "KALYANKJIL",
    "LTF", "AUBANK", "ICICIPRULI", "TECHM", "SUPREMEIND", "HINDZINC", "GMRINFRA", "LICHSGFIN", "BHARTIHEXA",
    "MOTHERSON", "GODREJPROP", "TATACONSUM", "BANKBARODA", "ABCAPITAL", "EXIDEIND", "ATGL", "BAJAJ-AUTO", 
    "TATATECH", "PNB", "IDBI", "NLCINDIA", "POLICYBZR", "ABFRL", "IRFC", "HDFCBANK", "OBEROIRLTY", "TCS", 
    "HCLTECH", "NMDC", "ZYDUSLIFE", "BAJAJFINSV", "COALINDIA", "SHREECEM", "TATAELXSI", "LTIM", "CANBK", 
    "MAZDOCK", "TIINDIA", "ADANIGREEN", "INFY", "MUTHOOTFIN", "HUDCO", "SBILIFE", "JINDALSTEL", "ESCORTS", 
    "ASHOKLEY", "IRB", "BOSCHLTD", "ACC", "VEDL", "PATANJALI", "LODHA", "SJVN", "BRITANNIA", "SBICARD", 
    "INDHOTEL", "FACT", "PIIND", "SUZLON", "BDL", "TATAPOWER", "UPL", "TORNTPOWER", "WIPRO", "IREDA", 
    "ADANIENT", "TRENT", "SOLARINDS", "KOTAKBANK", "MARICO", "ASTRAL", "APOLLOHOSP", "JIOFIN", "NYKAA", 
    "INDIGO", "BPCL", "YESBANK", "MARUTI", "BALKRISIND", "COLPAL", "UNIONBANK", "BHARATFORG", "SONACOMS", 
    "DABUR", "RVNL", "ONGC", "BIOCON", "RELIANCE", "POLYCAB", "BAJAJHLDNG", "VOLTAS", "TORNTPHARM", 
    "ITC", "PETRONET", "RECLTD", "IGL", "HINDALCO", "GAIL", "PRESTIGE", "UNITDSPR", "INDUSINDBK", 
    "HDFCAMC", "GODREJCP", "ASIANPAINT", "TATAMOTORS", "BHARTIARTL", "HDFCLIFE", "MAXHEALTH", "IRCTC", 
    "FEDERALBNK", "POONAWALLA", "SBIN", "AMBUJACEM", "DRREDDY", "INDIANB", "ULTRACEMCO", "CHOLAFIN", 
    "MRF", "NESTLEIND", "LICI", "GRASIM", "HEROMOTOCO", "NHPC", "HINDUNILVR", "ADANIPOWER", "HAVELLS", 
    "JSWSTEEL", "BANKINDIA", "PIDILITIND", "TATACOMM", "BEL", "HINDPETRO", "ICICIBANK", "PHOENIXLTD", 
    "TATASTEEL", "DLF", "PFC", "TITAN", "APOLLOTYRE", "AXISBANK", "IOC", "AUROPHARMA", "MANKIND", 
    "JUBLFOOD", "TATACHEM", "CIPLA", "NTPC", "ICICIGI", "DIVISLAB", "SUNDARMFIN", "HAL", "ADANIPORTS", 
    "LT", "POWERGRID", "SHRIRAMFIN", "IDEA", "CGPOWER", "OIL", "EICHERMOT", "DELHIVERY", "JSWINFRA", 
    "INDUSTOWER", "PAGEIND", "IDFCFIRSTB", "SUNPHARMA", "CONCOR", "ADANIENSOL", "LUPIN", "SAIL", 
    "KPITTECH", "TVSMOTOR", "CUMMINSIND", "M&M", "COCHINSHIP", "BHEL", "ALKEM", "APLAPOLLO", "SIEMENS", 
    "M&MFIN", "ABB"
]

nifty_200_symbols = [i + ".NS" for i in nifty_200_symbols]

# Connect to SQLite database
conn = sqlite3.connect('StockTest.db')
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
    zone_status TEXT
)
""")
conn.commit()

# Clean the table before inserting new data
cursor.execute("DELETE FROM demand_supply_zones;")

# Function to combine base candles
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

# Function to calculate price range for different zones
def calculate_zone_price_range(classified_candles, stock_data, zone_type, start_date, end_date):
    start_candle = classified_candles[start_date]
    start_index = stock_data.index.get_loc(start_date)
    end_index = stock_data.index.get_loc(end_date)

    base_candles = [
        classified_candles[stock_data.index[i].strftime('%Y-%m-%d %H:%M')] 
        for i in range(start_index + 1, end_index) 
        if classified_candles[stock_data.index[i].strftime('%Y-%m-%d %H:%M')]["candle_type"] == "Base"
    ]
    
    combined_base_candle = combine_multiple_base_candles(base_candles)
    
    if combined_base_candle and combined_base_candle['open'] is None:
        print(f"Warning: Invalid combined base candle for {start_date} to {end_date}. Returning None.")
        return None  
    
    if zone_type == 'DBD':  # Double Bottom
        highest_wick = round(combined_base_candle['high'], 2)
        lowest_body = round(combined_base_candle["lowest_body"], 2)
        return (start_date, end_date, highest_wick, lowest_body)
    
    elif zone_type == 'RBR':  # Reversal Bottom
        lowest_wick = round(combined_base_candle['low'], 2)
        highest_body = round(combined_base_candle["highest_body"], 2)
        return (start_date, end_date, highest_body, lowest_wick)
    
    return None

# Function to fetch stock data
def fetch_stock_data(symbol):
    try:
        stock_data = yf.download(symbol, interval='60m', period='1d')  # Get 1-hour interval data
        return stock_data
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return None

# Function to classify candles and calculate demand/supply zones
def classify_candles_and_calculate_zones(stock_data, symbol):
    classified_candles = {}
    
    for index, row in stock_data.iterrows():
        candle_open = row['Open']
        candle_close = row['Close']
        candle_high = row['High']
        candle_low = row['Low']

        # Determine candle color
        if candle_close > candle_open:
            color = 'g'
        elif candle_close < candle_open:
            color = 'r'
        else:
            color = 'd'  # Doji
        
        classified_candles[index.strftime('%Y-%m-%d %H:%M')] = {
            'open': candle_open,
            'close': candle_close,
            'high': candle_high,
            'low': candle_low,
            'color': color,
            'candle_type': 'Base' if abs(candle_close - candle_open) < (candle_high - candle_low) * 0.3 else 'Normal'
        }
    
    # Debugging output to check classified candles
    print(f"Candle Data for {symbol}: {classified_candles}")

    # Example of calculating zones
    for i in range(len(stock_data) - 1):
        current_time = stock_data.index[i]
        next_time = stock_data.index[i + 1]
        classified_zone = 'DBD' if classified_candles[current_time.strftime('%Y-%m-%d %H:%M')]['color'] == 'g' else 'RBR'
        
        price_range = calculate_zone_price_range(classified_candles, stock_data, classified_zone, current_time.strftime('%Y-%m-%d %H:%M'), next_time.strftime('%Y-%m-%d %H:%M'))
        
        if price_range:
            cursor.execute("""
            INSERT INTO demand_supply_zones (symbol, start_date, end_date, base_candles_count, zone_type, zone_classification, price_range_high, price_range_low, zone_status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (symbol, price_range[0], price_range[1], 1, classified_zone, classified_zone, price_range[2], price_range[3], "Active"))
            conn.commit()
        else:
            print(f"No valid price range for {symbol} between {current_time} and {next_time}.")
    print(stock_data)

# Main loop to process each stock symbol
for symbol in nifty_200_symbols:
    print(f"Processing {symbol}...")

    stock_data = fetch_stock_data(symbol)
    if stock_data is not None:
        classify_candles_and_calculate_zones(stock_data, symbol)

# Close the database connection
conn.close()
print("Processing complete.")

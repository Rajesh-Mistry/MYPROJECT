import sqlite3
import yfinance as yf

# List of Nifty 50 stock symbols
stocks = [
    "AAPL", "NVDA", "MSFT", "AVGO", "META", "AMZN", "TSLA", "COST", "GOOGL", "GOOG",
    "NFLX", "TMUS", "AMD", "PEP", "LIN", "CSCO", "ADBE", "QCOM", "ISRG", "TXN",
    "AMGN", "INTU", "CMCSA", "AMAT", "BKNG", "HON", "VRTX", "MU", "PANW", "ADP",
    "ADI", "SBUX", "GILD", "REGN", "MELI", "INTC", "MDLZ", "LRCX", "KLAC", "CTAS",
    "CEG", "PDD", "PYPL", "SNPS", "MAR", "CRWD", "CDNS", "ORLY", "MRVL", "ASML",
    "CSX", "ADSK", "FTNT", "ABNB", "ROP", "NXPI", "DASH", "PCAR", "FANG", "AEP",
    "TTD", "MNST", "WDAY", "CPRT", "PAYX", "KDP", "ROST", "CHTR", "AZN", "FAST",
    "KHC", "ODFL", "GEHC", "MCHP", "EXC", "DDOG", "CTSH", "EA", "VRSK", "IDXX",
    "BKR", "CCEP", "XEL", "LULU", "CSGP", "TEAM", "ON", "CDW", "DXCM", "ZS",
    "ANSS", "BIIB", "SMCI", "TTWO", "ILMN", "GFS", "MRNA", "MDB", "WBD", "ARM",
    "DLTR"
]


# stocks = [i + ".NS" for i in nifty_50_symbol]

# Connect to SQLite database
conn = sqlite3.connect('StockDZSZNDX.db')
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
        classified_candles[stock_data.index[i].strftime('%Y-%m-%d')] 
        for i in range(start_index + 1, end_index) 
        if classified_candles[stock_data.index[i].strftime('%Y-%m-%d')]["candle_type"] == "Base"
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
def update_zone_status(stock_data, price_range_high, price_range_low, zone_type, start_date, end_date):
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
        elif zone_type == "Demand Zone":
            if current_close < price_range_low:
                zone_status = "Violated"
                print(f"Demand Zone violated: Close {current_close} < Low {price_range_low}")
                break
            elif price_range_low <= current_high <= price_range_high or price_range_low <= current_low <= price_range_high:
                zone_status = "Tested"
                print(f"Demand Zone tested: Price in range [{price_range_low}, {price_range_high}]")

    return zone_status


# Function to analyze Demand and Supply Zones for each stock symbol
def analyze_zones(stock_symbol):
    try:
        # Download the stock data
        stock_data = yf.download(stock_symbol, period="3mo", interval="1d")
        
        # If no data is returned, skip this symbol
        if stock_data.empty:
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
            stock_data.index[i].strftime('%Y-%m-%d') 
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
            classified_candles[stock_data.index[i].strftime('%Y-%m-%d')] = {
                "close": round(stock_data['Close'][i], 1),
                "candle_size": rounded_candle_sizes[i-1],
                "candle_type": candle_type,
                "color": candle_color,
                "open": stock_data['Open'][i],
                "high": stock_data['High'][i],
                "low": stock_data['Low'][i],
                "date": stock_data.index[i].strftime('%Y-%m-%d')  # Explicitly add the date
            }

        # Loop through long candle dates and analyze zones
        for i in range(1, len(long_candle_dates)):
            start_date = long_candle_dates[i - 1]
            end_date = long_candle_dates[i]
            
            # Determine zone type and classification
            first_candle_color = classified_candles[start_date]["color"]
            second_candle_color = classified_candles[end_date]["color"]
            
            if first_candle_color == 'r' and second_candle_color == 'r':
                zone_type = 'Supply Zone'  # Reversal Bottom
                zone_classification = 'DBD'
            elif first_candle_color == 'g' and second_candle_color == 'g':
                zone_type = 'Demand Zone'  # Reversal Double
                zone_classification = 'RBR'
            if first_candle_color == 'r' and second_candle_color == 'g':
                zone_type = 'Demand Zone'  # Reversal Bottom
                zone_classification = 'DBR'
            elif first_candle_color == 'g' and second_candle_color == 'r':
                zone_type = 'Supply Zone'  # Reversal Double
                zone_classification = 'RBD'
            else:
                zone_type = 'Neutral'
                zone_classification = 'None'
                
            start_index = stock_data.index.get_loc(start_date)
            end_index = stock_data.index.get_loc(end_date)

            base_candles = [
                classified_candles[stock_data.index[i].strftime('%Y-%m-%d')] 
                for i in range(start_index + 1, end_index)  # Exclude long candles themselves
                if classified_candles[stock_data.index[i].strftime('%Y-%m-%d')]["candle_type"] == "Base"
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
                zone_status = update_zone_status(stock_data, price_range_high, price_range_low, zone_type, start_date, end_date)
                
                print(f"Zone found for {stock_symbol}: {zone_type} from {start_date} to {end_date} "
                      f"Price Range: {price_range_high} - {price_range_low} Status: {zone_status}")
                
                cursor.execute("""
                    INSERT INTO demand_supply_zones (symbol, start_date, end_date, base_candles_count, zone_type, 
                    zone_classification, price_range_high, price_range_low, zone_status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                               (stock_symbol, start_date, end_date, base_candle_count, zone_type,
                                zone_classification, price_range_high, price_range_low, zone_status))
                conn.commit()

    except Exception as e:
        print(f"Error processing {stock_symbol}: {str(e)}")

# Analyze zones for all Nifty 50 symbols
for symbol in stocks:
    analyze_zones(symbol)

# Close the database connection
conn.close()

import yfinance as yf
import pandas as pd
from datetime import datetime

# Define the stock symbol
stock_symbol = 'INFY.NS'  # Example stock on NSE

# Download 3 months of historical data
stock_data = yf.download(stock_symbol, period="3mo", interval="1d")

# Calculate the candle sizes and round them to 1 decimal
candle_sizes = [abs(stock_data['Close'][i] - stock_data['Close'][i-1]) for i in range(1, len(stock_data))]
rounded_candle_sizes = [round(size, 1) for size in candle_sizes]

# Calculate the average candle size
avg_candle_size = round(sum(rounded_candle_sizes) / len(rounded_candle_sizes), 1)

# Define the threshold for long candles (e.g., 1.5 times the average candle size)
long_candle_threshold = avg_candle_size * 1.5

# Create a list to track long candle dates
long_candle_dates = [
    stock_data.index[i].strftime('%Y-%m-%d') 
    for i in range(1, len(stock_data))  # Start from 1 because we calculate candle size starting from index 1
    if rounded_candle_sizes[i-1] >= long_candle_threshold  # i-1 corresponds to the first price (Close)
]

# Function to classify candles based on color (Green or Red)
def classify_candle_color(i):
    # Determine if the candle is green (Close > Open) or red (Close < Open)
    if stock_data['Close'][i] > stock_data['Open'][i]:
        return 'g'  # Green
    elif stock_data['Close'][i] < stock_data['Open'][i]:
        return 'r'  # Red
    else:
        return None  # Neutral (Do not consider neutral candles)

# Create a dictionary to classify candles as long or base (normal) and track the zone types
classified_candles = {}
for i in range(1, len(stock_data)):  # Start from 1 to account for the first comparison
    candle_color = classify_candle_color(i)
    if rounded_candle_sizes[i-1] >= long_candle_threshold:
        candle_type = "Long"
    else:
        candle_type = "Base"

    # Track the candle type and color
    classified_candles[stock_data.index[i].strftime('%Y-%m-%d')] = {
        "close": round(stock_data['Close'][i], 1),
        "candle_size": rounded_candle_sizes[i-1],  # Candle size based on previous day close
        "candle_type": candle_type,
        "color": candle_color,
        "open": stock_data['Open'][i],
        "high": stock_data['High'][i],
        "low": stock_data['Low'][i]
    }

# Function to combine multiple base candles
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


# Debugging: print out the base candles for each date range
def calculate_zone_price_range(zone_type, start_date, end_date):
    start_candle = classified_candles[start_date]

    # Get the list of base candles between the long candles
    start_index = stock_data.index.get_loc(start_date)
    end_index = stock_data.index.get_loc(end_date)
    

    
    base_candles = [
        classified_candles[stock_data.index[i].strftime('%Y-%m-%d')] 
        for i in range(start_index + 1, end_index)  # Exclude long candles themselves
        if classified_candles[stock_data.index[i].strftime('%Y-%m-%d')]["candle_type"] == "Base"
    ]
    # Combine the base candles
    combined_base_candle = combine_multiple_base_candles(base_candles)

    if combined_base_candle and combined_base_candle['open'] is None:
        print(f"Warning: Invalid combined base candle for {start_date} to {end_date}. Returning None.")
        return None  # Or you could return a default value
    
    # Return the price ranges based on the zone type
    if zone_type == 'DBD':  # Double Bottom
        highest_wick = round(combined_base_candle['high'], 1)
        lowest_body = round(combined_base_candle["lowest_body"], 1)
        return (start_date, end_date, highest_wick, lowest_body)
    
    elif zone_type == 'RBR':  # Reversal Bottom
        lowest_wick = round(combined_base_candle['low'], 1)
        highest_body = round(combined_base_candle["highest_body"], 1)
        # print(start_date, base_candles)
        return (start_date, end_date, lowest_wick, highest_body)
    
    elif zone_type == 'RBD':  # Reversal Double

        highest_wick = round(max(combined_base_candle['high'], start_candle["high"]), 1)
        # print(base_candles, combined_base_candle["open"], combined_base_candle["close"])
        lowest_body = round(combined_base_candle["lowest_body"], 1)
        return (start_date, end_date, highest_wick, lowest_body)
    
    elif zone_type == 'DBR':  # Double Bottom Reversal
        print(start_date)
        lowest_wick = round(min(combined_base_candle['low'], start_candle["low"]), 1)
        highest_body = round(combined_base_candle["highest_body"], 1)
        return (start_date, end_date, lowest_wick, highest_body)
    
    else:
        return None

# Now find base candles between consecutive long candles
consecutive_long_candles_with_base_gap = []

for i in range(1, len(long_candle_dates)):
    start_date = long_candle_dates[i-1]
    end_date = long_candle_dates[i]
    first_candle_color = classified_candles[start_date]["color"]
    second_candle_color = classified_candles[end_date]["color"]
    
    # Determine zone type and classification
    if first_candle_color == 'r' and second_candle_color == 'r':
        zone_type = 'DBD'
        zone_classification = 'Supply'
    elif first_candle_color == 'g' and second_candle_color == 'g':
        zone_type = 'RBR'
        zone_classification = 'Demand'
    elif first_candle_color == 'g' and second_candle_color == 'r':
        zone_type = 'RBD'
        zone_classification = 'Supply'
    elif first_candle_color == 'r' and second_candle_color == 'g':
        zone_type = 'DBR'
        zone_classification = 'Demand'
    else:
        zone_type = 'Unknown'
        zone_classification = 'Unknown'
    # Ensure the candles between long candles are base candles
    price_range = calculate_zone_price_range(zone_type, start_date, end_date)
    
    if price_range:
        consecutive_long_candles_with_base_gap.append(price_range)

# Output the results
print(consecutive_long_candles_with_base_gap)

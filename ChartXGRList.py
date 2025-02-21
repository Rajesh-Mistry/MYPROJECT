import configparser
import sqlite3
import matplotlib.pyplot as plt
import mplfinance as mpf
import pandas as pd
import yfinance as yf
import numpy as np
import pytz
from fyers_apiv3 import fyersModel
import configparser
import datetime
from dateutil.relativedelta import relativedelta
import time


# Flag to track whether the chart is in full screen
window_maximized = False


import configparser
import os


def convert_to_nse_symbol(symbol):
    # Remove any suffix like '.NS' and prepend 'NSE:'
    if symbol.endswith('.NS'):
        return 'NSE:' + symbol.split('.')[0] + '-EQ'
    else:
        return 'NSE:' + symbol + '-EQ'
    

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


# Function to fetch the stock price results from the database sorted by nearest_diff
def fetch_sorted_price_diff_data(database_name):
    conn = sqlite3.connect(database_name)
    cursor = conn.cursor()

    query = """
    SELECT symbol, price_range_low, price_range_high, start_date, end_date, timeframe
    FROM GreenRedList
    ORDER BY tested_date ASC;
    """

    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    # Store the results
    result_data = []
    for row in rows:
        result_data.append({
            'symbol': row[0],
            # 'current_price': row[1],
            'price_range_low': row[1],
            'price_range_high': row[2],
            # 'nearest_range': row[4],
            # 'nearest_diff': row[5],
            'start_date': row[3],
            'end_date': row[4],
            'timeframe': row[5]  # Fetch timeframe from the database
        })
    return result_data

def fetch_candlestick_data(stock_symbol, timeframe):
    stock = yf.Ticker(stock_symbol)
    
    # Adjust the period and interval based on the timeframe
    if timeframe in ['1h', '1hr']:
        data = stock.history(period="1mo", interval="1h")  # Fetch last month of 1-hour data
    elif timeframe == '2h':
        data1 = stock.history(period="1mo", interval="1h")  # Fetch last month of 1-hour data
        stock_symbol = convert_to_nse_symbol(stock_symbol)
        data = candles_2hr(r'config.ini', stock_symbol)  # Merge to 2-hour candles   
        print(data1)
        if data is None:
            print(f"No 2-hour candlestick data found for {stock_symbol}")
            return None
    else:
        data = stock.history(period="3mo")  # Default to 3 months of 1-day data
    
    # Ensure the data contains open, high, low, close, and volume columns for the candlestick chart
    if data.empty:
        print(f"No valid data for {stock_symbol} at the requested timeframe.")
        return None

    data = data[['Open', 'High', 'Low', 'Close', 'Volume']]
    
    # Make sure the index is a datetime index
    if not isinstance(data.index, pd.DatetimeIndex):
        data.index = pd.to_datetime(data.index)

    # Ensure the datetime index is timezone naive (remove timezone info if exists)
    if data.index.tzinfo is not None:  # Check if the index is timezone-aware
        data.index = data.index.tz_localize(None)  # Remove timezone information if present
    
    # Localize to +05:30 (Indian Standard Time)
    india_timezone = pytz.timezone('Asia/Kolkata')  # IST timezone (UTC+05:30)
    
    # Use pandas tz_localize method to localize the entire index at once
    data.index = data.index.tz_localize('UTC').tz_convert(india_timezone)
    
    data.index = data.index - pd.Timedelta(hours=5, minutes=30)
    return data
def plot_candlestick_chart(stock_symbol, data, zones):
    # Create additional plot elements to overlay zones
    add_plot = []
    
    # Fetch the current price for the stock
    stock = yf.Ticker(stock_symbol)
    current_price = stock.history(period="1d").iloc[-1]['Close']  # Latest closing price

    for zone in zones:
        # Convert start and end dates from database to pandas datetime, make them timezone naive
        zone_start_date = pd.to_datetime(zone['start_date']).tz_localize(None)
        zone_end_date = pd.to_datetime(zone['end_date']).tz_localize(None)
        
        print(f"Zone for {stock_symbol}: Start Date = {zone_start_date}, End Date = {zone_end_date}")
        
        # Localize the zone start and end dates to the same timezone as the candlestick data
        india_timezone = pytz.timezone('Asia/Kolkata')  # IST timezone (UTC+05:30)
        zone_start_date = india_timezone.localize(zone_start_date)
        zone_end_date = india_timezone.localize(zone_end_date)
        
        # Print the price range low and high for the current zone
        print(f"Price Range Low: {zone['price_range_low']}, Price Range High: {zone['price_range_high']}")

        # Check if the zone's start and end date are within the data period
        if zone_start_date < data.index[-1] and zone_end_date > data.index[0]:
            # Convert the zone's price range to the y-axis values on the chart
            zone_high = zone['price_range_high']
            zone_low = zone['price_range_low']
            
            # Determine if the zone is a supply or demand zone
            # If the nearest_diff is positive, it's a supply zone (high range); negative is a demand zone (low range)
            zone_color = 'red' if zone_high - zone_low > 0 else 'green'
            
            # We create a vertical rectangle by adding the zone as a line plot
            add_plot.append(mpf.make_addplot(
                np.full(len(data), zone_high),
                type='line',
                color=zone_color,
                linestyle='dotted',
                panel=0,
                secondary_y=False
            ))

            add_plot.append(mpf.make_addplot(
                np.full(len(data), zone_low),
                type='line',
                color=zone_color,
                linestyle='dotted',
                panel=0,
                secondary_y=False
            ))

            # Now, add the current price as a horizontal line within the zone
            if zone_low <= current_price <= zone_high:
                add_plot.append(mpf.make_addplot(
                    np.full(len(data), current_price),
                    type='line',
                    color='blue',  # Blue color for current price
                    linestyle='--',
                    panel=0,
                    secondary_y=False
                ))
                print(f"Current Price {current_price} is within the zone: {zone_low} - {zone_high}")

    # Custom X-axis format for 2-hour candles
    x_format = "%Y-%m-%d %H:%M"  # Date format: Year-Month-Day Hour:Minute
    
    # Plot using mplfinance
    mpf.plot(data, type='candle', style='charles', title=f"{stock_symbol} Candlestick Chart", ylabel='Price', volume=True, addplot=add_plot, datetime_format=x_format)

# Function to display candlestick charts for stocks based on the price difference
# Function to display candlestick charts for stocks based on the price difference
def display_candlestick_charts():
    # Fetch the sorted results from the database
    sorted_data = fetch_sorted_price_diff_data('../StockDZSZ.db')
    
    if not sorted_data:
        print("No results found in the database.")
        return

    for stock in sorted_data:
        symbol = stock['symbol']
        start_date = stock['start_date']
        timeframe = stock['timeframe']  # Get the timeframe directly from the database
        print(f"Displaying chart for: {symbol}")

        # Fetch historical candlestick data for the stock based on the determined timeframe
        candlestick_data = fetch_candlestick_data(symbol, timeframe)
        
        # Ensure that candlestick_data is not None before proceeding
        if candlestick_data is None or candlestick_data.empty:
            print(f"Could not fetch data for {symbol}. Skipping...")
            continue

        # Define the zones to overlay (we will use the same data as an example)
        zones = [
            {
                'symbol': symbol,
                'start_date': stock['start_date'],
                'end_date': stock['end_date'],
                'price_range_high': stock['price_range_high'],
                'price_range_low': stock['price_range_low']
            }
        ]

        # Display the candlestick chart with the demand/supply zones
        plot_candlestick_chart(symbol, candlestick_data, zones)

        # Ask the user if the zone is violated or not after displaying the chart
        violation_status = input("Enter 'V' if the zone is violated, 'A' to keep it as is: ").strip().upper()

        # Handle the user input after chart is displayed
        if violation_status in ['V', 'v']:
            conn1 = sqlite3.connect('../StockDZSZ.db')
            cur = conn1.cursor()
            cur.execute("DELETE FROM GreenRedList WHERE symbol=? AND price_range_low=?", (symbol, stock['price_range_low']))
            cur.close()
            conn1.commit()
            if timeframe == '1d':
                conn = sqlite3.connect('../StockDZSZ.db')
                cursor = conn.cursor()
                cursor.execute("UPDATE demand_supply_zones SET zone_status = 'Violated' WHERE symbol = ? AND price_range_high = ? ", (stock['symbol'], stock['price_range_high']))
                cursor.close()
                conn.commit()
            elif timeframe == '1hr':
                conn = sqlite3.connect('../StockTest.db')
                cursor = conn.cursor()
                cursor.execute("UPDATE demand_supply_zones_1hr SET zone_status = 'Violated' WHERE symbol = ? AND price_range_high = ? ", (stock['symbol'], stock['price_range_high']))
                cursor.close()
                conn.commit()
            elif timeframe == '2hr':
                conn = sqlite3.connect('../StockTest.db')
                cursor = conn.cursor()
                cursor.execute("UPDATE demand_supply_zones SET zone_status = 'Violated' WHERE symbol = ? AND price_range_high = ? ", (stock['symbol'], stock['price_range_high']))
                cursor.close()
                conn.commit()
        else:
            print(f"The zone for {symbol} is not violated.")
            zone_color = 'green'  # Unviolated zones are shown in green
            line_style = 'dotted'  # Dotted line for non-violated zones

        # Wait for user input to display the next chart
        input("Press Enter to display the next chart...")

# Main entry point  
if __name__ == "__main__":
    display_candlestick_charts()

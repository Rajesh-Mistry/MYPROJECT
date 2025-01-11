import sqlite3
import matplotlib.pyplot as plt
import mplfinance as mpf
import pandas as pd
import yfinance as yf
import numpy as np

# Flag to track whether the chart is in full screen
window_maximized = False

# Function to fetch the stock price results from the database sorted by nearest_diff
def fetch_sorted_price_diff_data(database_name):
    conn = sqlite3.connect(database_name)
    cursor = conn.cursor()

    query = """
    SELECT symbol, current_price, price_range_low, price_range_high, nearest_range, nearest_diff, start_date, end_date, timeframe
    FROM stock_price_results
    ORDER BY nearest_diff ASC;
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
            'current_price': row[1],
            'price_range_low': row[2],
            'price_range_high': row[3],
            'nearest_range': row[4],
            'nearest_diff': row[5],
            'start_date': row[6],
            'end_date': row[7],
            'timeframe': row[8]  # Fetch timeframe from the database
        })
    return result_data

# Function to merge consecutive 1-hour candles into 2-hour candles, including volume aggregation
def merge_to_2_hour_candles(stock_data):
    """
    Merges consecutive 1-hour candles into 2-hour candles, including volume aggregation.
    """
    two_hour_candles = []
    
    # Iterate over the stock data, stepping through the candles two by two (index i and i+1)
    for i in range(0, len(stock_data) - 1, 2):  # Step by 2 to combine two 1-hour candles
        # Open: First hour's open, Close: Last hour's close
        o = stock_data.iloc[i]['Open']
        c = stock_data.iloc[i + 1]['Close']
        
        # High: Maximum high of the two hours, Low: Minimum low of the two hours
        h = max(stock_data.iloc[i]['High'], stock_data.iloc[i + 1]['High'])
        l = min(stock_data.iloc[i]['Low'], stock_data.iloc[i + 1]['Low'])
        
        # Volume: Sum of both hours' volume
        volume = stock_data.iloc[i]['Volume'] + stock_data.iloc[i + 1]['Volume']
        
        # Use the starting time of the first candle in the pair for the 2-hour candle
        two_hour_candles.append([stock_data.index[i], o, h, l, c, volume])
    
    # Create a DataFrame with the 2-hour candles
    two_hour_df = pd.DataFrame(two_hour_candles, columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
    two_hour_df.set_index('Date', inplace=True)
    
    return two_hour_df

# Function to fetch historical candlestick data for a given stock symbol
def fetch_candlestick_data(stock_symbol, timeframe):
    stock = yf.Ticker(stock_symbol)
    
    # Adjust the period and interval based on the timeframe
    if timeframe == '1hr':
        data = stock.history(period="1mo", interval="1h")  # Fetch last month of 1-hour data
    elif timeframe == '2hr':
        data1 = stock.history(period="1mo", interval="1h")  # Fetch last month of 1-hour data
        data = merge_to_2_hour_candles(data1)  # Merge to 2-hour candles   
    else:
        data = stock.history(period="3mo")  # Default to 3 months of 1-day data
    
    # Ensure the data contains open, high, low, close, and volume columns for the candlestick chart
    data = data[['Open', 'High', 'Low', 'Close', 'Volume']]
    
    # Make sure the stock data is timezone naive
    if data.index.tz is not None:  # Check if the index is timezone-aware
        data.index = data.index.tz_localize(None)  # Remove timezone information if present
    
    return data

# Function to plot the candlestick chart with zones
def plot_candlestick_chart(stock_symbol, data, zones):
    # Create additional plot elements to overlay zones
    add_plot = []
    
    for zone in zones:
        # Convert start and end dates from database to pandas datetime, make them timezone naive
        zone_start_date = pd.to_datetime(zone['start_date']).tz_localize(None)
        zone_end_date = pd.to_datetime(zone['end_date']).tz_localize(None)
        
        print(f"Zone for {stock_symbol}: Start Date = {zone_start_date}, End Date = {zone_end_date}")

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
    
    # Custom X-axis format for 2-hour candles
    x_format = "%Y-%m-%d %H:%M"  # Date format: Year-Month-Day Hour:Minute
    
    # Plot using mplfinance
    mpf.plot(data, type='candle', style='charles', title=f"{stock_symbol} Candlestick Chart", ylabel='Price', volume=True, addplot=add_plot, datetime_format=x_format)

# Function to display candlestick charts for stocks based on the price difference
def display_candlestick_charts():
    # Fetch the sorted results from the database
    sorted_data = fetch_sorted_price_diff_data('../StockDZSZNDX.db')
    
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
        
        if candlestick_data.empty:
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

        # Wait for user input to display the next chart
        input("Press Enter to display the next chart...")

# Main entry point  
if __name__ == "__main__":
    display_candlestick_charts()

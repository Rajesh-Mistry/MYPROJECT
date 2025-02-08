import yfinance as yf
import pandas as pd
import sqlite3
from fyers_apiv3 import fyersModel
import configparser
import datetime
from dateutil.relativedelta import relativedelta
import time
import os

def process_stock_data_for_Engulfing_Candle(ticker, db_path, table_name):
    # Fetch zones from the database, ordered by 'nearest_diff'
    def fetch_zones_from_db(ticker, db_path, table_name):
        conn = sqlite3.connect(db_path)
        query = f"""
        SELECT price_range_low, price_range_high, end_date, nearest_diff
        FROM {table_name}
        WHERE symbol = '{ticker}'
        ORDER BY nearest_diff ASC
        """
        zones = pd.read_sql(query, conn)
        conn.close()
        return zones

    def convert_to_nse_symbol(symbol):
        # Remove any suffix like '.NS' and prepend 'NSE:'
        if symbol.endswith('.NS'):
            return 'NSE:' + symbol.split('.')[0] + '-EQ'
        else:
            return 'NSE:' + symbol + '-EQ'

    # Download stock data for 15-minute interval
    def download_stock_data(ticker, period="1mo", interval="15m"):
        confile = r'config.ini'
        config = configparser.ConfigParser()

        if not os.path.exists(confile):
            print(f"Config file not found: {confile}")
            return None
        
        config.read(confile)  # Read the configuration file

        if 'FyersAPI' not in config:
            print("FyersAPI section not found in config file.")
            return None

        client_id = config['FyersAPI']['client_id']
        access_token = config['FyersAPI']['access_token']

        fyers = fyersModel.FyersModel(client_id=client_id, is_async=False, token=access_token, log_path="")

        today = datetime.date(2025, 1, 24)
        month1bef = today - relativedelta(days=30)

        range_from_epoch = int(time.mktime(month1bef.timetuple()))
        range_to_epoch = int(time.mktime(today.timetuple()))

        data = {
            "symbol": ticker,
            "resolution": "15",  # For 15-minute interval
            "date_format": "0",
            "range_from": range_from_epoch,
            "range_to": range_to_epoch,
            "cont_flag": "1"
        }

        response = fyers.history(data=data)
        candles_data = response.get('candles', [])
        if not candles_data:
            print(f"No candles data found for symbol: {ticker}")
            return None

        converted_data = []
        for candle in candles_data:
            date = datetime.datetime.fromtimestamp(candle[0]).strftime('%Y-%m-%d %H:%M:%S')
            open_price = candle[1]
            high_price = candle[2]
            low_price = candle[3]
            close_price = candle[4]
            volume = candle[5]
            converted_data.append([date, open_price, high_price, low_price, close_price, volume])

        fifteen_min_df = pd.DataFrame(converted_data, columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])

        # Convert 'Date' to datetime
        fifteen_min_df['Date'] = pd.to_datetime(fifteen_min_df['Date'])

        fifteen_min_df.set_index('Date', inplace=True)

        if fifteen_min_df.empty:
            print(f"No valid stock data returned for {ticker}.")
            return None
        
        return fifteen_min_df

    # Identify Bullish and Bearish Engulfing patterns
    def identify_engulfing_patterns(data, engulf_percentage=0.7):
        try:
            data['Open_Previous'] = data['Open'].shift(1)
            data['Close_Previous'] = data['Close'].shift(1)
            previous_candle_range = abs(data['Close_Previous'] - data['Open_Previous'])

            # Bullish Engulfing pattern
            data['Bullish_Engulfing'] = (
                (data['Open'] <= data['Close_Previous']) &
                (data['Open'] < data['Open_Previous']) &
                (data['Close'] > data['Open_Previous']) &
                (abs(data['Close'] - data['Open']) >= engulf_percentage * previous_candle_range)
            )

            # Bearish Engulfing pattern
            data['Bearish_Engulfing'] = (
                (data['Open'] >= data['Close_Previous']) &
                (data['Open'] > data['Open_Previous']) &
                (data['Close'] < data['Open_Previous']) &
                (abs(data['Close'] - data['Open']) >= engulf_percentage * previous_candle_range)
            )

        except Exception as e:
            print(f"Error identifying engulfing patterns: {e}")
            return None  # Return None if there's an error

    # Filter Bullish and Bearish Engulfing Candles inside the Demand and Supply zones
    def filter_engulfing_candles_in_zone(data, zones):
        filtered_bullish = []
        filtered_bearish = []
        
        for index, zone in zones.iterrows():
            zone_low = zone['price_range_low']
            zone_high = zone['price_range_high']
            zone_type = "Demand Zone" if zone_low > zone_high else "Supply Zone"
            zone_end_date = pd.to_datetime(zone['end_date'])
            
            data['Upper_Wick'] = data['High'] - data[['Close', 'Open']].max(axis=1)
            data['Lower_Wick'] = data[['Close', 'Open']].min(axis=1) - data['Low']
            data['Candle_Range'] = data['High'] - data['Low']

            # Only consider candles that are after the zone's end_date
            data_after_zone_end = data[data.index > zone_end_date]
            
            if zone_type == "Demand Zone":
                bullish_in_zone = data_after_zone_end[(data_after_zone_end['Bullish_Engulfing']) & 
                                                       (data_after_zone_end['Low'] >= zone_low) &
                                                       (data_after_zone_end['High'] <= zone_high) &
                                                       (data_after_zone_end['Upper_Wick'] / data_after_zone_end['Candle_Range'] < 0.2)]
                filtered_bullish.append(bullish_in_zone)

            if zone_type == "Supply Zone":
                bearish_in_zone = data_after_zone_end[(data_after_zone_end['Bearish_Engulfing']) & 
                                                       (data_after_zone_end['Low'] >= zone_low) &
                                                       (data_after_zone_end['High'] <= zone_high) &
                                                       (data_after_zone_end['Lower_Wick'] / data_after_zone_end['Candle_Range'] < 0.2)]
                filtered_bearish.append(bearish_in_zone)
        
        # Combine filtered results
        bullish_in_zone_combined = pd.concat(filtered_bullish) if filtered_bullish else pd.DataFrame()
        bearish_in_zone_combined = pd.concat(filtered_bearish) if filtered_bearish else pd.DataFrame()

        return bullish_in_zone_combined, bearish_in_zone_combined

    # Main process
    ticker2 = convert_to_nse_symbol(ticker)
    data = download_stock_data(ticker2)  # This fetches 15-minute data
    
    if data is None:
        print(f"Skipping {ticker} due to missing or invalid data.")
        return

    zones = fetch_zones_from_db(ticker, db_path, table_name)  # Fetching zones for '1hr' timeframe
    identify_engulfing_patterns(data, engulf_percentage=0.7)
    bullish_in_zone, bearish_in_zone = filter_engulfing_candles_in_zone(data, zones)

    # Print the Bearish Engulfing Candles inside Supply Zones with zone range
    print(f"\nBearish Engulfing Candles Inside Supply Zones for {ticker}:")
    if not bearish_in_zone.empty:
        print("Bearish engulfing candles found inside supply zones.")  # Placeholder, as we don't need detailed info here.
    else:
        print("No bearish engulfing candles found inside supply zones.")

# Example usage for a list of Nifty 200 symbols
# nifty_200_symbols = [
#     "ABB", "ABFRL", "ABCAPITAL", "ADANIENT", "ADANIGREEN", "ADANIPOWER", "ADANIPORTS", "ALKE", "APOLLOHOSP",
#     # ... (same list as before)
# ]

# nifty_200_symbols = [i + ".NS" for i in nifty_200_symbols]

conn = sqlite3.connect("../StockDZSZ.db")
query = f"""
SELECT symbol
FROM stock_price_results
WHERE timeframe = '1hr'
ORDER BY nearest_diff ASC
"""

nifty_200_symbols = [row[0] for row in conn.execute(query).fetchall()]

for symbol in nifty_200_symbols:
    process_stock_data_for_Engulfing_Candle(symbol, '../StockDZSZ.db', 'stock_price_results')

conn.close()
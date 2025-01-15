import pandas as pd
import numpy as np
import requests
import time

def get_btcmyr_data():
    url = 'https://ajax.luno.com/ajax/1/charts_candles'
    
    headers = {
        'accept': 'application/json, text/plain, */*',
        'origin': 'https://www.luno.com',
        'referer': 'https://www.luno.com/',
        'x-luno-override-language': 'en'
    }
    
    params = {
        'base': 'XBT',
        'counter': 'MYR',
        'since': int(time.time()) - 24*60*60,  # Last 24 hours
        'include_partial': 'true'
    }
    
    response = requests.get(url, headers=headers, params=params)
    return response.json()

def get_usdmyr_data():
    url = 'https://query2.finance.yahoo.com/v8/finance/chart/USDMYR=X'
    
    headers = {
        'accept': '*/*',
        'origin': 'https://finance.yahoo.com',
        'referer': 'https://finance.yahoo.com/',
        'user-agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1'
    }
    
    params = {
        'period1': int(time.time()) - 24*60*60,  # Last 24 hours
        'period2': int(time.time()),
        'interval': '1m',
        'includePrePost': 'true',
        'events': 'div|split|earn',
        'lang': 'en-US',
        'region': 'US'
    }
    
    response = requests.get(url, headers=headers, params=params)
    return response.json()

def get_btcusd_data():
    url = 'https://query2.finance.yahoo.com/v8/finance/chart/BTC-USD'
    
    headers = {
        'accept': '*/*',
        'origin': 'https://finance.yahoo.com',
        'referer': 'https://finance.yahoo.com/',
        'user-agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1'
    }
    
    params = {
        'period1': int(time.time()) - 24*60*60,  # Last 24 hours
        'period2': int(time.time()),
        'interval': '1m',
        'includePrePost': 'true',
        'events': 'div|split|earn',
        'lang': 'en-US',
        'region': 'US'
    }
    
    response = requests.get(url, headers=headers, params=params)
    return response.json()

def process_btcmyr_data(data):
    try:
        candles = data['candles']
        df = pd.DataFrame(candles)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')

        # Convert string prices to float
        for col in ['open', 'close', 'high', 'low']:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # Set timestamp as index
        df = df.set_index('timestamp')

        # Resample to 1 minute and interpolate missing values
        df = df.resample('1T').mean().interpolate(method='time')

        return df
    except Exception as e:
        return {
            "error": f"Error processing BTC/MYR data: {str(e)}"
        }

def process_yahoo_data(data, pair_name):
    try:
        # Extract all available price data
        result = data['chart']['result'][0]
        timestamp = result['timestamp']
        close_prices = result['indicators']['quote'][0]['close']
        
        # Create DataFrame
        df = pd.DataFrame({
            'timestamp': pd.to_datetime(timestamp, unit='s'),
            pair_name: close_prices
        })

        # Convert prices to numeric, handling any strings
        df[pair_name] = pd.to_numeric(df[pair_name], errors='coerce')

        # Set timestamp as index
        df = df.set_index('timestamp')

        # Resample to 1 minute and interpolate missing values
        df = df.resample('1T').mean().interpolate(method='time')

        return df
    except Exception as e:
        return {
            "error": f"Error processing {pair_name} data: {str(e)}"
        }

def calculate_price_difference(btcmyr_price, implied_btcmyr):
    try:
        return ((btcmyr_price - implied_btcmyr) / implied_btcmyr) * 100
    except Exception as e:
        return {
            "error": f"Error calculating price difference: {str(e)}"
        }

def main():
    try:
        # Fetch live data for all pairs
        btcmyr_data = get_btcmyr_data()
        usdmyr_data = get_usdmyr_data()
        btcusd_data = get_btcusd_data()
        
        # Process each dataset
        btcmyr_df = process_btcmyr_data(btcmyr_data)
        btcusd_df = process_yahoo_data(btcusd_data, 'BTC/USD')
        usdmyr_df = process_yahoo_data(usdmyr_data, 'USD/MYR')
        
        # Get latest prices and convert to native Python float
        btcmyr_price = float(btcmyr_df['close'].iloc[-1]) if not pd.isna(btcmyr_df['close'].iloc[-1]) else 0
        btcusd_price = float(btcusd_df['BTC/USD'].iloc[-1]) if not pd.isna(btcusd_df['BTC/USD'].iloc[-1]) else 0
        usdmyr_price = float(usdmyr_df['USD/MYR'].iloc[-1]) if not pd.isna(usdmyr_df['USD/MYR'].iloc[-1]) else 0
        
        # Calculate implied BTC/MYR
        implied_btcmyr = btcusd_price * usdmyr_price

        # Align DataFrames based on timestamp index
        aligned_data = pd.concat([
            btcmyr_df['close'],
            btcusd_df['BTC/USD'],
            usdmyr_df['USD/MYR']
        ], axis=1, join='inner')
        
        # Convert to numpy arrays and handle NaN values
        btcmyr_prices = aligned_data['close'].fillna(0).to_numpy()
        btcusd_prices = aligned_data['BTC/USD'].fillna(0).to_numpy()
        usdmyr_prices = aligned_data['USD/MYR'].fillna(0).to_numpy()
        
        # Calculate implied prices and difference
        implied_prices = btcusd_prices * usdmyr_prices
        diff = ((btcmyr_prices - implied_prices) / implied_prices) * 100
        diff = diff[~np.isnan(diff)]

        # Calculate statistics and convert to native Python float
        if len(diff) > 0:
            median_diff = float(np.median(diff))
            percentile_25 = float(np.percentile(diff, 25))
            percentile_75 = float(np.percentile(diff, 75))
            percentile_90 = float(np.percentile(diff, 90))
            percentile_5 = float(np.percentile(diff, 5))
            max_diff = float(np.max(diff))
            min_diff = float(np.min(diff))
            current_diff = float(diff[-1])
        else:
            median_diff = percentile_25 = percentile_75 = percentile_90 = percentile_5 = max_diff = min_diff = current_diff = 0.0

        status_message = ""
        if current_diff > 0:
            status_message = "Price on Luno is higher than implied BTC/MYR"
        else:
            status_message = "Price on Luno is lower than implied BTC/MYR"
            
        # Add comparison with median
        if abs(current_diff) > abs(median_diff):
            status_message += f" (Price difference of {current_diff:.2f}% is larger than usual {median_diff:.2f}%)"
        else:
            status_message += f" (Price difference of {current_diff:.2f}% is smaller than usual {median_diff:.2f}%)"

        output = f"""
        {status_message}
        
        • Prices:
          - BTC/MYR (Luno): {btcmyr_price:,.2f}
          - BTC/MYR (Implied): {implied_btcmyr:,.2f}
          - BTC/USD: {btcusd_price:,.2f}
          - USD/MYR: {usdmyr_price:,.4f}
        
        • Price Difference Analysis:
          - Current Difference: {current_diff:,.2f}%
          - Minimum Difference: {min_diff:,.2f}%
          - 5th Percentile: {percentile_5:,.2f}%
          - 25th Percentile: {percentile_25:,.2f}%
          - Median Difference: {median_diff:,.2f}%
        """
        return output
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

return main()

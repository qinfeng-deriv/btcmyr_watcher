import json
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import requests
import time

def load_json_file(filename):
    with open(filename, 'r') as f:
        return json.load(f)

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
    candles = data['candles']
    df = pd.DataFrame(candles)
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')

    # Convert string prices to float
    for col in ['open', 'close', 'high', 'low']:
        df[col] = df[col].astype(float)

    # Set timestamp as index
    df = df.set_index('timestamp')

    # Resample to 1 minute and interpolate missing values
    df = df.resample('1T').mean().interpolate(method='time')

    return df

def process_yahoo_data(data, pair_name):
    # Extract all available price data
    result = data['chart']['result'][0]
    timestamp = result['timestamp']
    close_prices = result['indicators']['quote'][0]['close']
    
    # Create DataFrame
    df = pd.DataFrame({
        'timestamp': pd.to_datetime(timestamp, unit='s'),
        pair_name: close_prices
    })

    # Set timestamp as index
    df = df.set_index('timestamp')

    # Resample to 1 minute and interpolate missing values
    df = df.resample('1T').mean().interpolate(method='time')

    return df

def align_and_plot_data(btcmyr_df, btcusd_df, usdmyr_df):
    # Join all dataframes on timestamp
    combined_df = pd.concat([
        btcmyr_df['close'].rename('BTC/MYR'),
        btcusd_df['BTC/USD'],
        usdmyr_df['USD/MYR']
    ], axis=1)
    
    # Forward fill missing values
    combined_df = combined_df.ffill()
    
    # Calculate implied BTC/MYR
    combined_df['BTC/MYR (implied)'] = combined_df['BTC/USD'] * combined_df['USD/MYR']
    
    # Calculate differences
    combined_df['Difference (MYR)'] = combined_df['BTC/MYR'] - combined_df['BTC/MYR (implied)']
    combined_df['Difference (%)'] = (combined_df['Difference (MYR)'] / combined_df['BTC/MYR (implied)'] * 100)
    
    # Create the plot
    plt.figure(figsize=(15, 10))
    
    # Create two y-axes
    ax1 = plt.gca()
    ax2 = ax1.twinx()
    
    # Plot BTC prices on left axis
    line1 = ax1.plot(combined_df.index, combined_df['BTC/MYR'], 
                     label='BTC/MYR (actual)', color='blue', linewidth=2)
    line2 = ax1.plot(combined_df.index, combined_df['BTC/MYR (implied)'], 
                     label='BTC/MYR (implied)', color='red', linestyle='--', linewidth=2)
    
    # Plot USD/MYR on right axis
    line3 = ax2.plot(combined_df.index, combined_df['USD/MYR'], 
                     label='USD/MYR', color='green', linewidth=2)
    
    # Set labels and title
    ax1.set_xlabel('Time', fontsize=12)
    ax1.set_ylabel('BTC Price (MYR)', fontsize=12)
    ax2.set_ylabel('USD/MYR Rate', fontsize=12)
    plt.title('Bitcoin Prices and Exchange Rates', fontsize=14, pad=20)
    
    # Combine legends from both axes
    lines = line1 + line2 + line3
    labels = [line.get_label() for line in lines]
    ax1.legend(lines, labels, loc='upper left')
    
    # Rotate x-axis labels
    plt.xticks(rotation=45)
    
    # Grid
    ax1.grid(True, alpha=0.3)
    
    # Adjust layout
    plt.tight_layout()
    
    # Save and show the plot
    plt.savefig('crypto_analysis.png')
    plt.show()
    
    return combined_df

def main():
    # Fetch live data for all pairs
    btcmyr_data = get_btcmyr_data()
    usdmyr_data = get_usdmyr_data()
    btcusd_data = get_btcusd_data()
    
    # Process each dataset
    btcmyr_df = process_btcmyr_data(btcmyr_data)
    btcusd_df = process_yahoo_data(btcusd_data, 'BTC/USD')
    usdmyr_df = process_yahoo_data(usdmyr_data, 'USD/MYR')
    
    # Get latest prices
    btcmyr_price = btcmyr_df['close'].iloc[-1]
    btcusd_price = btcusd_df['BTC/USD'].iloc[-1]
    usdmyr_price = usdmyr_df['USD/MYR'].iloc[-1]
    
    # Calculate implied BTC/MYR
    implied_btcmyr = btcusd_price * usdmyr_price

    # Align DataFrames based on timestamp index
    aligned_data = pd.concat([
        btcmyr_df['close'],
        btcusd_df['BTC/USD'],
        usdmyr_df['USD/MYR']
    ], axis=1, join='inner')  # inner join keeps only timestamps present in all series
    
    # Now convert to numpy arrays - they'll all have the same length
    btcmyr_prices = aligned_data['close'].to_numpy()
    btcusd_prices = aligned_data['BTC/USD'].to_numpy()
    usdmyr_prices = aligned_data['USD/MYR'].to_numpy()
    
    # Calculate implied prices and difference in one vectorized operation
    implied_prices = btcusd_prices * usdmyr_prices
    diff = ((btcmyr_prices - implied_prices) / implied_prices) * 100

    # Remove any NaN values that might have occurred during calculation
    diff = diff[~np.isnan(diff)]

    # Calculate statistics using numpy
    median_diff = np.median(diff)
    percentile_25 = np.percentile(diff, 25)
    percentile_75 = np.percentile(diff, 75)
    percentile_90 = np.percentile(diff, 90)
    percentile_5 = np.percentile(diff, 5)
    max_diff = np.max(diff)
    min_diff = np.min(diff)

    diff_pct = diff[-1]
    print("Price difference stats for the past hour")
    print(f"Max difference: {max_diff:.2f}%")
    print(f"Min difference: {min_diff:.2f}%")
    print(f"5th percentile: {percentile_5:.2f}%")
    print(f"25th percentile: {percentile_25:.2f}%")
    print(f"Median difference: {median_diff:.2f}%")
    print(f"75th percentile: {percentile_75:.2f}%")
    print(f"90th percentile: {percentile_90:.2f}%")
    print("==========")
    print(f"\nBTC/MYR: {btcmyr_price:,.2f}")
    print(f"BTC/USD × USD/MYR: {implied_btcmyr:,.2f}")
    # print(f"Difference: {diff_myr:,.2f} MYR ({diff_pct:.2f}%)")

    if diff_pct > 1:
        print(f"Price on Luno is {diff_pct} % higher than implied BTC/MYR")
    else:
        print(f"Price on Luno is {diff_pct} % lower than implied BTC/MYR")


    combined_df = align_and_plot_data(btcmyr_df, btcusd_df, usdmyr_df)

if __name__ == "__main__":
    main()

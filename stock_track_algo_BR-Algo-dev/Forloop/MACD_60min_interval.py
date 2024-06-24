import pandas as pd
import yfinance as yf
import numpy as np

# Function to fetch historical data
def fetch_historical_data(ticker, interval='60m', period='5mo'):
    df = yf.download(ticker, interval=interval, period=period)
    return df

# Calculate MACD and Signal line
def calculate_macd(data, short_window=34, large_window=81):
    # Calculate the 12-period Exponential Moving Average (EMA)
    data['EMA_short_60m'] = data['Close'].ewm(span=short_window, adjust=False).mean()

    # Calculate the 26-period Exponential Moving Average (EMA)
    data['EMA_large_60m'] = data['Close'].ewm(span=large_window, adjust=False).mean()

    # Calculate the Moving Average Convergence Divergence (MACD) line
    data['MACD_60m'] = data['EMA_short_60m'] - data['EMA_large_60m']

    # Calculate the 9-period EMA of MACD (Signal Line)
    data['Signal_Line_60m'] = data['MACD_60m'].ewm(span=9, adjust=False).mean()

    # Generate a signal: 1 for MACD line above Signal line, 0 otherwise
    data['msg_signal_60m'] = np.where(data['MACD_60m'] > data['Signal_Line_60m'].shift(1), 1, 0)

    return data

# Function to check and print crossover events
def check_crossover(data):
    for i in range(len(data)-1, -1, -1):
        if (data['msg_signal_60m'].iloc[i] == 0) and (data['msg_signal_60m'].iloc[i-1] == 1):
            print(f"MACD Crossover Alert: \nThe Cross over to below signal line in 60m time interval on {data.index[i]}")
        elif (data['msg_signal_60m'].iloc[i] == 1) and (data['msg_signal_60m'].iloc[i-1] == 0):
            print(f"MACD Crossover Alert: \nThe Cross over to above signal line in 60m time interval on {data.index[i]}")
            # send_whatsapp_messages("leads.csv", message)

# Main execution
if __name__ == "__main__":
    ticker = "RELIANCE.NS"  # You can change this to any ticker symbol you want
    df = fetch_historical_data(ticker)
    df = calculate_macd(df, short_window=34, large_window=81)
    check_crossover(df)

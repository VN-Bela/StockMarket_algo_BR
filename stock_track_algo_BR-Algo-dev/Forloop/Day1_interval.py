import pandas as pd
import yfinance as yf
import numpy as np

# Function to fetch historical data
def fetch_historical_data(ticker, interval='1d', period='5mo'):
    df = yf.download(ticker, interval=interval, period=period)
    return df

# Calculate MACD and Signal line
def calculate_macd(data, short_window=12, large_window=26):
    # Calculate the 12-period Exponential Moving Average (EMA)
    data['EMA_short_1d'] = data['Close'].ewm(span=short_window, adjust=False).mean()

    # Calculate the 26-period Exponential Moving Average (EMA)
    data['EMA_large_1d'] = data['Close'].ewm(span=large_window, adjust=False).mean()

    # Calculate the Moving Average Convergence Divergence (MACD) line
    data['MACD_1d'] = data['EMA_short_1d'] - data['EMA_large_1d']

    # Calculate the 9-period EMA of MACD (Signal Line)
    data['Signal_Line_1d'] = data['MACD_1d'].ewm(span=9, adjust=False).mean()

    # Generate a signal: 1 for MACD line above Signal line, 0 otherwise
    data['msg_signal_1d'] = np.where(data['MACD_1d'] > data['Signal_Line_1d'].shift(1), 1, 0)

    print(data.tail(5))
    return data

# Function to check and print crossover events
def check_crossover(data):
    for i in range(len(data)-1, -1, -1):
        #print(i)
        if (data['msg_signal_1d'].iloc[i] == 0) and (data['msg_signal_1d'].iloc[i-1] == 1):
            print(f"MACD Crossover Alert: \nThe Cross over to below signal line in  1day time interval on {data.index[i]}")
        elif (data['msg_signal_1d'].iloc[i] == 1) and (data['msg_signal_1d'].iloc[i-1] == 0):
            print(f"MACD Crossover Alert: \nThe Cross over to above signal line in 1day time interval on {data.index[i]}")
            # send_whatsapp_messages("leads.csv", message)
        # else:
        #     print(f"No crossover alert:{data.index[i]}")

# Main execution
if __name__ == "__main__":
    ticker = "RELIANCE.NS"  # You can change this to any ticker symbol you want
    df = fetch_historical_data(ticker)
    df = calculate_macd(df, short_window=12, large_window=26)
    check_crossover(df)



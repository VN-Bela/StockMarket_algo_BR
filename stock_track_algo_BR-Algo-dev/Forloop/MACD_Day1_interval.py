import pandas as pd
import yfinance as yf
import numpy as np

# Function to fetch historical data
def get_historical_data(symbol, start_date, interval):
    raw_df = yf.download(symbol, start=start_date, interval=interval)
    df = pd.DataFrame(raw_df)
    return df

# Calculate MACD and Signal line
def calculate_macd(data, short_window=34, large_window=81,signal_window=9):
    # Calculate the 12-period Exponential Moving Average (EMA)
    data['EMA_short_1d'] = data['Close'].ewm(span=short_window, adjust=False).mean()

    # Calculate the 26-period Exponential Moving Average (EMA)
    data['EMA_large_1d'] = data['Close'].ewm(span=large_window, adjust=False).mean()

    # Calculate the Moving Average Convergence Divergence (MACD) line
    data['MACD_1d'] = data['EMA_short_1d'] - data['EMA_large_1d']

    # Calculate the 9-period EMA of MACD (Signal Line)
    data['Signal_Line_1d'] = data['MACD_1d'].ewm(span=signal_window, adjust=False).mean()

    # # Generate a signal: 1 for MACD line above Signal line, 0 otherwise
    # data['msg_signal_1d'] = np.where(data['MACD_1d'] > data['Signal_Line_1d'].shift(1), 1, 0)

    # print(data.tail(5))
    return  data['MACD_1d'], data['Signal_Line_1d']

ticker = 'AXISBANK.NS'  # Example: Apple Inc.
data = yf.download(ticker, start='2023-01-01', interval='1d')
# Calculate MACD and Signal Line
data['MACD'], data['Signal_Line'] = calculate_macd(data)

# Identify the crossover points
data['Crossover'] = data['MACD'] - data['Signal_Line']
data['Signal'] = 0
data.loc[data['Crossover'] > 0, 'Signal'] = 1
data.loc[data['Crossover'] < 0, 'Signal'] = -1

# Print buy and sell signals
for i in range(1, len(data)):
    if data['Signal'].iloc[i] == 1 and data['Signal'].iloc[i - 1] == -1:
        print(f"Buy Signal on {data.index[i].date()} at price {data['Close'].iloc[i]}")
    elif data['Signal'].iloc[i] == -1 and data['Signal'].iloc[i - 1] == 1:
        print(f"Sell Signal on {data.index[i].date()} at price {data['Close'].iloc[i]}")



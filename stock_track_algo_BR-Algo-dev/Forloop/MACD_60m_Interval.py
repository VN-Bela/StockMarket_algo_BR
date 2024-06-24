import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt

# Function to calculate MACD and Signal Line
def calculate_macd(data, short_window=34, long_window=81, signal_window=9):
    # Calculate the short-term and long-term EMAs
    short_ema = data['Close'].ewm(span=short_window, adjust=False).mean()
    long_ema = data['Close'].ewm(span=long_window, adjust=False).mean()
    
    # Calculate the MACD line
    macd = short_ema - long_ema
    
    # Calculate the Signal line
    signal = macd.ewm(span=signal_window, adjust=False).mean()
    
    return macd, signal

# Fetch historical stock data
ticker = 'AXISBANK.NS'  # Example: Apple Inc.
data = yf.download(ticker, start='2023-01-01', interval='60m')

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

# Plotting
plt.figure(figsize=(14, 7))

# Plot the closing price
plt.subplot(2, 1, 1)
plt.plot(data['Close'], label='Close Price')
plt.title(f'{ticker} Price and MACD')
plt.legend()

# Plot MACD and Signal Line
plt.subplot(2, 1, 2)
plt.plot(data['MACD'], label='MACD', color='b')
plt.plot(data['Signal_Line'], label='Signal Line', color='r')

# Highlight the crossover points
buy_signals = data[(data['Signal'] == 1) & (data['Signal'].shift(1) == -1)]
sell_signals = data[(data['Signal'] == -1) & (data['Signal'].shift(1) == 1)]
plt.scatter(buy_signals.index, buy_signals['MACD'], marker='^', color='g', s=100, label='Buy Signal')
plt.scatter(sell_signals.index, sell_signals['MACD'], marker='v', color='r', s=100, label='Sell Signal')

plt.legend()
plt.show()

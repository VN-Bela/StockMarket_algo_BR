import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import yfinance as yf

# Function to fetch historical data
def fetch_historical_data(ticker, startdate, interval):
    intc_5m = yf.download(ticker, start=startdate, interval=interval)
    return intc_5m

# Fetching 5-minute interval data for AXISBANK.NS starting from 2024-05-01
intc_5m = fetch_historical_data('AXISBANK.NS', '2024-05-01', '5m')

# Displaying the first few rows of the DataFrame
print("First few rows of the 5-minute interval data:")
print(intc_5m.head())

# Function to calculate Keltner Channels
def get_kc(high, low, close, kc_lookback, multiplier, atr_lookback):
    tr1 = pd.DataFrame(high - low)
    tr2 = pd.DataFrame(abs(high - close.shift()))
    tr3 = pd.DataFrame(abs(low - close.shift()))
    frames = [tr1, tr2, tr3]
    tr = pd.concat(frames, axis=1, join='inner').max(axis=1)
    atr = tr.ewm(alpha=1/atr_lookback).mean()
    
    kc_middle = close.ewm(span=kc_lookback, adjust=False).mean()
    kc_upper = kc_middle + (multiplier * atr)
    kc_lower = kc_middle - (multiplier * atr)
    
    return kc_middle, kc_upper, kc_lower

intc_5m = intc_5m.iloc[:, :4]  # Selecting only the OHLC columns (Open, High, Low, Close)
intc_5m['kc_middle'], intc_5m['kc_upper'], intc_5m['kc_lower'] = get_kc(intc_5m['High'], intc_5m['Low'], intc_5m['Close'], 20, 2, 10)
print(intc_5m.tail())

# Keltner Channel Plot for 5-minute data
plt.plot(intc_5m['Close'], linewidth=2, label='AXISBANK.NS 5m')
plt.plot(intc_5m['kc_upper'], linewidth=2, color='orange', linestyle='--', label='KC UPPER 20')
plt.plot(intc_5m['kc_middle'], linewidth=1.5, color='grey', label='KC MIDDLE 20')
plt.plot(intc_5m['kc_lower'], linewidth=2, color='orange', linestyle='--', label='KC LOWER 20')
plt.legend(loc='lower right', fontsize=15)
plt.title('AXISBANK.NS 5-minute KELTNER CHANNEL 20')
plt.show()

# Keltner Channel Strategy with middle crossover
def implement_kc_strategy(prices, kc_upper, kc_middle, kc_lower):
    buy_price = []
    sell_price = []
    kc_signal = []
    signal = 0
    
    for i in range(len(prices) - 1):  # Adjusted to prevent out-of-bounds error
        if prices.iloc[i] < kc_lower.iloc[i] and prices.iloc[i + 1] > prices.iloc[i]:
            if signal != 1:
                buy_price.append(prices.iloc[i])
                sell_price.append(np.nan)
                signal = 1
                kc_signal.append(signal)
            else:
                buy_price.append(np.nan)
                sell_price.append(np.nan)
                kc_signal.append(0)
        elif prices.iloc[i] > kc_upper.iloc[i] and prices.iloc[i + 1] < prices.iloc[i]:
            if signal != -1:
                buy_price.append(np.nan)
                sell_price.append(prices.iloc[i])
                signal = -1
                kc_signal.append(signal)
            else:
                buy_price.append(np.nan)
                sell_price.append(np.nan)
                kc_signal.append(0)
        elif prices.iloc[i] > kc_middle.iloc[i] and prices.iloc[i + 1] < prices.iloc[i]:
            if signal != -1:
                buy_price.append(np.nan)
                sell_price.append(prices.iloc[i])
                signal = -1
                kc_signal.append(signal)
            else:
                buy_price.append(np.nan)
                sell_price.append(np.nan)
                kc_signal.append(0)
        elif prices.iloc[i] < kc_middle.iloc[i] and prices.iloc[i + 1] > prices.iloc[i]:
            if signal != 1:
                buy_price.append(prices.iloc[i])
                sell_price.append(np.nan)
                signal = 1
                kc_signal.append(signal)
            else:
                buy_price.append(np.nan)
                sell_price.append(np.nan)
                kc_signal.append(0)
        else:
            buy_price.append(np.nan)
            sell_price.append(np.nan)
            kc_signal.append(0)
    
    # Append NaNs for the last row since the loop does not cover it
    buy_price.append(np.nan)
    sell_price.append(np.nan)
    kc_signal.append(0)
    
    return buy_price, sell_price, kc_signal

buy_price_5m, sell_price_5m, kc_signal_5m = implement_kc_strategy(intc_5m['Close'], intc_5m['kc_upper'], intc_5m['kc_middle'], intc_5m['kc_lower'])

# Trading Signals Plot for 5-minute data
plt.plot(intc_5m['Close'], linewidth=2, label='AXISBANK.NS 5m')
plt.plot(intc_5m['kc_upper'], linewidth=2, color='orange', linestyle='--', label='KC UPPER 20')
plt.plot(intc_5m['kc_middle'], linewidth=1.5, color='grey', label='KC MIDDLE 20')
plt.plot(intc_5m['kc_lower'], linewidth=2, color='orange', linestyle='--', label='KC LOWER 20')
plt.plot(intc_5m.index, buy_price_5m, marker='^', color='green', markersize=15, linewidth=0, label='BUY SIGNAL')
plt.plot(intc_5m.index, sell_price_5m, marker='v', color='red', markersize=15, linewidth=0, label='SELL SIGNAL')
plt.legend(loc='lower right')
plt.title('AXISBANK.NS 5-minute KELTNER CHANNEL 20 TRADING SIGNALS')
plt.show()

# Stock Position for 5-minute data
position_5m = [0] * len(kc_signal_5m)  # Initialize positions with 0
for i in range(len(kc_signal_5m)):
    if kc_signal_5m[i] == 1:
        position_5m[i] = 1
    elif kc_signal_5m[i] == -1:
        position_5m[i] = 0
    else:
        if i > 0:
            position_5m[i] = position_5m[i-1]

close_price_5m = intc_5m['Close']
kc_upper_5m = intc_5m['kc_upper']
kc_middle_5m = intc_5m['kc_middle']
kc_lower_5m = intc_5m['kc_lower']
kc_signal_5m = pd.DataFrame(kc_signal_5m).rename(columns={0: 'kc_signal'}).set_index(intc_5m.index)
position_5m = pd.DataFrame(position_5m).rename(columns={0: 'kc_position'}).set_index(intc_5m.index)

frames_5m = [close_price_5m, kc_upper_5m, kc_middle_5m, kc_lower_5m, kc_signal_5m, position_5m]
strategy_5m = pd.concat(frames_5m, join='inner', axis=1)

print(strategy_5m)
strategy_5m.to_csv('kc_strategy_5m.csv')

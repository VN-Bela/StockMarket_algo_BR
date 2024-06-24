import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import yfinance as yf
from termcolor import colored as cl

plt.style.use('fivethirtyeight')
plt.rcParams['figure.figsize'] = (20, 10)

# Function to extract historical stock data
def get_historical_data(symbol, start_date, interval):
    raw_df = yf.download(symbol, start=start_date, interval=interval)
    df = pd.DataFrame(raw_df)
    return df

# Extracting data for Axis Bank (AXISBANK.NS)
intc = get_historical_data('AXISBANK.NS', '2023-01-01', '60m')

# Displaying the first few rows of the DataFrame
print("First few rows of the data:")
print(intc.head())

def cal_kc(df, n=20):
    df['TR'] = np.maximum(df['High'] - df['Low'], np.maximum(abs(df['High'] - df['Close'].shift(1)), abs(df['Low'] - df['Close'].shift())))
    df['ATR'] = df['TR'].rolling(window=n).mean()
    df['EMA_20'] = df['Close'].ewm(span=n, adjust=False).mean()
    df['KC_up'] = df['EMA_20'] + df['ATR'] * 2
    df['KC_lower'] = df['EMA_20'] - df['ATR'] * 2
    return df

kc_data = cal_kc(intc)
print(kc_data.tail())
#Adding crossover signals
# def add_kc_signals(df):
#     df['Upper_Cross_Signal'] = np.where((df['Close'] > df['KC_up']) & (df['Close'].shift(1) <= df['KC_up'].shift(1)), 1, 0)
#     df['Middle_Cross_Signal'] = np.where((df['Close'] < df['EMA_20']) & (df['Close'].shift(1) >= df['EMA_20'].shift(1)), -1, 0)
#     df['Lower_Cross_Signal'] = np.where((df['Close'] < df['KC_lower']) & (df['Close'].shift(1) >= df['KC_lower'].shift(1)), -1, 0)
#     return df

# kc_data = add_kc_signals(kc_data)
# print(kc_data[['Close', 'KC_up', 'EMA_20', 'KC_lower', 'Upper_Cross_Signal', 'Middle_Cross_Signal','Lower_Cross_Signal']].tail())

def add_kc_signals(df):
    df['Buy_Signal'] = np.where(df['Close'] > df['KC_up'], 1, 0)
    df['Sell_Signal'] = np.where(df['Close'] < df['EMA_20'], -1, 0)
    df['Lower_Signal'] = np.where(df['Close'] < df['KC_lower'], -1, 0)
    return df

kc_data = add_kc_signals(kc_data)
print(kc_data[['Close', 'KC_up', 'EMA_20', 'KC_lower','Buy_Signal','Sell_Signal']].tail())

# # Dropping NaN values
kc_data.dropna(inplace=True)
print(kc_data.tail())
kc_data.to_csv('kc_data_60mNew.csv')


from warnings import filterwarnings
filterwarnings('ignore')
# Import necessary libraries
import yfinance as yf  # Library to fetch stock data from Yahoo Finance
import pandas as pd  # Library for data manipulation and analysis
import numpy as np  # Library for numerical computing
import plotly.graph_objs as go  # Library for interactive plotting
import smtplib  # Library for sending email
from email.mime.text import MIMEText  # MIMEText to create email body
from email.mime.multipart import MIMEMultipart  # MIMEMultipart to create email message
from datetime import datetime  # Library for datetime operations
import time  # Library for time-related operations
from IPython.display import clear_output, display  # Library for clearing output and displaying output
import subprocess  # Library for subprocess management
import re  # Library for regular expressions
import pyautogui  # Library for GUI automation

from plotly.subplots import make_subplots

from sqlalchemy import create_engine, Table, MetaData  # Library for database connection

user_name = "postgres"
user_password = "root"
host = "localhost"
port = "5432"
database_name = "client_db_test_clone"

# PostgreSQL connection string
postgres_connection_string = f"postgresql://{user_name}:{user_password}@{host}:{port}/{database_name}"

# Create the connection to the database
engine = create_engine(postgres_connection_string)

# Function to send email messages

def send_email(message_text):
    """
    Function to send email messages.

    Parameters:
    - message_text: Text message to be sent via email.

    The function sets up email parameters, creates a MIME multipart message,
    and sends the email using the smtplib library to the specified recipient.
    """

    # Set up email parameters
    sender_email = "kaushal.cilans@gmail.com"  # Email address of the sender
    receiver_email = "kaushaljadav111@gmail.com"  # Email address of the receiver
    password = "fndkdayybiqfotsy"  # Password of the sender's email account

    subject = "EMA Crossover Alert"  # Subject of the email
    body = message_text  # Body of the email, contains the message text

    # Create message
    message = MIMEMultipart()  # Create a MIME multipart message
    message["From"] = sender_email  # Set the 'From' field of the email
    message["To"] = receiver_email  # Set the 'To' field of the email
    message["Subject"] = subject  # Set the subject of the email
    message.attach(MIMEText(body, "plain"))  # Attach the body of the email as plain text

    # Print Statement Before Sending Email
    print("Sending Email...")  # Print statement to indicate email sending process is initiated

    # Establish a connection with the SMTP server
    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:  # Connect to Gmail SMTP server
            server.starttls()  # Start TLS encryption
            server.login(sender_email, password)  # Login to the email account
            server.sendmail(sender_email, receiver_email, message.as_string())  # Send the email message
        print("Email Sent Successfully!")  # Print statement if email is sent successfully
    except Exception as e:
        print("Error Sending Email:", str(e))  # Print statement if an error occurs while sending the email


# Function to send WhatsApp messages to numbers listed in a CSV file

def send_whatsapp_messages(numbers_filename, message_text):
    """
    Function to send WhatsApp messages to numbers listed in a CSV file.

    Parameters:
    - numbers_filename: Path to the CSV file containing contact numbers.
    - message_text: Text message to be sent via WhatsApp.

    The function reads the contact numbers from the provided CSV file, validates each number,
    and sends the specified message via WhatsApp using subprocess and pyautogui libraries.
    """

    # Display the selected contact file path
    print("\n========================= Selected Contact File Path ===========================\n")
    print(numbers_filename)

    # Read the CSV file containing contact numbers into a DataFrame
    df1 = pd.read_csv(numbers_filename)
    print(df1)

    # Process the input text message
    input_txt = message_text

    # List to store rejected numbers
    rejected_number = []

    # Iterate through each row in the DataFrame
    for _, i in df1.iterrows():
        print(i['Number'])
        print(i['Name'])

        # Check if the number matches the valid pattern
        r = re.fullmatch('[6-9][0-9]{9}', str(i['Number']))
        if r is not None:
            print('Valid Number')

            raw_text = "Hello there, *{name}* !".format(name=i['Name'])
            text = raw_text.replace(" ", "%20")
            input_txt = input_txt.replace(' ', '%20')
            input_txt = input_txt.replace('\n', '%0A')
            input_txt = input_txt.replace(':)', '\U0001F601')

            print(input_txt)

            try:
                print('Opening WhatsApp...')
                subprocess.Popen(["cmd", "/C", "start whatsapp://"], shell=True)
                time.sleep(2)
                subprocess.Popen(["cmd", "/C", "start whatsapp://send?phone=" + str(i['Number'])], shell=True)
                time.sleep(2)
                subprocess.Popen(["cmd", "/C", "start whatsapp://send?phone=" + str(i['Number']) + "^&text=" + input_txt], shell=True)
                time.sleep(2)
                print('WhatsApp Opened')
                pyautogui.click(1000,1000)
                pyautogui.press('enter')  # Press Enter key to send message
                print('Message Sent')

            except Exception as e:
                print(e)

        else:
            print('Not a valid number')
            rejected_number.append(str(i['Number']))

    # Print the list of rejected numbers
    print(rejected_number)

    # Create a DataFrame for rejected numbers and store them in a CSV file
    rejected_num = pd.DataFrame()
    for i in rejected_number:
        print(i)
        rejected = df1.loc[df1['Number'] == i]
        rejected_num = pd.concat([rejected_num, rejected])
        print(rejected_num)

    rejected_num.to_csv('rejected_numbers.csv')


# Initialize figure
fig = go.Figure()

def initial_fetch_data(symbol, date):

    # if symbol == "^NSEI" or symbol == "^NSEBANK":
    #     symbol = symbol.upper()
    # else:
    #     symbol = f"{symbol}.NS".upper()

    date = datetime.strptime(date, '%Y-%m-%d')
    # temp_end = datetime.strptime("2024-01-31", '%Y-%m-%d')

    # Download 5-minute data
    min5 = yf.download(symbol, start=date, interval='5m')
    min5.index = min5.index.strftime('%Y-%m-%d %H:%M:%S') 
    min5.index = pd.to_datetime(min5.index)
    min5['Date'] = min5.index.date
    for i in range(len(min5.columns)):
        min5.columns.values[i] = "5m_" + min5.columns.values[i]
    min5.rename(columns={'5m_Date':'Date'}, inplace=True)
    min5 = min5.reindex(columns=['Date','5m_Open', '5m_High', '5m_Low', '5m_Close', '5m_Adj Close', '5m_Volume'])

    # Download 15-minute data
    min15 = yf.download(symbol, start=date, interval='15m')
    min15.index = min15.index.strftime('%Y-%m-%d %H:%M:%S')
    min15.index = pd.to_datetime(min15.index)
    min15['Date'] = min15.index.date
    for i in range(len(min15.columns)):
        min15.columns.values[i] = "15m_" + min15.columns.values[i]
    min15.rename(columns={'15m_Date':'Date'}, inplace=True)
    min15 = min15.reindex(columns=['Date','15m_Open', '15m_High', '15m_Low', '15m_Close', '15m_Adj Close', '15m_Volume'])
    min15 = min15.resample('5T').ffill()

    # Download 60-minute data
    min60 = yf.download(symbol, start=date, interval='60m')
    min60.index = min60.index.strftime('%Y-%m-%d %H:%M:%S')
    min60.index = pd.to_datetime(min60.index)
    min60['Date'] = min60.index.date
    for i in range(len(min60.columns)):
        min60.columns.values[i] = "60m_" + min60.columns.values[i]
    min60.rename(columns={'60m_Date':'Date'}, inplace=True)
    min60 = min60.reindex(columns=['Date','60m_Open', '60m_High', '60m_Low', '60m_Close', '60m_Adj Close', '60m_Volume'])
    min60 = min60.resample('5T').ffill()

    # Download 60-minute data
    day1 = yf.download(symbol, start=date, interval='1d')
    day1.index = day1.index.strftime('%Y-%m-%d %H:%M:%S')
    day1.index = pd.to_datetime(day1.index)
    day1['Date'] = day1.index.date
    for i in range(len(day1.columns)):
        day1.columns.values[i] = "1d_" + day1.columns.values[i]
    day1.rename(columns={'1d_Date':'Date'}, inplace=True)
    day1 = day1.reindex(columns=['Date','1d_Open', '1d_High', '1d_Low', '1d_Close', '1d_Adj Close', '1d_Volume'])

    # Extend the index to the end of the day
    last_timestamp = day1.index[-1]
    end_of_day = last_timestamp.replace(hour=23, minute=59, second=59)
    new_index = pd.date_range(start=day1.index[0], end=end_of_day, freq='T')
    day1 = day1.reindex(new_index)

    day1 = day1.resample('5T').ffill()
    day1.fillna(method='ffill', inplace=True)

    # Concatenate dataframes
    main_df = pd.concat([min5, min15, min60, day1], axis=1)
    # Drop duplicate columns
    main_df = main_df.loc[:, ~main_df.columns.duplicated()]
    # pd.set_option('display.max_rows', None)
    main_df = main_df.dropna(subset=['5m_Open','5m_High','5m_Low','5m_Close'])

    return main_df

##################### Plot Keltner channel on different time frames #####################

def keltner_channel_plot(data, symbol, kc_lookback=20, multiplier=2, atr_lookback=10):

    """
    Function to implement Keltner Channel trading strategy and visualize signals on a plot.

    Parameters:
    - data: DataFrame containing financial market data with 'Datetime', 'Close', 'High', and 'Low' columns.
    - fig: Plotly figure to update and display.
    - kc_lookback: Lookback period for Keltner Channel calculation (default is 20).
    - multiplier: Multiplier for ATR in Keltner Channel calculation (default is 2).
    - atr_lookback: Lookback period for ATR calculation (default is 10).

    The function calculates Keltner Channel upper, middle, and lower bands, implements a Keltner Channel trading strategy,
    and visualizes buy and sell signals on a Plotly plot.
    """

    message = ""

    # Keltner Channel Calculation 5 minutes data
    
    tr1_5m = pd.DataFrame(data['5m_High'] - data['5m_Low'])
    tr2_5m = pd.DataFrame(abs(data['5m_High'] - data['5m_Close'].shift()))
    tr3_5m = pd.DataFrame(abs(data['5m_Low'] - data['5m_Close'].shift()))
    frames_5m = [tr1_5m, tr2_5m, tr3_5m]
    tr_5m = pd.concat(frames_5m, axis=1, join='inner').max(axis=1)
    atr_5m = tr_5m.ewm(alpha=1 / atr_lookback).mean()
    kc_middle_5m = data['5m_Close'].ewm(kc_lookback).mean()
    kc_upper_5m = data['5m_Close'].ewm(kc_lookback).mean() + multiplier * atr_5m
    kc_lower_5m = data['5m_Close'].ewm(kc_lookback).mean() - multiplier * atr_5m

    # Keltner Channel Strategy Implementation
    buy_price_5m = []
    sell_price_5m = []
    kc_signal_5m = []
    signal_5m = 0
    mid_buy_price_5m = []
    mid_sell_price_5m = []
    mid_kc_signal_5m = []
    mid_signal = 0


    for i in range(len(data) - 1):

        if data['5m_Close'].iloc[i] > kc_middle_5m.iloc[i] and data['5m_Close'].iloc[i + 1] < data['5m_Close'].iloc[i]:
            if mid_signal != 1:
                mid_buy_price_5m.append(data['5m_Close'].iloc[i])
                mid_sell_price_5m.append(np.nan)
                mid_signal = 1
                mid_kc_signal_5m.append(mid_signal)
            else:
                mid_buy_price_5m.append(np.nan)
                mid_sell_price_5m.append(np.nan)
                mid_kc_signal_5m.append(0)

        elif data['5m_Close'].iloc[i] < kc_middle_5m.iloc[i] and data['5m_Close'].iloc[i + 1] > data['5m_Close'].iloc[i]:
            if mid_signal != -1:
                mid_buy_price_5m.append(np.nan)
                mid_sell_price_5m.append(data['5m_Close'].iloc[i])
                mid_signal = -1
                mid_kc_signal_5m.append(mid_signal)
            else:
                mid_buy_price_5m.append(np.nan)
                mid_sell_price_5m.append(np.nan)
                mid_kc_signal_5m.append(0)

        else:
            mid_buy_price_5m.append(np.nan)
            mid_sell_price_5m.append(np.nan)
            mid_kc_signal_5m.append(0)

        if data['5m_Close'].iloc[i] < kc_lower_5m.iloc[i] and data['5m_Close'].iloc[i + 1] > data['5m_Close'].iloc[i]:
            if signal_5m != 1:
                buy_price_5m.append(data['5m_Close'].iloc[i])
                sell_price_5m.append(np.nan)
                signal_5m = 1
                kc_signal_5m.append(signal_5m)
            else:
                buy_price_5m.append(np.nan)
                sell_price_5m.append(np.nan)
                kc_signal_5m.append(0)
        elif data['5m_Close'].iloc[i] > kc_upper_5m.iloc[i] and data['5m_Close'].iloc[i + 1] < data['5m_Close'].iloc[i]:
            if signal_5m != -1:
                buy_price_5m.append(np.nan)
                sell_price_5m.append(data['5m_Close'].iloc[i])
                signal_5m = -1
                kc_signal_5m.append(signal_5m)
            else:
                buy_price_5m.append(np.nan)
                sell_price_5m.append(np.nan)
                kc_signal_5m.append(0)
        else:
            buy_price_5m.append(np.nan)
            sell_price_5m.append(np.nan)
            kc_signal_5m.append(0)


    # Keltner Channel Calculation 15 minutes data

    tr1_15m = pd.DataFrame(data['15m_High'] - data['15m_Low'])
    tr2_15m = pd.DataFrame(abs(data['15m_High'] - data['15m_Close'].shift()))
    tr3_15m = pd.DataFrame(abs(data['15m_Low'] - data['15m_Close'].shift()))
    frames_15m = [tr1_15m, tr2_15m, tr3_15m]
    tr_15m = pd.concat(frames_15m, axis=1, join='inner').max(axis=1)
    atr_15m = tr_15m.ewm(alpha=1 / atr_lookback).mean()
    kc_middle_15m = data['15m_Close'].ewm(kc_lookback).mean()
    kc_upper_15m = data['15m_Close'].ewm(kc_lookback).mean() + multiplier * atr_15m
    kc_lower_15m = data['15m_Close'].ewm(kc_lookback).mean() - multiplier * atr_15m

    # Keltner Channel Strategy Implementation
    buy_price_15m = []
    sell_price_15m = []
    kc_signal_15m = []
    signal_15m = 0
    mid_buy_price_15m = []
    mid_sell_price_15m = []
    mid_kc_signal_15m = []
    mid_signal = 0

    for i in range(len(data) - 1):

        if data['15m_Close'].iloc[i] > kc_middle_15m.iloc[i] and data['15m_Close'].iloc[i + 1] < data['15m_Close'].iloc[i]:
            if mid_signal != 1:
                mid_buy_price_15m.append(data['15m_Close'].iloc[i])
                mid_sell_price_15m.append(np.nan)
                mid_signal = 1
                mid_kc_signal_15m.append(mid_signal)
            else:
                mid_buy_price_15m.append(np.nan)
                mid_sell_price_15m.append(np.nan)
                mid_kc_signal_15m.append(0)

        elif data['15m_Close'].iloc[i] < kc_middle_15m.iloc[i] and data['15m_Close'].iloc[i + 1] > data['15m_Close'].iloc[i]:
            if mid_signal != -1:
                mid_buy_price_15m.append(np.nan)
                mid_sell_price_15m.append(data['15m_Close'].iloc[i])
                mid_signal = -1
                mid_kc_signal_15m.append(mid_signal)
            else:
                mid_buy_price_15m.append(np.nan)
                mid_sell_price_15m.append(np.nan)
                mid_kc_signal_15m.append(0)

        else:
            mid_buy_price_15m.append(np.nan)
            mid_sell_price_15m.append(np.nan)
            mid_kc_signal_15m.append(0)

        if data['15m_Close'].iloc[i] < kc_lower_15m.iloc[i] and data['15m_Close'].iloc[i + 1] > data['15m_Close'].iloc[i]:
            if signal_15m != 1:
                buy_price_15m.append(data['15m_Close'].iloc[i])
                sell_price_15m.append(np.nan)
                signal_15m = 1
                kc_signal_15m.append(signal_15m)
            else:
                buy_price_15m.append(np.nan)
                sell_price_15m.append(np.nan)
                kc_signal_15m.append(0)
        elif data['15m_Close'].iloc[i] > kc_upper_15m.iloc[i] and data['15m_Close'].iloc[i + 1] < data['15m_Close'].iloc[i]:
            if signal_15m != -1:
                buy_price_15m.append(np.nan)
                sell_price_15m.append(data['15m_Close'].iloc[i])
                signal_15m = -1
                kc_signal_15m.append(signal_15m)
            else:
                buy_price_15m.append(np.nan)
                sell_price_15m.append(np.nan)
                kc_signal_15m.append(0)
        else:
            buy_price_15m.append(np.nan)
            sell_price_15m.append(np.nan)
            kc_signal_15m.append(0)

    # Keltner Channel Calculation 60 minutes data

    tr1_60m = pd.DataFrame(data['60m_High'] - data['60m_Low'])
    tr2_60m = pd.DataFrame(abs(data['60m_High'] - data['60m_Close'].shift()))
    tr3_60m = pd.DataFrame(abs(data['60m_Low'] - data['60m_Close'].shift()))
    frames_60m = [tr1_60m, tr2_60m, tr3_60m]
    tr_60m = pd.concat(frames_60m, axis=1, join='inner').max(axis=1)
    atr_60m = tr_60m.ewm(alpha=1 / atr_lookback).mean()
    kc_middle_60m = data['60m_Close'].ewm(kc_lookback).mean()
    kc_upper_60m = data['60m_Close'].ewm(kc_lookback).mean() + multiplier * atr_60m
    kc_lower_60m = data['60m_Close'].ewm(kc_lookback).mean() - multiplier * atr_60m

    # Keltner Channel Strategy Implementation
    buy_price_60m = []
    sell_price_60m = []
    kc_signal_60m = []
    signal_60m = 0
    mid_buy_price_60m = []
    mid_sell_price_60m = []
    mid_kc_signal_60m = []
    mid_signal = 0


    for i in range(len(data) - 1):

        if data['60m_Close'].iloc[i] > kc_middle_60m.iloc[i] and data['60m_Close'].iloc[i + 1] < data['60m_Close'].iloc[i]:
            if mid_signal != 1:
                mid_buy_price_60m.append(data['60m_Close'].iloc[i])
                mid_sell_price_60m.append(np.nan)
                mid_signal = 1
                mid_kc_signal_60m.append(mid_signal)
            else:
                mid_buy_price_60m.append(np.nan)
                mid_sell_price_60m.append(np.nan)
                mid_kc_signal_60m.append(0)

        elif data['60m_Close'].iloc[i] < kc_middle_60m.iloc[i] and data['60m_Close'].iloc[i + 1] > data['60m_Close'].iloc[i]:
            if mid_signal != -1:
                mid_buy_price_60m.append(np.nan)
                mid_sell_price_60m.append(data['60m_Close'].iloc[i])
                mid_signal = -1
                mid_kc_signal_60m.append(mid_signal)
            else:
                mid_buy_price_60m.append(np.nan)
                mid_sell_price_60m.append(np.nan)
                mid_kc_signal_60m.append(0)

        else:
            mid_buy_price_60m.append(np.nan)
            mid_sell_price_60m.append(np.nan)
            mid_kc_signal_60m.append(0)

        if data['60m_Close'].iloc[i] < kc_lower_60m.iloc[i] and data['60m_Close'].iloc[i + 1] > data['60m_Close'].iloc[i]:
            if signal_60m != 1:
                buy_price_60m.append(data['60m_Close'].iloc[i])
                sell_price_60m.append(np.nan)
                signal_60m = 1
                kc_signal_60m.append(signal_60m)
            else:
                buy_price_60m.append(np.nan)
                sell_price_60m.append(np.nan)
                kc_signal_60m.append(0)
        elif data['60m_Close'].iloc[i] > kc_upper_60m.iloc[i] and data['60m_Close'].iloc[i + 1] < data['60m_Close'].iloc[i]:
            if signal_60m != -1:
                buy_price_60m.append(np.nan)
                sell_price_60m.append(data['60m_Close'].iloc[i])
                signal_60m = -1
                kc_signal_60m.append(signal_60m)
            else:
                buy_price_60m.append(np.nan)
                sell_price_60m.append(np.nan)
                kc_signal_60m.append(0)
        else:
            buy_price_60m.append(np.nan)
            sell_price_60m.append(np.nan)
            kc_signal_60m.append(0)






    # Keltner Channel Calculation 60 minutes data

    tr1_1d = pd.DataFrame(data['1d_High'] - data['1d_Low'])
    tr2_1d = pd.DataFrame(abs(data['1d_High'] - data['1d_Close'].shift()))
    tr3_1d = pd.DataFrame(abs(data['1d_Low'] - data['1d_Close'].shift()))
    frames_1d = [tr1_1d, tr2_1d, tr3_1d]
    tr_1d = pd.concat(frames_1d, axis=1, join='inner').max(axis=1)
    atr_1d = tr_1d.ewm(alpha=1 / atr_lookback).mean()
    kc_middle_1d = data['1d_Close'].ewm(kc_lookback).mean()
    kc_upper_1d = data['1d_Close'].ewm(kc_lookback).mean() + multiplier * atr_1d
    kc_lower_1d = data['1d_Close'].ewm(kc_lookback).mean() - multiplier * atr_1d

    # Keltner Channel Strategy Implementation
    buy_price_1d = []
    sell_price_1d = []
    kc_signal_1d = []
    signal_1d = 0
    mid_buy_price_1d = []
    mid_sell_price_1d = []
    mid_kc_signal_1d = []
    mid_signal = 0


    for i in range(len(data) - 1):

        if data['1d_Close'].iloc[i] > kc_middle_1d.iloc[i] and data['1d_Close'].iloc[i + 1] < data['1d_Close'].iloc[i]:
            if mid_signal != 1:
                mid_buy_price_1d.append(data['1d_Close'].iloc[i])
                mid_sell_price_1d.append(np.nan)
                mid_signal = 1
                mid_kc_signal_1d.append(mid_signal)
            else:
                mid_buy_price_1d.append(np.nan)
                mid_sell_price_1d.append(np.nan)
                mid_kc_signal_1d.append(0)

        elif data['1d_Close'].iloc[i] < kc_middle_1d.iloc[i] and data['1d_Close'].iloc[i + 1] > data['1d_Close'].iloc[i]:
            if mid_signal != -1:
                mid_buy_price_1d.append(np.nan)
                mid_sell_price_1d.append(data['1d_Close'].iloc[i])
                mid_signal = -1
                mid_kc_signal_1d.append(mid_signal)
            else:
                mid_buy_price_1d.append(np.nan)
                mid_sell_price_1d.append(np.nan)
                mid_kc_signal_1d.append(0)

        else:
            mid_buy_price_1d.append(np.nan)
            mid_sell_price_1d.append(np.nan)
            mid_kc_signal_1d.append(0)

        if data['1d_Close'].iloc[i] < kc_lower_1d.iloc[i] and data['1d_Close'].iloc[i + 1] > data['1d_Close'].iloc[i]:
            if signal_1d != 1:
                buy_price_1d.append(data['1d_Close'].iloc[i])
                sell_price_1d.append(np.nan)
                signal_1d = 1
                kc_signal_1d.append(signal_1d)
            else:
                buy_price_1d.append(np.nan)
                sell_price_1d.append(np.nan)
                kc_signal_1d.append(0)
        elif data['1d_Close'].iloc[i] > kc_upper_1d.iloc[i] and data['1d_Close'].iloc[i + 1] < data['1d_Close'].iloc[i]:
            if signal_1d != -1:
                buy_price_1d.append(np.nan)
                sell_price_1d.append(data['1d_Close'].iloc[i])
                signal_1d = -1
                kc_signal_1d.append(signal_1d)
            else:
                buy_price_1d.append(np.nan)
                sell_price_1d.append(np.nan)
                kc_signal_1d.append(0)
        else:
            buy_price_1d.append(np.nan)
            sell_price_1d.append(np.nan)
            kc_signal_1d.append(0)

# # Check if a new buy or sell signal is generated in 5min timeframe

#     if (mid_kc_signal_5m[-1] == -1) & (mid_kc_signal_5m[-2] == 1):
#         message = f"Sell Signal:\n {symbol} \nPrice crossed above Middle Keltner Channel in 5min timeframe"
#         # send_whatsapp_messages("leads.csv", message)
#         # send_email(message_text=message)

#     elif (kc_signal_5m[-1] == -1) & (kc_signal_5m[-2] == 1):
#         message = f"Sell Signal:\n {symbol} \nPrice crossed above Upper Keltner Channel in 5min timeframe"
#         # send_whatsapp_messages("leads.csv", message)  # Adjust the filename accordingly
#         # send_email(message_text=message)  # Send a detailed email
    
#     if (mid_kc_signal_5m[-1] == 1) & (mid_kc_signal_5m[-2] == -1):
#         message = f"Buy Signal: \n{symbol} \n Price crossed below Middle Keltner Channel in 5min timeframe"
#         # send_whatsapp_messages("leads.csv", message)
#         # send_email(message_text=message)

#     elif (kc_signal_5m[-1] == 1) & (kc_signal_5m[-2] == -1):
#         message = f"Buy Signal: \n{symbol} \n Price crossed below Lower Keltner Channel in 5min timeframe"
#         # send_whatsapp_messages("leads.csv", message)  # Adjust the filename accordingly
#         # send_email(message_text=message)  # Send a detailed email

#     else:
#         print("No Signal in 5min timeframe above Middle Keltner Channel or below Middle Keltner Channel")
  
    # Check if a new buy or sell signal is generated in 1day timeframe
        
    if (mid_kc_signal_1d[-1] == 1) & (mid_kc_signal_1d[-2] == -1) & (mid_kc_signal_60m[-1] == 1) & (mid_kc_signal_15m[-1] == 1) & (mid_kc_signal_5m[-1] == 1):
        message = f"Buy Signal: \n{symbol} \n Price crossed below Middle Keltner Channel in 1day timeframe and also followed by 60min, 15min and 5min timeframes as well"
        # send_whatsapp_messages("leads.csv", message)
        # send_email(message_text=message)

    elif (mid_kc_signal_1d[-1] == 1) & (mid_kc_signal_1d[-2] == -1) & (mid_kc_signal_60m[-1] == 1) & (mid_kc_signal_15m[-1] == 1):
        message = f"Buy Signal: \n{symbol} \n Price crossed below Middle Keltner Channel in 1day timeframe and also followed by 60min and 15min timeframe as well"
        # send_whatsapp_messages("leads.csv", message)
        # send_email(message_text=message)

    elif (mid_kc_signal_1d[-1] == 1) & (mid_kc_signal_1d[-2] == -1) & (mid_kc_signal_60m[-1] == 1):
        message = f"Buy Signal: \n{symbol} \n Price crossed below Middle Keltner Channel in 1day timeframe and also followed by 60min timeframe as well"
        # send_whatsapp_messages("leads.csv", message)
        # send_email(message_text=message)

    elif (mid_kc_signal_1d[-1] == 1) & (mid_kc_signal_1d[-2] == -1):
        message = f"Buy Signal: \n{symbol} \n Price crossed below Middle Keltner Channel in 1day timeframe"
        # send_whatsapp_messages("leads.csv", message)
        # send_email(message_text=message)

    elif (mid_kc_signal_1d[-1] == -1) & (mid_kc_signal_1d[-2] == 1) & (mid_kc_signal_60m[-1] == -1) & (mid_kc_signal_15m[-1] == -1) & (mid_kc_signal_5m[-1] == -1):
        message = f"Sell Signal:\n {symbol} \nPrice crossed above Middle Keltner Channel in 1day timeframe and also followed by 60min, 15min and 5min timeframes as well"
        # send_whatsapp_messages("leads.csv", message)
        # send_email(message_text=message)

    elif (mid_kc_signal_1d[-1] == -1) & (mid_kc_signal_1d[-2] == 1) & (mid_kc_signal_60m[-1] == -1) & (mid_kc_signal_15m[-1] == -1):
        message = f"Sell Signal:\n {symbol} \nPrice crossed above Middle Keltner Channel in 1day timeframe and also followed by 60min and 15min timeframe as well"
        # send_whatsapp_messages("leads.csv", message)
        # send_email(message_text=message)

    elif (mid_kc_signal_1d[-1] == -1) & (mid_kc_signal_1d[-2] == 1) & (mid_kc_signal_60m[-1] == -1):
        message = f"Sell Signal:\n {symbol} \nPrice crossed above Middle Keltner Channel in 1day timeframe and also followed by 60min timeframe as well"
        # send_whatsapp_messages("leads.csv", message)
        # send_email(message_text=message)

    elif (mid_kc_signal_1d[-1] == -1) & (mid_kc_signal_1d[-2] == 1):
        message = f"Sell Signal:\n {symbol} \nPrice crossed above Middle Keltner Channel in 1day timeframe"
        # send_whatsapp_messages("leads.csv", message)
        # send_email(message_text=message)

    else:
        print("No Signal in 1day timeframe above Middle Keltner Channel or below Middle Keltner Channel")
    
    if (kc_signal_1d[-1] == 1) & (kc_signal_1d[-2] == -1) & (kc_signal_60m[-1] == 1) & (kc_signal_15m[-1] == 1) & (kc_signal_5m[-1] == 1):
        message = f"Buy Signal: \n{symbol} \n Price crossed below Lower Keltner Channel in 1day timeframe and also followed by 60min, 15min and 5min timeframes as well"
        # send_whatsapp_messages("leads.csv", message)
        # send_email(message_text=message)

    elif (kc_signal_1d[-1] == 1) & (kc_signal_1d[-2] == -1) & (kc_signal_60m[-1] == 1) & (kc_signal_15m[-1] == 1):
        message = f"Buy Signal: \n{symbol} \n Price crossed below Lower Keltner Channel in 1day timeframe and also followed by 60min and 15min timeframe as well"
        # send_whatsapp_messages("leads.csv", message)
        # send_email(message_text=message)

    elif (kc_signal_1d[-1] == 1) & (kc_signal_1d[-2] == -1) & (kc_signal_60m[-1] == 1):
        message = f"Buy Signal: \n{symbol} \n Price crossed below Lower Keltner Channel in 1day timeframe and also followed by 60min timeframe as well"
        # send_whatsapp_messages("leads.csv", message)
        # send_email(message_text=message)

    elif (kc_signal_1d[-1] == 1) & (kc_signal_1d[-2] == -1):
        message = f"Buy Signal: \n{symbol} \n Price crossed below Lower Keltner Channel in 1day timeframe"
        # send_whatsapp_messages("leads.csv", message)
        # send_email(message_text=message)

    elif (kc_signal_1d[-1] == -1) & (kc_signal_1d[-2] == 1) & (kc_signal_60m[-1] == -1) & (kc_signal_15m[-1] == -1) & (kc_signal_5m[-1] == -1):
        message = f"Sell Signal:\n {symbol} \nPrice crossed above Upper Keltner Channel in 1day timeframe and also followed by 60min, 15min and 5min timeframes as well"
        # send_whatsapp_messages("leads.csv", message)
        # send_email(message_text=message)

    elif (kc_signal_1d[-1] == -1) & (kc_signal_1d[-2] == 1) & (kc_signal_60m[-1] == -1) & (kc_signal_15m[-1] == -1):
        message = f"Sell Signal:\n {symbol} \nPrice crossed above Upper Keltner Channel in 1day timeframe and also followed by 60min and 15min timeframe as well"
        # send_whatsapp_messages("leads.csv", message)
        # send_email(message_text=message)

    elif (kc_signal_1d[-1] == -1) & (kc_signal_1d[-2] == 1) & (kc_signal_60m[-1] == -1):
        message = f"Sell Signal:\n {symbol} \nPrice crossed above Upper Keltner Channel in 1day timeframe and also followed by 60min timeframe as well"
        # send_whatsapp_messages("leads.csv", message)
        # send_email(message_text=message)
    
    elif (kc_signal_1d[-1] == -1) & (kc_signal_1d[-2] == 1):
        message = f"Sell Signal:\n {symbol} \nPrice crossed above Upper Keltner Channel in 1day timeframe"
        # send_whatsapp_messages("leads.csv", message)
        # send_email(message_text=message)

    else:
        print("No Signal in 1day timeframe above Upper Keltner Channel or below Lower Keltner Channel")


    
    # Check if a new buy or sell signal is generated in 60min timeframe
        
    if (mid_kc_signal_60m[-1] == 1) & (mid_kc_signal_60m[-2] == -1) & (mid_kc_signal_15m[-1] == 1) & (mid_kc_signal_5m[-1] == 1):
        message = f"Buy Signal: \n{symbol} \n Price crossed below Middle Keltner Channel in 60min timeframe and also followed by 15min and 5min timeframes as well"
        # send_whatsapp_messages("leads.csv", message)
        # send_email(message_text=message)

    elif (mid_kc_signal_60m[-1] == 1) & (mid_kc_signal_60m[-2] == -1) & (mid_kc_signal_15m[-1] == 1):
        message = f"Buy Signal: \n{symbol} \n Price crossed below Middle Keltner Channel in 60min timeframe and also followed by 15min timeframe as well"
        # send_whatsapp_messages("leads.csv", message)
        # send_email(message_text=message)

    elif (mid_kc_signal_60m[-1] == 1) & (mid_kc_signal_60m[-2] == -1):
        message = f"Buy Signal: \n{symbol} \n Price crossed below Middle Keltner Channel in 60min timeframe"
        # send_whatsapp_messages("leads.csv", message)
        # send_email(message_text=message)

    elif (mid_kc_signal_60m[-1] == -1) & (mid_kc_signal_60m[-2] == 1) & (mid_kc_signal_15m[-1] == -1) & (mid_kc_signal_5m[-1] == -1):
        message = f"Sell Signal:\n {symbol} \nPrice crossed above Middle Keltner Channel in 60min timeframe and also followed by 15min and 5min timeframes as well"
        # send_whatsapp_messages("leads.csv", message)
        # send_email(message_text=message)

    elif (mid_kc_signal_60m[-1] == -1) & (mid_kc_signal_60m[-2] == 1) & (mid_kc_signal_15m[-1] == -1):
        message = f"Sell Signal:\n {symbol} \nPrice crossed above Middle Keltner Channel in 60min timeframe and also followed by 15min timeframe as well"
        # send_whatsapp_messages("leads.csv", message)
        # send_email(message_text=message)

    elif (mid_kc_signal_60m[-1] == -1) & (mid_kc_signal_60m[-2] == 1):
        message = f"Sell Signal:\n {symbol} \nPrice crossed above Middle Keltner Channel in 60min timeframe"
        # send_whatsapp_messages("leads.csv", message)
        # send_email(message_text=message)

    else:
        print("No Signal in 60min timeframe above Middle Keltner Channel or below Middle Keltner Channel")
    
    if (kc_signal_60m[-1] == 1) & (kc_signal_60m[-2] == -1) & (kc_signal_15m[-1] == 1) & (kc_signal_5m[-1] == 1):
        message = f"Buy Signal: \n{symbol} \n Price crossed below Lower Keltner Channel in 60min timeframe and also followed by 15min and 5min timeframes as well"
        # send_whatsapp_messages("leads.csv", message)
        # send_email(message_text=message)

    elif (kc_signal_60m[-1] == 1) & (kc_signal_60m[-2] == -1) & (kc_signal_15m[-1] == 1):
        message = f"Buy Signal: \n{symbol} \n Price crossed below Lower Keltner Channel in 60min timeframe and also followed by 15min timeframe as well"
        # send_whatsapp_messages("leads.csv", message)
        # send_email(message_text=message)

    elif (kc_signal_60m[-1] == 1) & (kc_signal_60m[-2] == -1):
        message = f"Buy Signal: \n{symbol} \n Price crossed below Lower Keltner Channel in 60min timeframe"
        # send_whatsapp_messages("leads.csv", message)
        # send_email(message_text=message)

    elif (kc_signal_60m[-1] == -1) & (kc_signal_60m[-2] == 1) & (kc_signal_15m[-1] == -1) & (kc_signal_5m[-1] == -1):
        message = f"Sell Signal:\n {symbol} \nPrice crossed above Upper Keltner Channel in 60min timeframe and also followed by 15min and 5min timeframes as well"
        # send_whatsapp_messages("leads.csv", message)
        # send_email(message_text=message)

    elif (kc_signal_60m[-1] == -1) & (kc_signal_60m[-2] == 1) & (kc_signal_15m[-1] == -1):
        message = f"Sell Signal:\n {symbol} \nPrice crossed above Upper Keltner Channel in 60min timeframe and also followed by 15min timeframe as well"
        # send_whatsapp_messages("leads.csv", message)
        # send_email(message_text=message)

    elif (kc_signal_60m[-1] == -1) & (kc_signal_60m[-2] == 1):
        message = f"Sell Signal:\n {symbol} \nPrice crossed above Upper Keltner Channel in 60min timeframe"
        # send_whatsapp_messages("leads.csv", message)
        # send_email(message_text=message)

    else:
        print("No Signal in 60min timeframe above Upper Keltner Channel or below Lower Keltner Channel")



    # Check if a new buy or sell signal is generated in 15min timeframe

    if (mid_kc_signal_15m[-1] == 1) & (mid_kc_signal_15m[-2] == -1) & (mid_kc_signal_5m[-1] == 1):
        message = f"Buy Signal: \n{symbol} \n Price crossed below Middle Keltner Channel in 15min timeframe and also followed by 5min timeframe as well"
        # send_whatsapp_messages("leads.csv", message)
        # send_email(message_text=message)
    
    elif (mid_kc_signal_15m[-1] == 1) & (mid_kc_signal_15m[-2] == -1):
        message = f"Buy Signal: \n{symbol} \n Price crossed below Middle Keltner Channel in 15min timeframe"
        # send_whatsapp_messages("leads.csv", message)
        # send_email(message_text=message)
    
    elif (mid_kc_signal_15m[-1] == -1) & (mid_kc_signal_15m[-2] == 1) & (mid_kc_signal_5m[-1] == -1):
        message = f"Sell Signal:\n {symbol} \nPrice crossed above Middle Keltner Channel in 15min timeframe and also followed by 5min timeframe as well"
        # send_whatsapp_messages("leads.csv", message)
        # send_email(message_text=message)

    elif (mid_kc_signal_15m[-1] == -1) & (mid_kc_signal_15m[-2] == 1):
        message = f"Sell Signal:\n {symbol} \nPrice crossed above Middle Keltner Channel in 15min timeframe"
        # send_whatsapp_messages("leads.csv", message)
        # send_email(message_text=message)

    else:
        print("No Signal in 15min timeframe above Middle Keltner Channel or below Middle Keltner Channel")
    
    if (kc_signal_15m[-1] == 1) & (kc_signal_15m[-2] == -1) & (kc_signal_5m[-1] == 1):
        message = f"Buy Signal: \n{symbol} \n Price crossed below Lower Keltner Channel in 15min timeframe and also followed by 5min timeframe as well"
        # send_whatsapp_messages("leads.csv", message)
        # send_email(message_text=message)

    elif (kc_signal_15m[-1] == 1) & (kc_signal_15m[-2] == -1):
        message = f"Buy Signal: \n{symbol} \n Price crossed below Lower Keltner Channel in 15min timeframe"
        # send_whatsapp_messages("leads.csv", message)
        # send_email(message_text=message)

    elif (kc_signal_15m[-1] == -1) & (kc_signal_15m[-2] == 1) & (kc_signal_5m[-1] == -1):
        message = f"Sell Signal:\n {symbol} \nPrice crossed above Upper Keltner Channel in 15min timeframe and also followed by 5min timeframe as well"
        # send_whatsapp_messages("leads.csv", message)
        # send_email(message_text=message)

    elif (kc_signal_15m[-1] == -1) & (kc_signal_15m[-2] == 1):
        message = f"Sell Signal:\n {symbol} \nPrice crossed above Upper Keltner Channel in 15min timeframe"
        # send_whatsapp_messages("leads.csv", message)
        # send_email(message_text=message)

    else:
        print("No Signal in 15min timeframe above Upper Keltner Channel or below Lower Keltner Channel")
     
    # # Display the figure
    # clear_output(wait=True)
    # display(fig)

    return message   #, fig

def get_previous_working_day(df):
    # Fetch the latest date from the database
    latest_date = df['Date'].iloc[-1]

    # Subtract one day
    previous_date = latest_date - pd.Timedelta(days=1)

    # Check if the previous date is a working day (Monday to Friday)
    while previous_date.weekday() >= 4:  # Saturday or Sunday
        previous_date -= pd.Timedelta(days=1)

    # Format the date as YYYY-MM-DD
    date_to_fetch = str(previous_date.strftime('%Y-%m-%d'))

    return date_to_fetch

# Function to track time taken
def track_time(stock_symbol, start_time):
    end_time = time.time()
    total_time = end_time - start_time
    return {'Stock': stock_symbol, 'Start Time': start_time, 'End Time': end_time, 'Total Time': total_time}

# Initialize empty list to store time tracking data
# time_tracking_data = []

# # Define an infinite loop to continuously fetch minute-level stock data and plot EMAs
# while True:

# stocks_unsorted = ['acc', 'acl', 'adanient', 'adaniports', 'adanipower', 'alkem', '^NSEI', 'axisbank', 'bajaj-auto', 'bajajfinsv', 'bajfinance', 'biocon', 'boschltd', 'bpcl', 'britannia', 'canbk', 'cgpower', 'cholafin', 'cipla', 'coalindia', 'coforge', 'colpal', 'ltim', 'ltts', 'lupin', 'm&m', 'prestige', 'sbicard', 'sbilife', 'sbin', 'shreecem', 'muthootfin', 'titan', 'torntpharm', 'abb', 'industower', 'voltas', 'bhartiartl', 'bhel', 'pnb', 'policybzr', 'abfrl', 'polycab', 'shriramfin', 'siemens', 'sonacoms', 'srf', 'sunpharma', 'suntv', 'syngene', 'tatachem', 'tatacomm', 'tataconsum', 'tataelxsi', 'tatamotors', 'tatamtrdvr', 'itc', 'jindalstel', 'ramcocem', 'recltd', 'infy', 'ioc', 'tatapower', 'tatasteel', 'ipcalab', 'irctc', '^NSEBANK', 'YESBANK', 'ZYDUSLIFE', 'balkrisind', 'bankbaroda', 'bataindia', 'bdl', 'bel', 'bergepaint', 'bharatforg', 'concor', 'coromandel', 'crompton', 'cumminsind', 'reliance', 'sail', 'dalbharat', 'deepakntr', 'msumi', 'powergrid', 'epigral', 'escorts', 'fact', 'fluorochem', 'gail', 'gland', 'delhivery', 'divislab', 'dixon', 'dlf', 'drreddy', 'eichermot', 'm&mfin', 'mankind', 'marico', 'maruti', 'maxhealth', 'mazdock', 'mcdowell-n', 'mfsl', 'motherson', 'jswenergy', 'jswsteel', 'jublfood', 'kotakbank', 'ambujacem', 'aplapollo', 'naukri', 'mphasis', 'godrejprop', 'dabur', 'grasim', 'gujgasltd', 'hal', 'havells', 'kpittech', 'l&tfh', 'latentview', 'lauruslabs', 'lichsgfin', 'lici', 'hcltech', 'hdfcamc', 'hdfcbank', 'hdfclife', 'irfc', 'ubl', 'ultracemco', 'unionbank', 'upl', 'vbl', 'vedl', 'godrejcp', 'wipro', 'pel', 'trent', 'tvsmotor', 'aartiind', 'persistent', 'petronet', 'pfc', 'pghh', 'pidilitind', 'piind', 'tcs', 'techm', 'tiindia', 'navinfluor', 'nestleind', 'nhpc', 'nmdc', 'ntpc', 'nykaa', 'oberoirlty', 'oil', 'ongc', 'pageind', 'patanjali', 'heromotoco', 'hindalco', 'hindpetro', 'hindunilvr', 'icicibank', 'icicigi', 'icicipruli', 'idfcfirstb', 'igl', 'indhotel', 'indigo', 'indusindbk', 'apollohosp', 'apollotyre', 'asianpaint', 'lodha', 'lt', 'astral', 'aubank', 'auropharma', 'abcapital']
# Reflect the database tables
metadata = MetaData()
metadata.reflect(bind=engine)

stocks_unsorted = metadata.tables.keys()

stocks = sorted(stocks_unsorted)[::3]
print(f'\n{stocks}\n')

while True:

    time_tracking_data = []

    combine_msgs = []

    for stock_symbol in stocks:

        start_time = time.time()

        print(stock_symbol)

        sql_df = pd.read_sql_table(stock_symbol, engine)

        date_to_fetch_from = get_previous_working_day(sql_df)

        sql_df['Date'] = sql_df['Date'].astype(str)
        
        # find the list of index number which hase the value in the date column as the date_to_fetch
        index_list = sql_df.index[sql_df['Date'] == date_to_fetch_from].tolist()

        for idx in range(index_list[-1] + 1, sql_df.shape[0]):
            sql_df.drop(idx, inplace=True)

        next_date = sql_df['Date'].iloc[-1]
        next_date = pd.to_datetime(next_date)
        next_date = next_date + pd.Timedelta(days=1)
        next_date = str(next_date.strftime('%Y-%m-%d'))

        if stock_symbol == "^NSEI" or stock_symbol == "^NSEBANK":
            latest_df = initial_fetch_data(stock_symbol.upper(), next_date)
        else:
            latest_df = initial_fetch_data(f"{stock_symbol.upper()}.NS", next_date)

        latest_df.reset_index(inplace=True)

        final_df = pd.concat([sql_df, latest_df], axis=0)
        final_df.reset_index(drop=True, inplace=True)

        final_df.drop_duplicates(subset=['5m_Open','5m_High','5m_Low','5m_Close','5m_Volume'], keep='last', inplace=True)
        final_df = final_df.dropna(subset=['5m_Open','5m_High','5m_Low','5m_Close'])
        final_df.fillna(method='ffill', inplace=True)
        # print(final_df.tail(10))

        # print(final_df.tail(10))
        # final_df.to_csv(f'{stock_symbol}_data.csv')

        msg = keltner_channel_plot(data=final_df, kc_lookback=10, multiplier=1, atr_lookback=10, symbol= stock_symbol.upper())

        print(msg)

        if msg :
            combine_msgs.append(msg)

        # Track time for this stock
        time_tracking_data.append(track_time(stock_symbol, start_time))

        time.sleep(1)

    update = [word.replace('&', 'and') for word in combine_msgs]

    formatted_alerts = [alert + ' ' for alert in update]

    combine_msg = '\n\n'.join(formatted_alerts)

    send_whatsapp_messages("leads.csv", combine_msg)

    # Convert time tracking data to DataFrame
    time_df = pd.DataFrame(time_tracking_data)

    # Get the current time
    current_time = time.time()

    # Convert the current time to a datetime object
    datetime_object = datetime.fromtimestamp(current_time)

    # Get the latest time with date
    latest_time_with_date = datetime_object.strftime("%Y_%m_%d_%H_%M_%S")

    # Save time tracking data to CSV 
    time_df.to_csv(f'KELTNER_stock_time_sheet{latest_time_with_date}.csv')

    # Sleep for 1 minute before running the loop again
    # time.sleep(900)

# Sleep for 1 minute before running the loop again                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              to avoid overwhelming the system and the API
# time.sleep(20)
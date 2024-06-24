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
user_password = "manager"
host = "localhost"
port = "5432"
database_name = "stock_market_db"

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
            input_txt = raw_text.replace(" ", "%20")
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

    # Download day1 data
    day1 = yf.download(symbol, start=date, interval='1d')
    day1.index = pd.to_datetime(day1.index)
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

    data['5m_TR'] = np.maximum(data['5m_High'] - data['5m_Low'], np.maximum(abs(data['5m_High'] - data['5m_Close'].shift(1)), abs(data['5m_Low'] - data['5m_Close'].shift())))
    data['5m_ATR'] = data['5m_TR'].rolling(window=atr_lookback).mean()
    data['kc_middle_5m'] = data['5m_Close'].ewm(span=kc_lookback, adjust=False).mean()
    data['kc_upper_5m'] = data['kc_middle_5m'] + data['5m_ATR'] * multiplier
    data['kc_lower_5m'] = data['kc_middle_5m'] - data['5m_ATR'] * multiplier

    # Keltner Channel Strategy Implementation
    data['Upper_Signal_5m'] = np.where(data['5m_Close'] > data['kc_upper_5m'], 1, 0)
    data['Middle_Signal_5m'] = np.where(data['5m_Close'] < data['kc_middle_5m'], -1, 0)
    data['Lower_Signal_5m'] = np.where(data['5m_Close'] < data['kc_lower_5m'], -1, 0)
    
    # Keltner Channel Calculation 15 minutes data
    data['15m_TR'] = np.maximum(data['15m_High'] - data['15m_Low'], np.maximum(abs(data['15m_High'] - data['15m_Close'].shift(1)), abs(data['15m_Low'] - data['15m_Close'].shift())))
    data['15m_ATR'] = data['15m_TR'].rolling(window=atr_lookback).mean()
    data['kc_middle_15m'] = data['15m_Close'].ewm(span=kc_lookback, adjust=False).mean()
    data['kc_upper_15m'] = data['kc_middle_15m'] + data['15m_ATR'] * multiplier
    data['kc_lower_15m'] = data['kc_middle_15m'] - data['15m_ATR'] * multiplier

    # Keltner Channel Strategy Implementation
    data['Upper_Signal_15m'] = np.where(data['15m_Close'] > data['kc_upper_15m'], 1, 0)
    data['Middle_Signal_15m'] = np.where(data['15m_Close'] < data['kc_middle_15m'], -1, 0)
    data['Lower_Signal_15m'] = np.where(data['5m_Close'] < data['kc_lower_15m'], -1, 0)
 
    # Keltner Channel Calculation 60 minutes data
    data['60m_TR'] = np.maximum(data['60m_High'] - data['60m_Low'], np.maximum(abs(data['60m_High'] - data['60m_Close'].shift(1)), abs(data['60m_Low'] - data['60m_Close'].shift())))
    data['60m_ATR'] = data['60m_TR'].rolling(window=atr_lookback).mean()
    data['kc_middle_60m'] = data['60m_Close'].ewm(span=kc_lookback, adjust=False).mean()
    data['kc_upper_60m'] = data['kc_middle_60m'] + data['60m_ATR'] * multiplier
    data['kc_lower_60m'] = data['kc_middle_60m'] - data['60m_ATR'] * multiplier
  
    # Keltner Channel Strategy Implementation
    data['Upper_Signal_60m'] = np.where(data['60m_Close'] > data['kc_upper_60m'], 1, 0)
    data['Middle_Signal_60m'] = np.where(data['60m_Close'] < data['kc_middle_60m'], -1, 0)
    data['Lower_Signal_60m'] = np.where(data['60m_Close'] < data['kc_lower_60m'], -1, 0)
    
    # Keltner Channel Calculation 60 minutes data
    data['1d_TR'] = np.maximum(data['1d_High'] - data['1d_Low'], np.maximum(abs(data['1d_High'] - data['1d_Close'].shift(1)), abs(data['1d_Low'] - data['1d_Close'].shift())))
    data['1d_ATR'] = data['1d_TR'].rolling(window=kc_lookback).mean()
    data['kc_middle_1d'] = data['1d_Close'].ewm(span=kc_lookback, adjust=False).mean()
    data['kc_upper_1d'] = data['kc_middle_1d'] + data['1d_ATR'] * multiplier
    data['kc_lower_1d'] = data['kc_middle_1d'] - data['1d_ATR'] * multiplier

    # Keltner Channel Strategy Implementation
    data['Upper_Signal_1d'] = np.where(data['1d_Close'] > data['kc_upper_1d'], 1, 0)
    data['Middle_Signal_1d'] = np.where(data['1d_Close'] < data['kc_middle_1d'], -1, 0)
    data['Lower_Signal_1d'] = np.where(data['1d_Close'] < data['kc_lower_1d'], -1, 0)
  
    return data

#     # Check if a new buy or sell signal is generated in 1day timeframe
#     mid_kc_signal_1d = data['Middle_Signal_1d'].values
#     mid_kc_signal_60m = data['Middle_Signal_60m'].values
#     mid_kc_signal_15m = data['Middle_Signal_15m'].values
#     mid_kc_signal_5m = data['Middle_Signal_5m'].values
    
#     if (mid_kc_signal_1d[-1] == 1) & (mid_kc_signal_1d[-2] == -1) & (mid_kc_signal_60m[-1] == 1) & (mid_kc_signal_15m[-1] == 1) & (mid_kc_signal_5m[-1] == 1):
#         message = f"Buy Signal: \n{symbol} \n Price crossed below Middle Keltner Channel in 1day timeframe and also followed by 60min, 15min and 5min timeframes as well"
#         # send_whatsapp_messages("leads.csv", message)
#         # send_email(message_text=message)

#     elif (mid_kc_signal_1d[-1] == 1) & (mid_kc_signal_1d[-2] == -1) & (mid_kc_signal_60m[-1] == 1) & (mid_kc_signal_15m[-1] == 1):
#         message = f"Buy Signal: \n{symbol} \n Price crossed below Middle Keltner Channel in 1day timeframe and also followed by 60min and 15min timeframe as well"
#         # send_whatsapp_messages("leads.csv", message)
#         # send_email(message_text=message)

#     elif (mid_kc_signal_1d[-1] == 1) & (mid_kc_signal_1d[-2] == -1) & (mid_kc_signal_60m[-1] == 1):
#         message = f"Buy Signal: \n{symbol} \n Price crossed below Middle Keltner Channel in 1day timeframe and also followed by 60min timeframe as well"
#         # send_whatsapp_messages("leads.csv", message)
#         # send_email(message_text=message)

#     elif (mid_kc_signal_1d[-1] == 1) & (mid_kc_signal_1d[-2] == -1):
#         message = f"Buy Signal: \n{symbol} \n Price crossed below Middle Keltner Channel in 1day timeframe"
#         # send_whatsapp_messages("leads.csv", message)
#         # send_email(message_text=message)

#     elif (mid_kc_signal_1d[-1] == -1) & (mid_kc_signal_1d[-2] == 1) & (mid_kc_signal_60m[-1] == -1) & (mid_kc_signal_15m[-1] == -1) & (mid_kc_signal_5m[-1] == -1):
#         message = f"Sell Signal:\n {symbol} \nPrice crossed above Middle Keltner Channel in 1day timeframe and also followed by 60min, 15min and 5min timeframes as well"
#         # send_whatsapp_messages("leads.csv", message)
#         # send_email(message_text=message)

#     elif (mid_kc_signal_1d[-1] == -1) & (mid_kc_signal_1d[-2] == 1) & (mid_kc_signal_60m[-1] == -1) & (mid_kc_signal_15m[-1] == -1):
#         message = f"Sell Signal:\n {symbol} \nPrice crossed above Middle Keltner Channel in 1day timeframe and also followed by 60min and 15min timeframe as well"
#         # send_whatsapp_messages("leads.csv", message)
#         # send_email(message_text=message)

#     elif (mid_kc_signal_1d[-1] == -1) & (mid_kc_signal_1d[-2] == 1) & (mid_kc_signal_60m[-1] == -1):
#         message = f"Sell Signal:\n {symbol} \nPrice crossed above Middle Keltner Channel in 1day timeframe and also followed by 60min timeframe as well"
#         # send_whatsapp_messages("leads.csv", message)
#         # send_email(message_text=message)

#     elif (mid_kc_signal_1d[-1] == -1) & (mid_kc_signal_1d[-2] == 1):
#         message = f"Sell Signal:\n {symbol} \nPrice crossed above Middle Keltner Channel in 1day timeframe"
#         # send_whatsapp_messages("leads.csv", message)
#         # send_email(message_text=message)

#     else:
#         print("No Signal in 1day timeframe above Middle Keltner Channel or below Middle Keltner Channel")
#     upper_kc_signal_1d = data['Upper_Signal_1d'].values
#     upper_kc_signal_60m = data['Upper_Signal_60m'].values
#     upper_kc_signal_15m = data['Upper_Signal_15m'].values
#     upper_kc_signal_5m = data['Upper_Signal_5m'].values

#     lower_kc_signal_1d = data['Lower_Signal_1d'].values
#     lower_kc_signal_60m = data['Lower_Signal_60m'].values
#     lower_kc_signal_15m = data['Lower_Signal_15m'].values
#     lower_kc_signal_5m = data['Lower_Signal_5m'].values


#     if (lower_kc_signal_1d[-1] == 1) & (lower_kc_signal_1d[-2] == -1) & (lower_kc_signal_60m[-1] == 1) & (lower_kc_signal_15m[-1] == 1) & (lower_kc_signal_5m[-1] == 1):
#         message = f"Buy Signal: \n{symbol} \n Price crossed below Lower Keltner Channel in 1day timeframe and also followed by 60min, 15min and 5min timeframes as well"
#         # send_whatsapp_messages("leads.csv", message)
#         # send_email(message_text=message)

#     elif (lower_kc_signal_1d[-1] == 1) & (lower_kc_signal_1d[-2] == -1) & (lower_kc_signal_60m[-1] == 1) & (lower_kc_signal_15m[-1] == 1):
#         message = f"Buy Signal: \n{symbol} \n Price crossed below Lower Keltner Channel in 1day timeframe and also followed by 60min and 15min timeframe as well"
#         # send_whatsapp_messages("leads.csv", message)
#         # send_email(message_text=message)

#     elif (lower_kc_signal_1d[-1] == 1) & (lower_kc_signal_1d[-2] == -1) & (lower_kc_signal_60m[-1] == 1):
#         message = f"Buy Signal: \n{symbol} \n Price crossed below Lower Keltner Channel in 1day timeframe and also followed by 60min timeframe as well"
#         # send_whatsapp_messages("leads.csv", message)
#         # send_email(message_text=message)

#     elif (lower_kc_signal_1d[-1] == 1) & (lower_kc_signal_1d[-2] == -1):
#         message = f"Buy Signal: \n{symbol} \n Price crossed below Lower Keltner Channel in 1day timeframe"
#         # send_whatsapp_messages("leads.csv", message)
#         # send_email(message_text=message)

#     elif (upper_kc_signal_1d[-1] == -1) & (upper_kc_signal_1d[-2] == 1) & (upper_kc_signal_60m[-1] == -1) & (upper_kc_signal_15m[-1] == -1) & (upper_kc_signal_5m[-1] == -1):
#         message = f"Sell Signal:\n {symbol} \nPrice crossed above Upper Keltner Channel in 1day timeframe and also followed by 60min, 15min and 5min timeframes as well"
#         # send_whatsapp_messages("leads.csv", message)
#         # send_email(message_text=message)

#     elif (upper_kc_signal_1d[-1] == -1) & (upper_kc_signal_1d[-2] == 1) & (upper_kc_signal_60m[-1] == -1) & (upper_kc_signal_15m[-1] == -1):
#         message = f"Sell Signal:\n {symbol} \nPrice crossed above Upper Keltner Channel in 1day timeframe and also followed by 60min and 15min timeframe as well"
#         # send_whatsapp_messages("leads.csv", message)
#         # send_email(message_text=message)

#     elif (upper_kc_signal_1d[-1] == -1) & (upper_kc_signal_1d[-2] == 1) & (upper_kc_signal_60m[-1] == -1):
#         message = f"Sell Signal:\n {symbol} \nPrice crossed above Upper Keltner Channel in 1day timeframe and also followed by 60min timeframe as well"
#         # send_whatsapp_messages("leads.csv", message)
#         # send_email(message_text=message)
    
#     elif (upper_kc_signal_1d[-1] == -1) & (upper_kc_signal_1d[-2] == 1):
#         message = f"Sell Signal:\n {symbol} \nPrice crossed above Upper Keltner Channel in 1day timeframe"
#         # send_whatsapp_messages("leads.csv", message)
#         # send_email(message_text=message)

#     else:
#         print("No Signal in 1day timeframe above Upper Keltner Channel or below Lower Keltner Channel")


    
#     # Check if a new buy or sell signal is generated in 60min timeframe
        
#     if (mid_kc_signal_60m[-1] == 1) & (mid_kc_signal_60m[-2] == -1) & (mid_kc_signal_15m[-1] == 1) & (mid_kc_signal_5m[-1] == 1):
#         message = f"Buy Signal: \n{symbol} \n Price crossed below Middle Keltner Channel in 60min timeframe and also followed by 15min and 5min timeframes as well"
#         # send_whatsapp_messages("leads.csv", message)
#         # send_email(message_text=message)

#     elif (mid_kc_signal_60m[-1] == 1) & (mid_kc_signal_60m[-2] == -1) & (mid_kc_signal_15m[-1] == 1):
#         message = f"Buy Signal: \n{symbol} \n Price crossed below Middle Keltner Channel in 60min timeframe and also followed by 15min timeframe as well"
#         # send_whatsapp_messages("leads.csv", message)
#         # send_email(message_text=message)

#     elif (mid_kc_signal_60m[-1] == 1) & (mid_kc_signal_60m[-2] == -1):
#         message = f"Buy Signal: \n{symbol} \n Price crossed below Middle Keltner Channel in 60min timeframe"
#         # send_whatsapp_messages("leads.csv", message)
#         # send_email(message_text=message)

#     elif (mid_kc_signal_60m[-1] == -1) & (mid_kc_signal_60m[-2] == 1) & (mid_kc_signal_15m[-1] == -1) & (mid_kc_signal_5m[-1] == -1):
#         message = f"Sell Signal:\n {symbol} \nPrice crossed above Middle Keltner Channel in 60min timeframe and also followed by 15min and 5min timeframes as well"
#         # send_whatsapp_messages("leads.csv", message)
#         # send_email(message_text=message)

#     elif (mid_kc_signal_60m[-1] == -1) & (mid_kc_signal_60m[-2] == 1) & (mid_kc_signal_15m[-1] == -1):
#         message = f"Sell Signal:\n {symbol} \nPrice crossed above Middle Keltner Channel in 60min timeframe and also followed by 15min timeframe as well"
#         # send_whatsapp_messages("leads.csv", message)
#         # send_email(message_text=message)

#     elif (mid_kc_signal_60m[-1] == -1) & (mid_kc_signal_60m[-2] == 1):
#         message = f"Sell Signal:\n {symbol} \nPrice crossed above Middle Keltner Channel in 60min timeframe"
#         # send_whatsapp_messages("leads.csv", message)
#         # send_email(message_text=message)

#     else:
#         print("No Signal in 60min timeframe above Middle Keltner Channel or below Middle Keltner Channel")
    
#     if (lower_kc_signal_60m[-1] == 1) & (lower_kc_signal_60m[-2] == -1) & (lower_kc_signal_15m[-1] == 1) & (lower_kc_signal_5m[-1] == 1):
#         message = f"Buy Signal: \n{symbol} \n Price crossed below Lower Keltner Channel in 60min timeframe and also followed by 15min and 5min timeframes as well"
#         # send_whatsapp_messages("leads.csv", message)
#         # send_email(message_text=message)

#     elif (lower_kc_signal_60m[-1] == 1) & (lower_kc_signal_60m[-2] == -1) & (lower_kc_signal_15m[-1] == 1):
#         message = f"Buy Signal: \n{symbol} \n Price crossed below Lower Keltner Channel in 60min timeframe and also followed by 15min timeframe as well"
#         # send_whatsapp_messages("leads.csv", message)
#         # send_email(message_text=message)

#     elif (lower_kc_signal_60m[-1] == 1) & (lower_kc_signal_60m[-2] == -1):
#         message = f"Buy Signal: \n{symbol} \n Price crossed below Lower Keltner Channel in 60min timeframe"
#         # send_whatsapp_messages("leads.csv", message)
#         # send_email(message_text=message)

#     elif (upper_kc_signal_60m[-1] == -1) & (upper_kc_signal_60m[-2] == 1) & (upper_kc_signal_15m[-1] == -1) & (upper_kc_signal_5m[-1] == -1):
#         message = f"Sell Signal:\n {symbol} \nPrice crossed above Upper Keltner Channel in 60min timeframe and also followed by 15min and 5min timeframes as well"
#         # send_whatsapp_messages("leads.csv", message)
#         # send_email(message_text=message)

#     elif (upper_kc_signal_60m[-1] == -1) & (upper_kc_signal_60m[-2] == 1) & (upper_kc_signal_15m[-1] == -1):
#         message = f"Sell Signal:\n {symbol} \nPrice crossed above Upper Keltner Channel in 60min timeframe and also followed by 15min timeframe as well"
#         # send_whatsapp_messages("leads.csv", message)
#         # send_email(message_text=message)

#     elif (upper_kc_signal_60m[-1] == -1) & (upper_kc_signal_60m[-2] == 1):
#         message = f"Sell Signal:\n {symbol} \nPrice crossed above Upper Keltner Channel in 60min timeframe"
#         # send_whatsapp_messages("leads.csv", message)
#         # send_email(message_text=message)

#     else:
#         print("No Signal in 60min timeframe above Upper Keltner Channel or below Lower Keltner Channel")



#     # Check if a new buy or sell signal is generated in 15min timeframe

#     if (mid_kc_signal_15m[-1] == 1) & (mid_kc_signal_15m[-2] == -1) & (mid_kc_signal_5m[-1] == 1):
#         message = f"Buy Signal: \n{symbol} \n Price crossed below Middle Keltner Channel in 15min timeframe and also followed by 5min timeframe as well"
#         # send_whatsapp_messages("leads.csv", message)
#         # send_email(message_text=message)
    
#     elif (mid_kc_signal_15m[-1] == 1) & (mid_kc_signal_15m[-2] == -1):
#         message = f"Buy Signal: \n{symbol} \n Price crossed below Middle Keltner Channel in 15min timeframe"
#         # send_whatsapp_messages("leads.csv", message)
#         # send_email(message_text=message)
    
#     elif (mid_kc_signal_15m[-1] == -1) & (mid_kc_signal_15m[-2] == 1) & (mid_kc_signal_5m[-1] == -1):
#         message = f"Sell Signal:\n {symbol} \nPrice crossed above Middle Keltner Channel in 15min timeframe and also followed by 5min timeframe as well"
#         # send_whatsapp_messages("leads.csv", message)
#         # send_email(message_text=message)

#     elif (mid_kc_signal_15m[-1] == -1) & (mid_kc_signal_15m[-2] == 1):
#         message = f"Sell Signal:\n {symbol} \nPrice crossed above Middle Keltner Channel in 15min timeframe"
#         # send_whatsapp_messages("leads.csv", message)
#         # send_email(message_text=message)

#     else:
#         print("No Signal in 15min timeframe above Middle Keltner Channel or below Middle Keltner Channel")
    
#     if (lower_kc_signal_15m[-1] == 1) & (lower_kc_signal_15m[-2] == -1) & (lower_kc_signal_5m[-1] == 1):
#         message = f"Buy Signal: \n{symbol} \n Price crossed below Lower Keltner Channel in 15min timeframe and also followed by 5min timeframe as well"
#         # send_whatsapp_messages("leads.csv", message)
#         # send_email(message_text=message)

#     elif (lower_kc_signal_15m[-1] == 1) & (lower_kc_signal_15m[-2] == -1):
#         message = f"Buy Signal: \n{symbol} \n Price crossed below Lower Keltner Channel in 15min timeframe"
#         # send_whatsapp_messages("leads.csv", message)
#         # send_email(message_text=message)

#     elif (upper_kc_signal_15m[-1] == -1) & (upper_kc_signal_15m[-2] == 1) & (upper_kc_signal_5m[-1] == -1):
#         message = f"Sell Signal:\n {symbol} \nPrice crossed above Upper Keltner Channel in 15min timeframe and also followed by 5min timeframe as well"
#         # send_whatsapp_messages("leads.csv", message)
#         # send_email(message_text=message)

#     elif (upper_kc_signal_15m[-1] == -1) & (upper_kc_signal_15m[-2] == 1):
#         message = f"Sell Signal:\n {symbol} \nPrice crossed above Upper Keltner Channel in 15min timeframe"
#         # send_whatsapp_messages("leads.csv", message)
#         # send_email(message_text=message)

#     else:
#         print("No Signal in 15min timeframe above Upper Keltner Channel or below Lower Keltner Channel")
     
#     # # Display the figure
#     # clear_output(wait=True)
#     # display(fig)

#     return message   #, fig

def get_previous_working_day(df):
    latest_date = df['Date'].iloc[-1]
    previous_date = latest_date - pd.Timedelta(days=1)

    while previous_date.weekday() >= 5:  # Saturday (5) or Sunday (6)
        previous_date -= pd.Timedelta(days=1)

    date_to_fetch = str(previous_date.strftime('%Y-%m-%d'))
    return date_to_fetch

def track_time(stock_symbol, start_time):
    end_time = time.time()
    total_time = end_time - start_time
    return {'Stock': stock_symbol, 'Start Time': start_time, 'End Time': end_time, 'Total Time': total_time}

def send_signal(message):
    send_whatsapp_messages("leads.csv", message)
    print(message)

def check_signals(symbol, timeframe, kc_signals):
    mid_kc_signal = kc_signals['middle'][timeframe]
    lower_kc_signal = kc_signals['lower'][timeframe]
    upper_kc_signal = kc_signals['upper'][timeframe]

    if (mid_kc_signal[-1] == 1) and (mid_kc_signal[-2] == -1):
        message = f"Buy Signal: \n{symbol} \nPrice crossed below Middle Keltner Channel in {timeframe} timeframe"
        if timeframe == '1day':
            if all([kc_signals['middle']['60min'][-1] == 1, kc_signals['middle']['15min'][-1] == 1, kc_signals['middle']['5min'][-1] == 1]):
                message += " and also followed by 60min, 15min, and 5min timeframes as well"
            elif all([kc_signals['middle']['60min'][-1] == 1, kc_signals['middle']['15min'][-1] == 1]):
                message += " and also followed by 60min and 15min timeframes as well"
            elif kc_signals['middle']['60min'][-1] == 1:
                message += " and also followed by 60min timeframe as well"
        elif timeframe == '60min':
            if all([kc_signals['middle']['15min'][-1] == 1, kc_signals['middle']['5min'][-1] == 1]):
                message += " and also followed by 15min and 5min timeframes as well"
            elif kc_signals['middle']['15min'][-1] == 1:
                message += " and also followed by 15min timeframe as well"
        send_signal(message)

    elif (mid_kc_signal[-1] == -1) and (mid_kc_signal[-2] == 1):
        message = f"Sell Signal:\n{symbol} \nPrice crossed above Middle Keltner Channel in {timeframe} timeframe"
        if timeframe == '1day':
            if all([kc_signals['middle']['60min'][-1] == -1, kc_signals['middle']['15min'][-1] == -1, kc_signals['middle']['5min'][-1] == -1]):
                message += " and also followed by 60min, 15min, and 5min timeframes as well"
            elif all([kc_signals['middle']['60min'][-1] == -1, kc_signals['middle']['15min'][-1] == -1]):
                message += " and also followed by 60min and 15min timeframes as well"
            elif kc_signals['middle']['60min'][-1] == -1:
                message += " and also followed by 60min timeframe as well"
        elif timeframe == '60min':
            if all([kc_signals['middle']['15min'][-1] == -1, kc_signals['middle']['5min'][-1] == -1]):
                message += " and also followed by 15min and 5min timeframes as well"
            elif kc_signals['middle']['15min'][-1] == -1:
                message += " and also followed by 15min timeframe as well"
        send_signal(message)

    if (lower_kc_signal[-1] == 1) and (lower_kc_signal[-2] == -1):
        message = f"Buy Signal: \n{symbol} \nPrice crossed below Lower Keltner Channel in {timeframe} timeframe"
        if timeframe == '1day':
            if all([kc_signals['lower']['60min'][-1] == 1, kc_signals['lower']['15min'][-1] == 1, kc_signals['lower']['5min'][-1] == 1]):
                message += " and also followed by 60min, 15min, and 5min timeframes as well"
            elif all([kc_signals['lower']['60min'][-1] == 1, kc_signals['lower']['15min'][-1] == 1]):
                message += " and also followed by 60min and 15min timeframes as well"
            elif kc_signals['lower']['60min'][-1] == 1:
                message += " and also followed by 60min timeframe as well"
        elif timeframe == '60min':
            if all([kc_signals['lower']['15min'][-1] == 1, kc_signals['lower']['5min'][-1] == 1]):
                message += " and also followed by 15min and 5min timeframes as well"
            elif kc_signals['lower']['15min'][-1] == 1:
                message += " and also followed by 15min timeframe as well"
        send_signal(message)

    elif (upper_kc_signal[-1] == -1) and (upper_kc_signal[-2] == 1):
        message = f"Sell Signal:\n{symbol} \nPrice crossed above Upper Keltner Channel in {timeframe} timeframe"
        if timeframe == '1day':
            if all([kc_signals['upper']['60min'][-1] == -1, kc_signals['upper']['15min'][-1] == -1, kc_signals['upper']['5min'][-1] == -1]):
                message += " and also followed by 60min, 15min, and 5min timeframes as well"
            elif all([kc_signals['upper']['60min'][-1] == -1, kc_signals['upper']['15min'][-1] == -1]):
                message += " and also followed by 60min and 15min timeframes as well"
            elif kc_signals['upper']['60min'][-1] == -1:
                message += " and also followed by 60min timeframe as well"
        elif timeframe == '60min':
            if all([kc_signals['upper']['15min'][-1] == -1, kc_signals['upper']['5min'][-1] == -1]):
                message += " and also followed by 15min and 5min timeframes as well"
            elif kc_signals['upper']['15min'][-1] == -1:
                message += " and also followed by 15min timeframe as well"
        send_signal(message)
    else:
        print(f"No Signal in {timeframe} timeframe above Upper Keltner Channel or below Lower Keltner Channel")

def main(stock_symbol, data):
    kc_signals = {
        'middle': {
            '1day': data['Middle_Signal_1d'].values,
            '60min': data['Middle_Signal_60m'].values,
            '15min': data['Middle_Signal_15m'].values,
            '5min': data['Middle_Signal_5m'].values,
        },
        'lower': {
            '1day': data['Lower_Signal_1d'].values,
            '60min': data['Lower_Signal_60m'].values,
            '15min': data['Lower_Signal_15m'].values,
            '5min': data['Lower_Signal_5m'].values,
        },
        'upper': {
            '1day': data['Upper_Signal_1d'].values,
            '60min': data['Upper_Signal_60m'].values,
            '15min': data['Upper_Signal_15m'].values,
            '5min': data['Upper_Signal_5m'].values,
        }
    }

    for timeframe in ['1day', '60min', '15min']:
        check_signals(stock_symbol, timeframe, kc_signals)

# Initialize metadata and get stock symbols
metadata = MetaData()
metadata.reflect(bind=engine)
stocks_unsorted = list(metadata.tables.keys())
stocks = sorted(stocks_unsorted)[::3]
print(f'\n{stocks}\n')

while True:
    time_tracking_data = []
    combine_msgs = []

    for stock_symbol in stocks:
        start_time = time.time()
        print(stock_symbol)

        try:
            sql_df = pd.read_sql_table(stock_symbol, engine)
            if sql_df.empty:
                continue

            date_to_fetch_from = get_previous_working_day(sql_df)
            sql_df['Date'] = sql_df['Date'].astype(str)
            index_list = sql_df.index[sql_df['Date'] == date_to_fetch_from].tolist()
            if not index_list:
                continue

            for idx in range(index_list[-1] + 1, sql_df.shape[0]):
                sql_df.drop(idx, inplace=True)

            next_date = pd.to_datetime(sql_df['Date'].iloc[-1]) + pd.Timedelta(days=1)
            next_date = str(next_date.strftime('%Y-%m-%d'))

            if stock_symbol in ["^NSEI", "^NSEBANK"]:
                latest_df = initial_fetch_data(stock_symbol.upper(), next_date)
            else:
                latest_df = initial_fetch_data(f"{stock_symbol.upper()}.NS", next_date)

            latest_df.reset_index(inplace=True)
            final_df = pd.concat([sql_df, latest_df], axis=0).reset_index(drop=True)
            final_df.drop_duplicates(subset=['5m_Open', '5m_High', '5m_Low', '5m_Close', '5m_Volume'], keep='last', inplace=True)
            final_df.dropna(subset=['5m_Open', '5m_High', '5m_Low', '5m_Close'], inplace=True)
            final_df.fillna(method='ffill', inplace=True)

            data = keltner_channel_plot(data=final_df, kc_lookback=10, multiplier=1, atr_lookback=10, symbol=stock_symbol.upper())
            if isinstance(msg, str) and msg.strip():
                combine_msgs.append(msg)

            time_tracking_data.append(track_time(stock_symbol, start_time))
            time.sleep(1)

        except IndexError as e:
            print(f"Failed download: {stock_symbol}: {e}")

    update = [word.replace('&', 'and') for word in combine_msgs]
    formatted_alerts = [alert + ' ' for alert in update]
    combine_msg = '\n\n'.join(formatted_alerts)
    send_whatsapp_messages("leads.csv", combine_msg)

    time_df = pd.DataFrame(time_tracking_data)
    current_time = time.time()
    latest_time_with_date = datetime.fromtimestamp(current_time).strftime("%Y_%m_%d_%H_%M_%S")
    time_df.to_csv(f'KELTNER_stock_time_sheet{latest_time_with_date}.csv')

    time.sleep(900)
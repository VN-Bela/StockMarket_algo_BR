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
from sqlalchemy import create_engine, Table, MetaData  # Library for SQL operations
from requests.exceptions import ReadTimeout
from plotly.subplots import make_subplots

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
            text = raw_text.replace(" ","%20")
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

# Calculate MACD and Signal line
def calculate_macd(data, short_window=34, large_window=81, signal_window=9):
    data['EMA_short'] = data['5m_Close'].ewm(span=short_window, adjust=False).mean()
    data['EMA_large'] = data['5m_Close'].ewm(span=large_window, adjust=False).mean()
    data['MACD'] = data['EMA_short'] - data['EMA_large']
    data['Signal_Line'] = data['MACD'].ewm(span=signal_window, adjust=False).mean()
    return data

# Identify signals
def identify_signals(data):
    message=""
    data['Crossover'] = data['MACD'] - data['Signal_Line']
    data['Signal'] = 0
    data.loc[data['Crossover'] > 0, 'Signal'] = 1
    data.loc[data['Crossover'] < 0, 'Signal'] = -1
    
 
def initial_fetch_data(symbol, date):
    date = datetime.strptime(date, '%Y-%m-%d')

    # Download 5-minute data
    min5 = yf.download(symbol, start=date, interval='5m')
    min5.index = min5.index.strftime('%Y-%m-%d %H:%M:%S')
    min5.index = pd.to_datetime(min5.index)
    min5['Date'] = min5.index.date
    for i in range(len(min5.columns)):
        min5.columns.values[i] = "5m_" + min5.columns.values[i]
    min5.rename(columns={'5m_Date': 'Date'}, inplace=True)
    min5 = min5.reindex(columns=['Date', '5m_Open', '5m_High', '5m_Low', '5m_Close', '5m_Adj Close', '5m_Volume'])

    # Download 15-minute data
    min15 = yf.download(symbol, start=date, interval='15m')
    min15.index = min15.index.strftime('%Y-%m-%d %H:%M:%S')
    min15.index = pd.to_datetime(min15.index)
    min15['Date'] = min15.index.date
    for i in range(len(min15.columns)):
        min15.columns.values[i] = "15m_" + min15.columns.values[i]
    min15.rename(columns={'15m_Date': 'Date'}, inplace=True)
    min15 = min15.reindex(columns=['Date', '15m_Open', '15m_High', '15m_Low', '15m_Close', '15m_Adj Close', '15m_Volume'])
    min15 = min15.resample('5T').ffill()

    # Download 60-minute data
    min60 = yf.download(symbol, start=date, interval='60m')
    min60.index = min60.index.strftime('%Y-%m-%d %H:%M:%S')
    min60.index = pd.to_datetime(min60.index)
    min60['Date'] = min60.index.date
    for i in range(len(min60.columns)):
        min60.columns.values[i] = "60m_" + min60.columns.values[i]
    min60.rename(columns={'60m_Date': 'Date'}, inplace=True)
    min60 = min60.reindex(columns=['Date', '60m_Open', '60m_High', '60m_Low', '60m_Close', '60m_Adj Close', '60m_Volume'])
    min60 = min60.resample('5T').ffill()

    # Download daily data
    day1 = yf.download(symbol, start=date, interval='1d')
    day1.index = day1.index.strftime('%Y-%m-%d %H:%M:%S')
    day1.index = pd.to_datetime(day1.index)
    day1['Date'] = day1.index.date
    for i in range(len(day1.columns)):
        day1.columns.values[i] = "1d_" + day1.columns.values[i]
    day1.rename(columns={'1d_Date': 'Date'}, inplace=True)
    day1 = day1.reindex(columns=['Date', '1d_Open', '1d_High', '1d_Low', '1d_Close', '1d_Adj Close', '1d_Volume'])

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
    main_df = main_df.dropna(subset=['5m_Open', '5m_High', '5m_Low', '5m_Close'])
    return main_df

# Function to track time taken
def track_time(stock_symbol, start_time):
    end_time = time.time()
    total_time = end_time - start_time
    return {'Stock': stock_symbol, 'Start Time': start_time, 'End Time': end_time, 'Total Time': total_time}

# Reflect the database tables
metadata = MetaData()
metadata.reflect(bind=engine)

stocks_unsorted = metadata.tables.keys()
stocks = sorted(stocks_unsorted)[::3]
print(f'\n{stocks}\n')

# Define an infinite loop to continuously fetch minute-level stock data and plot EMAs
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
            sql_df.drop(idx,inplace=True)

        next_date = sql_df['Date'].iloc[-1]
        next_date = pd.to_datetime(next_date)
        next_date = next_date + pd.Timedelta(days=1)
        next_date = str(next_date.strftime('%Y-%m-%d'))

        if stock_symbol == "^NSEI" or stock_symbol == "^NSEBANK":
            latest_df = initial_fetch_data(stock_symbol.upper(), next_date)
        else:
            latest_df = initial_fetch_data(f"{stock_symbol.upper()}.NS", next_date)

        latest_df.reset_index(inplace=True)
        print(latest_df.tail())

        final_df = pd.concat([sql_df, latest_df], axis=0)
        final_df.reset_index(drop=True, inplace=True)
        final_df.drop_duplicates(subset=['5m_Open','5m_High','5m_Low','5m_Close','5m_Volume'], keep='last', inplace=True)
        final_df = final_df.dropna(subset=['5m_Open','5m_High','5m_Low','5m_Close'])
        final_df.fillna(method='ffill', inplace=True)
        # print(final_df.tail())

        # Call the MACD_plot function to plot Exponential Moving Averages (EMAs) on the downloaded data
        # Note: 'fig' is likely a pre-existing figure object used for plotting
        data = calculate_macd(data=final_df,short_window=34, large_window=81,signal_window=9)
        msg = calculate_macd(data)
        print(msg)
        if not msg.empty:
            combine_msgs.append(msg.to_string())

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
    time_df.to_csv(f'MACD_stock_time_sheet{latest_time_with_date}.csv')
    print(combine_msg)


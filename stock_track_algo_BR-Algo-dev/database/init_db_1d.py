import pandas as pd
import numpy as np
import yfinance as yf
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from warnings import filterwarnings
filterwarnings('ignore')

def select_database(database_name):

    db = database_name

    return db

def create_db_engine(user, password, host_name, database_name):
    """
    Creates a connection to the MySQL database.
    """
    init_engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host_name}/{database_name}')
    return init_engine

def connect_to_engine(engine):
    conn = engine.connect()
    return conn

user_name = "postgres"
user_password = "manager"
host = "localhost"
port = "5432"
database_name = "stock_market_db"

# PostgreSQL connection string
postgres_connection_string = f"postgresql://{user_name}:{user_password}@{host}:{port}/{database_name}"

# Create the connection to the database
engine = create_engine(postgres_connection_string)

conn = connect_to_engine(engine)


def initial_fetch_data(symbol, date, conn):

    if symbol == "^NSEI" or symbol == "^NSEBANK":
        symbol = symbol.upper()
    else:
        symbol = f"{symbol}.NS".upper()

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
    day1.index = day1.index.strftime('%Y-%m-%d %H:%M:%S')
    day1.index = pd.to_datetime(day1.index)
    day1['Date'] = day1.index.date
    for i in range(len(day1.columns)):
        day1.columns.values[i] = "1d_" + day1.columns.values[i]
    day1.rename(columns={'1d_Date':'Date'}, inplace=True)
    day1 = day1.reindex(columns=['Date','1d_Open', '1d_High', '1d_Low', '1d_Close', '1d_Adj Close', '1d_Volume'])
    day1 = day1.resample('5T').ffill()

    # Concatenate dataframes
    main_df = pd.concat([min5, min15, min60, day1], axis=1)
    # Drop duplicate columns
    main_df = main_df.loc[:, ~main_df.columns.duplicated()]
    # pd.set_option('display.max_rows', None)
    main_df = main_df.dropna(subset=['5m_Open','5m_High','5m_Low','5m_Close'])
    
    if symbol == "^NSEI" or symbol == "^NSEBANK":
        main_df.to_sql(symbol, conn, if_exists='append', index=True, index_label='Datetime')
    else:
        main_df.to_sql(symbol[:-3], conn, if_exists='append', index=True, index_label='Datetime')

    return main_df

# stocks = ['acc', 'acl', 'adanient', 'adaniports', 'adanipower', 'alkem', '^NSEI', 'axisbank', 'bajaj-auto', 'bajajfinsv', 'bajfinance', 'biocon', 'boschltd', 'bpcl', 'britannia', 'canbk', 'cgpower', 'cholafin', 'cipla', 'coalindia', 'coforge', 'colpal', 'ltim', 'ltts', 'lupin', 'm&m', 'prestige', 'sbicard', 'sbilife', 'sbin', 'shreecem', 'muthootfin', 'titan', 'torntpharm', 'abb', 'industower', 'voltas', 'bhartiartl', 'bhel', 'pnb', 'policybzr', 'abfrl', 'polycab', 'shriramfin', 'siemens', 'sonacoms', 'srf', 'sunpharma', 'suntv', 'syngene', 'tatachem', 'tatacomm', 'tataconsum', 'tataelxsi', 'tatamotors', 'tatamtrdvr', 'itc', 'jindalstel', 'ramcocem', 'recltd', 'infy', 'ioc', 'tatapower', 'tatasteel', 'ipcalab', 'irctc', '^NSEBANK', 'YESBANK', 'ZYDUSLIFE', 'balkrisind', 'bankbaroda', 'bataindia', 'bdl', 'bel', 'bergepaint', 'bharatforg', 'concor', 'coromandel', 'crompton', 'cumminsind', 'reliance', 'sail', 'dalbharat', 'deepakntr', 'msumi', 'powergrid', 'epigral', 'escorts', 'fact', 'fluorochem', 'gail', 'gland', 'delhivery', 'divislab', 'dixon', 'dlf', 'drreddy', 'eichermot', 'm&mfin', 'mankind', 'marico', 'maruti', 'maxhealth', 'mazdock', 'mcdowell-n', 'mfsl', 'motherson', 'jswenergy', 'jswsteel', 'jublfood', 'kotakbank', 'ambujacem', 'aplapollo', 'naukri', 'mphasis', 'godrejprop', 'dabur', 'grasim', 'gujgasltd', 'hal', 'havells', 'kpittech', 'l&tfh', 'latentview', 'lauruslabs', 'lichsgfin', 'lici', 'hcltech', 'hdfcamc', 'hdfcbank', 'hdfclife', 'irfc', 'ubl', 'ultracemco', 'unionbank', 'upl', 'vbl', 'vedl', 'godrejcp', 'wipro', 'pel', 'trent', 'tvsmotor', 'aartiind', 'persistent', 'petronet', 'pfc', 'pghh', 'pidilitind', 'piind', 'tcs', 'techm', 'tiindia', 'navinfluor', 'nestleind', 'nhpc', 'nmdc', 'ntpc', 'nykaa', 'oberoirlty', 'oil', 'ongc', 'pageind', 'patanjali', 'heromotoco', 'hindalco', 'hindpetro', 'hindunilvr', 'icicibank', 'icicigi', 'icicipruli', 'idfcfirstb', 'igl', 'indhotel', 'indigo', 'indusindbk', 'apollohosp', 'apollotyre', 'asianpaint', 'lodha', 'lt', 'astral', 'aubank', 'auropharma', 'abcapital']
# stocks = ['reliance', '^NSEI', '^NSEBANK', 'tatamotors','ireda']

stocks = [
    "AARTIIND", "ABB", "ACC", "ADANIENT", "ADANIPORTS", "ADANIPOWER", "ABCAPITAL", "ABFRL", "ALKEM", "AMBUJACEM",
    "APLAPOLLO", "APOLLOHOSP", "APOLLOTYRE", "ACL", "ASIANPAINT", "ASTRAL", "AUBANK", "AUROPHARMA", "AXISBANK",
    "BAJAJ-AUTO", "BAJFINANCE", "BAJAJFINSV", "BALKRISIND", "BANKBARODA", "BATAINDIA", "BERGEPAINT", "BDL", "BEL",
    "BHARATFORG", "BHEL", "BPCL", "BHARTIARTL", "BIOCON", "BOSCHLTD", "BRITANNIA", "CANBK", "CGPOWER", "CHOLAFIN",
    "CIPLA", "COALINDIA", "COFORGE", "COLPAL", "CONCOR", "COROMANDEL", "CROMPTON", "CUMMINSIND", "DABUR", "DALBHARAT",
    "DEEPAKNTR", "DELHIVERY", "DIVISLAB", "DIXON", "DLF", "DRREDDY", "EICHERMOT", "EPIGRAL", "ESCORTS", "FACT", "NYKAA",
    "GAIL", "GLAND", "GODREJCP", "GODREJPROP", "GRASIM", "FLUOROCHEM", "GUJGASLTD", "HAVELLS", "HCLTECH", "HDFCAMC",
    "HDFCBANK", "HDFCLIFE", "HEROMOTOCO", "HINDALCO", "HAL", "HINDPETRO", "HINDUNILVR", "ICICIBANK", "ICICIGI",
    "ICICIPRULI", "IDFCFIRSTB", "INDHOTEL", "IOC", "IRCTC", "IRFC", "IGL", "INDUSTOWER", "INDUSINDBK", "NAUKRI", "INFY",
    "INDIGO", "IPCALAB", "ITC", "JINDALSTEL", "JSWENERGY", "JSWSTEEL", "JUBLFOOD", "KOTAKBANK", "KPITTECH", "L&TFH",
    "LTTS", "LT", "LATENTVIEW", "LAURUSLABS", "LICHSGFIN", "LICI", "LICI", "LTIM", "LUPIN", "LODHA", "M&MFIN", "M&M",
    "MANKIND", "MARICO", "MARUTI", "MFSL", "MAXHEALTH", "MAZDOCK", "MSUMI", "MPHASIS", "MUTHOOTFIN", "NAVINFLUOR",
    "NESTLEIND", "NHPC", "NMDC", "NTPC", "OBEROIRLTY", "ONGC", "OIL", "PAGEIND", "PATANJALI", "POLICYBZR",
    "PERSISTENT", "PETRONET", "PIIND", "PIDILITIND", "PEL", "POLYCAB", "PFC", "POWERGRID", "PRESTIGE", "PGHH",
    "PNB", "RECLTD", "RELIANCE", "MOTHERSON", "SBICARD", "SBILIFE", "SHREECEM", "SHRIRAMFIN", "SIEMENS", "SONACOMS",
    "SRF", "SBIN", "SAIL", "SUNPHARMA", "SUNTV", "SYNGENE", "TATACHEM", "TATACOMM", "TCS", "TATACONSUM", "TATAELXSI",
    "TATAMTRDVR", "TATAMOTORS", "TATAPOWER", "TATASTEEL", "TECHM", "RAMCOCEM", "TITAN", "TORNTPHARM", "TORNTPOWER",
    "TRENT", "TIINDIA", "TVSMOTOR", "ULTRACEMCO", "UNIONBANK", "UBL", "MCDOWELL-N", "UPL", "VBL", "VEDL", "VOLTAS",
    "WIPRO", "YESBANK", "ZYDUSLIFE"
]

print(len(stocks))

before_2m_date = (datetime.now() - timedelta(days=55)).strftime("%Y-%m-%d")
print(f"*********************************\n{before_2m_date}\n*********************************")

for symbols in stocks:

    print(f"\nFetching data for {symbols}\n")

    date = before_2m_date

    try:

        data = initial_fetch_data(symbols, date, conn)

        print(f"Data fetched for {symbols}\n")

    except Exception as e:

        print(f"Error fetching data for {symbols}: {e}")

        continue
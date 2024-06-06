from warnings import filterwarnings
filterwarnings('ignore')
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table
import time
from datetime import datetime
import yfinance as yf

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

# Reflect the database tables
metadata = MetaData()
metadata.reflect(bind=engine)

table_names_list = metadata.tables.keys()

print(f"Tables in the database: {table_names_list}")

def rerun_fetch_data(symbol, date):

    if symbol == "^NSEI" or symbol == "^NSEBANK":
        symbol = symbol.upper()
    else:
        symbol = f"{symbol}.NS".upper()

    date = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
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

for table_name in table_names_list:

    print(f"\nProcessing table: {table_name}\n")

    try:

        df = pd.read_sql_table(table_name, con=engine)

        # Find the starting index of the latest chunk of consecutive null-filled values
        null_chunks = df['1d_Open'].isnull().astype(int).diff().fillna(0)
        latest_chunk_start_index = null_chunks[null_chunks == 1].index[-1]

        date_value = df.loc[latest_chunk_start_index, 'Datetime'].strftime('%Y-%m-%d %H:%M:%S')

        proper_df = df[df['Datetime'] < date_value]


        # print the last row datetime value of the proper_df
        null_values_df = df[df['Datetime'] >= date_value]


        datetime_value = df.loc[null_values_df.index[0], 'Datetime']

        date_value = datetime_value.strftime('%Y-%m-%d %H:%M:%S')

        after_df = rerun_fetch_data(table_name, str(datetime_value))

        # convert the index of after_df as Datetime column
        after_df['Datetime'] = after_df.index

        after_df = after_df.reindex(columns=['Datetime','Date','5m_Open', '5m_High', '5m_Low', '5m_Close', '5m_Adj Close', '5m_Volume',
                                '15m_Open', '15m_High', '15m_Low', '15m_Close', '15m_Adj Close', '15m_Volume',
                                '60m_Open', '60m_High', '60m_Low', '60m_Close', '60m_Adj Close', '60m_Volume',
                                '1d_Open', '1d_High', '1d_Low', '1d_Close', '1d_Adj Close', '1d_Volume'])
        
        after_df.reset_index(drop=True, inplace=True)

        final_df = pd.concat([proper_df, after_df])

        final_df.reset_index(drop=True, inplace=True)

        final_df.iloc[-2:, final_df.columns.get_indexer(['1d_Open', '1d_High', '1d_Low', '1d_Close', '1d_Adj Close', '1d_Volume'])] = None

        # final_df.to_csv(f'final_{table_name}.csv')

        # Delete the existing table
        table_to_delete = Table(table_name, metadata)
        table_to_delete.drop(engine)

        final_df.to_sql(table_name, engine, if_exists='replace', index=False)
        # after_df.to_csv(f'{table_name}.csv')
        print(f"\nTable {table_name} updated successfully\n")
    except Exception as e:
        print(f"Error: {e}")
        continue
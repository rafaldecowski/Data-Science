import pandas as pd
from pymongo import MongoClient
import requests
import shutil
import json
import urllib3
http = urllib3.PoolManager()
from bs4 import BeautifulSoup



def nasdaq_symbol_download(symbols):
    ''' Downloads the latest and most complete list of Nasdaq equities and inserts into a mdb collection
        Args:   symbols collection (str)
        Return: None
    '''
    url = "http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NASDAQ&render=download"
    response = http.request('GET', url, preload_content=False)
    soup = BeautifulSoup(response, 'lxml')
    ar = soup.get_text().split('\r\n')

    fullarray = []
    for i in ar:
        i = i.replace('"', '')
        i = i.split(',')
        fullarray.append(i[:-1])

    df = pd.DataFrame(fullarray)
    df = df.rename(columns=df.iloc[0])
    df = df.drop(0, axis=0)
    df = df[['Symbol', 'Name']]
    #j = df.to_json(index=False)
    symbol_list = json.loads(df.T.to_json()).values()

    # Insert the json-formatted symbols into the trader database
    symbols.remove()  # @todo remove this line for prod
    symbols.insert_many(symbol_list)



def nasdaq_symbol_universe(symbols):
    ''' Downloads the latest and most complete list of Nasdaq equities into "static" directory and inserts into a mdb collection
        Args:   symbols collection (str)
        Return: None
    '''

    # Download the entire universe of NASDAQ symbols and save as csv in the static directory
    # It will overwrite the file if exists
    try:
        url = "http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NASDAQ&render=download"
        r = requests.get(url, stream=True)
        with open('static/symbols.csv', 'wb') as out_file:
            shutil.copyfileobj(r.raw, out_file)
    except:
        print('Unable to download the symbol list from Nasdaq.')

    # Read the previously saved csv with symbol as json and insert into MDB
    try:
        # Read NASDAQ symbols from a csv and convert to json
        symbols_df = pd.read_csv('static/symbols.csv')
        symbols_json = json.loads(symbols_df.to_json(orient='records'))

        # Insert the json-formatted symbols into the trader database
        symbols.remove()  # @todo remove this line for prod
        symbols.insert_many(symbols_json)
    except:
        print('Unable to insert the symbol list into MDB.')





def cash_and_pl(blotter):
    ''' Add cash float to blotter and initialize an empty pandas dataframe with 9 columns for pl
        Args:   None
        Return: portfolio(dataframe)
    '''

    # Clean the entire blotter before initializing a new one
    blotter.remove()

    # Add cash to blotter
    blotter.insert_one({'Cash': 100000.00})

    # Define the PL/Portfolio dataframe
    portfolio = pd.DataFrame(0, index=[], columns=['Inventory',
                                                   'Last Price',
                                                   'WAP',
                                                   'UPL',
                                                   'RPL',
                                                   'Total PL',
                                                   'By Shares',
                                                   'By Value'])

    return portfolio

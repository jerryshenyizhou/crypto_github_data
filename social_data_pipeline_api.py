# coding: utf-8

# In[2]:


import datetime
import json
import warnings

import pandas as pd
import requests
from pandas.io.json import json_normalize

warnings.filterwarnings('ignore')
import sys

reload(sys)
sys.setdefaultencoding('utf8')
sys.path.append('./utils')
from google_sheet_update import *


# In[3]:


# utility function, http request
def getjson(url, auth=0):
    if auth == 0:
        with open('./utils/secret.json') as json_file:
            secret = json.load(json_file)
        auth = (str(secret['github']['username']), str(secret['github']['password']))
    else:
        pass
    header = {'x-requested-with': 'XMLHttpRequest'}
    mainPage = requests.get(url, auth=auth)
    data = mainPage.json()
    return data


# In[4]:


# ingest coin github org
def coin_github_org_ingestion():
    sheet_key = '1tpOAiuRo9RNKnyPCVTGjc3H9S1miIJD1AimFLg8sv4E'
    tab = 'organization'
    data = get_googlesheet_data(sheet_key, tab)
    return data


# ingest coinmarketcap data
def coin_marketcap_ingestion(limit=200):
    data = json_normalize(getjson("https://api.coinmarketcap.com/v1/ticker/?limit=" + str(limit)))
    return data


# write github org google sheet with coins that needs to be updated with github orgs
def update_no_org_coins(coin_github_org_data, coin_marketcap_data):
    coin_org_list = coin_github_org_data.symbol.unique()
    coin_total_list = coin_marketcap_data.symbol.unique()
    coin_gap_list = list(set(coin_total_list) - set(coin_org_list))
    coin_gap_list_df = coin_marketcap_data[coin_marketcap_data.symbol.isin(coin_gap_list)][['symbol', 'id']]
    sheet_key = '1tpOAiuRo9RNKnyPCVTGjc3H9S1miIJD1AimFLg8sv4E'
    tab = 'undocumented_top_200_coins'
    cell_col = 'A'
    cell_row = 1
    write_cells(coin_gap_list_df, sheet_key, tab, cell_col, cell_row, transpose=0)
    return coin_gap_list


# getting crypto_compare data
def coin_technical_data_ingestion(coin_list):
    coin_list_data = getjson('https://www.cryptocompare.com/api/data/coinlist/')['Data']
    coin_technical_data = pd.DataFrame()
    for coin in coin_list:
        try:
            coin_technical_data = coin_technical_data.append(json_normalize(coin_list_data[coin]))
            print 'flatterning technical data success ' + str(coin)
        except:
            print 'flatterning technical data failed ' + str(coin)
    coin_technical_data.columns = coin_technical_data.columns.str.lower()
    return coin_technical_data


def coin_social_data_ingestion(coin_technical_data, source='Reddit'):
    coin_social_data = pd.DataFrame()
    for coin_id in coin_technical_data['id']:
        try:
            coin = coin_technical_data[coin_technical_data.id == coin_id].symbol[0]
            coin_social = json_normalize(
                getjson('https://www.cryptocompare.com/api/data/socialstats/?id=' + str(coin_id))['Data'][source])
            coin_social['symbol'] = coin
            coin_social_data = coin_social_data.append(coin_social)
            print source + ' social data ' + coin + ' completed!'
        except:
            print source + ' social data ' + coin + ' failed!'
    coin_social_data['date'] = datetime.date.today()
    return coin_social_data


print 'start social data pipeline! UTC time: ' + str(datetime.datetime.today())

coin_github_org_data = coin_github_org_ingestion()
coin_marketcap_data = coin_marketcap_ingestion()
coin_gap_list = update_no_org_coins(coin_github_org_data, coin_marketcap_data)
coin_technical_data = coin_technical_data_ingestion(coin_marketcap_data['symbol'])
coin_reddit_data = coin_social_data_ingestion(coin_technical_data, source='Reddit')

today = datetime.date.today()
coin_reddit_data_existing = pd.DataFrame.from_csv('./data/latest_data/coin_social_data_reddit.csv')
coin_reddit_data_existing.date = pd.to_datetime(coin_reddit_data_existing.date)
coin_reddit_data_new = coin_reddit_data_existing[coin_reddit_data_existing.date < today].append(coin_reddit_data)

pd.DataFrame.to_csv(coin_reddit_data_new, './data/latest_data/coin_social_data_reddit.csv')

print 'finished social data pipeline UTC time: ' + str(datetime.datetime.today())

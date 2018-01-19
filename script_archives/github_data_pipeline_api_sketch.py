# coding: utf-8

# In[10]:


import datetime
import json
import sys
import time
import warnings

import pandas as pd
import requests
from pandas.io.json import json_normalize

warnings.filterwarnings('ignore')
reload(sys)
sys.setdefaultencoding('utf8')

# In[11]:


with open('./utils/secret.json') as json_file:
    secret = json.load(json_file)

# In[19]:

print 'start ingesting coin market data'


def getjson(url, auth=(str(secret['github']['username']), str(secret['github']['password']))):
    start_time = time.time()
    header = {'x-requested-with': 'XMLHttpRequest'}
    mainPage = requests.get(url, auth=auth)
    data = mainPage.json()
    return data


print 'finished ingesting coin market data'
# In[5]:


# get CMC Symbol Info from API
print 'start ingesting coin technical data'
try:
    data_token_list = getjson("https://api.coinmarketcap.com/v1/ticker/?limit=1500")
except ValueError:
    print "CMC API Not Responding"

data_token_list = json_normalize(data_token_list)
pd.DataFrame.to_csv(data_token_list, './data/data_top_token_list.csv')
pd.DataFrame.to_csv(data_token_list[0:100], './data/data_top_100_token_list.csv')
print 'finished ingesting coin technical data'

# In[6]:


# getting coinlist from coin marketcap
coin_list_data = getjson('https://www.cryptocompare.com/api/data/coinlist/')['Data']
with open('./data/coinmarketcap_coinlist.txt', 'w') as outfile:
    json.dump(coin_list_data, outfile)

coin_list_data_flatten = pd.DataFrame()
for coin in coin_list_data.keys():
    coin_list_data_flatten = coin_list_data_flatten.append(json_normalize(coin_list_data[coin]))

coin_list_data_flatten = coin_list_data_flatten.set_index('Symbol')
pd.DataFrame.to_csv(coin_list_data_flatten, './data/coinmarketcap_coinlist.csv')
print 'finished ingesting coin technical data'

# In[7]:


# # long execution, getting the github org name from cryptocompare, this only gets 50% of the coins githubs

# coin_github_org = pd.DataFrame()
# for coin_id in coin_list_data_flatten.Id.unique():
#     try:
#         coin = coin_list_data_flatten[coin_list_data_flatten.Id==coin_id].index[0]
#         data_coin_social = getJSON('https://www.cryptocompare.com/api/data/socialstats/?id='+coin_id)
#         coin_org = json_normalize(data_coin_social['Data']['CodeRepository']['List']).url.str.split('/', expand=True)[3].unique()
#         coin_org_df = pd.DataFrame(data=coin_org, columns=['github_org'])
#         coin_org_df['Symbol'] = coin
#         coin_github_org = coin_github_org.append(coin_org_df)
#     except:
#         pass
# pd.DataFrame.to_csv(coin_github_org, './data/coin_github_org_list.csv')
# coin_github_org = coin_github_org.rename(columns={'Symbol':'symbol'})


# In[8]:


# manual check for github org for top 100 coins, and merge with cryptocompare data, to get a more complete list
token_list = {'XRP': 'ripple', 'DASH': 'dashpay', 'XEM': 'NemProject', 'MIOTA': 'iotaledger', 'NEO': 'neo-project',
              'XMR': 'monero-project', 'OMG': 'omise', 'BCC': 'bitconnectcoin', 'BTC': 'bitcoin',
              'ETH': 'ethereum', 'LTC': 'litecoin-project', 'QTUM': 'qtumproject', 'LSK': 'LiskHQ', 'ZEC': 'Zcash',
              'WAVES': 'WAVESPLATFORM', 'STRAT': 'stratisproject', 'STEEM': 'steemit', 'ADA': 'input-output-hk',
              'DCT': 'DECENTfoundation', 'RHOC': 'rchain', 'BTM': 'Bytom'}
# token list 1 is my manual crrection
token_list_1 = pd.DataFrame.from_dict(token_list, orient='index').reset_index().rename(
    columns={'index': 'symbol', 0: 'github_org'})

# token list 2 is what I scraped from social data of cryptocompare, its not very complete
token_list_2 = pd.DataFrame.from_csv('./data/blockchain_repo.csv', index_col=None)[['symbol', 'github_org']]
token_list_3 = pd.DataFrame.from_csv('./data/coin_github_org_list.csv')
token_list_3 = token_list_3[token_list_3.github_org <> 'petertodd']

token_list_complete = token_list_2.append(token_list_1).append(token_list_3)
token_list_complete['github_org'] = token_list_complete['github_org'].str.lower()
token_list_complete = token_list_complete.drop_duplicates()
pd.DataFrame.to_csv(token_list_complete, './data/blockchain_org_list.csv')
print 'finished ingesting coin github org data!'

# get token repo list from github org name top 100 token
start_time = datetime.datetime.today()
today = datetime.date.today()
trunc_date = datetime.datetime(2016, 7, 1)
token_org_df = pd.DataFrame.from_csv('./data/blockchain_org_list.csv').dropna().set_index('symbol')
data_top_100_token_list = pd.DataFrame.from_csv('./data/data_top_100_token_list.csv')
token_repo_df = pd.DataFrame()

# use this for all coins, long execution
# for symbol in token_org_df.index.unique():
# use this for top 100 coins, shorter execution
for symbol in data_top_100_token_list.symbol.unique():
    for github_org in list(token_org_df[token_org_df.index == symbol].github_org):
        try:
            data_repo = getjson(
                "https://api.github.com/users/" + str(github_org) + "/repos?sort=updated&direction=desc&per_page=100")
            repo_dict = json_normalize(data_repo).set_index('name')
            repo_dict['updated_at'] = pd.to_datetime(repo_dict['updated_at'])
            repo_dict['symbol'] = symbol
            repo_list = repo_dict[repo_dict.updated_at >= trunc_date].index
            token_repo_df = token_repo_df.append(repo_dict)
            print str(github_org) + ' completed!'
        except:
            print str(github_org) + ' failed!'
            pass

pd.DataFrame.to_csv(token_repo_df, './data/token_repo_dictionary_' + str(today) + '.csv')
minutes_passed = (datetime.datetime.today() - start_time).seconds / 60
print 'finished ingesting coin github repo data! used ' + str(minutes_passed) + ' minutes!'

# In[ ]:


# get full contribution list per repo


today = datetime.date.today()
start_time = datetime.datetime.today()
data_top_100_token_list = pd.DataFrame.from_csv('./data/data_top_100_token_list.csv')

token_repo_df = pd.DataFrame.from_csv('./data/token_repo_dictionary_' + str(today) + '.csv')
# token_repo_df.updated_at = pd.to_datetime(token_repo_df.updated_at)
# this filters for just top 100 tokens updated ones
# token_repo_df_select= token_repo_df[token_repo_df.symbol.isin(data_top_100_token_list.symbol)&
#                                     (token_repo_df.updated_at>=(datetime.datetime.today() - datetime.timedelta(days=14)))]

token_repo_df_select = token_repo_df[token_repo_df.symbol.isin(data_top_100_token_list.symbol)]

data_contributions_entry = pd.DataFrame()

for repo_name in token_repo_df_select.full_name.unique():
    try:
        data_repo_contributors = json_normalize(getjson(
            "https://api.github.com/repos/" + repo_name + "/stats/contributors?sort=total&direction=desc&per_page=100"))
        data_repo_contributors['repo_full_name'] = repo_name
        data_repo_contributors = \
        data_repo_contributors.dropna(subset=['author.login']).set_index(['repo_full_name', 'author.login'])[['weeks']]
        data_repo_contributors = data_repo_contributors.weeks.apply(pd.Series)
        data_repo_contributors = pd.DataFrame(data_repo_contributors.stack())[0].apply(pd.Series)
        data_contributions_entry = data_contributions_entry.append(data_repo_contributors)
        memory = (data_contributions_entry.memory_usage()).sum() / (1024 ** 2)
        minutes_passed = (datetime.datetime.today() - start_time).seconds / 60
        print 'repo ' + repo_name + ' flattern completed! used ' + str(
            minutes_passed) + ' minutes! ' + 'memory used ' + str(memory) + 'MB'
        del data_repo_contributors

    except:
        print 'repo ' + repo_name + ' flattern failed! used ' + str(
            minutes_passed) + ' minutes! ' + 'memory used ' + str(memory) + 'MB'
        pass

minutes_passed = (datetime.datetime.today() - start_time).seconds / 60
print 'finished ingesting coin contribution data! used ' + str(minutes_passed) + ' minutes!'

data_contributions_entry['w'] = pd.to_datetime(data_contributions_entry.w, unit='s')
data_contributions_entry = data_contributions_entry.reset_index().drop(['level_2'], axis=1)
data_contributions_entry = data_contributions_entry.rename(
    columns={'w': 'week', 'c': 'commits', 'a': 'additions', 'd': 'deletions', 'author.login': 'login'})

data_contributions_merge = data_contributions_entry.merge(
    token_repo_df[['full_name', 'symbol']].rename(columns={'full_name': 'repo_full_name'}), on='repo_full_name')
data_contributions_merge['uploaded_at'] = datetime.datetime.today()

minutes_passed = (datetime.datetime.today() - start_time).seconds / 60

print 'finished flatterning coin contribution data! used ' + str(minutes_passed) + ' minutes!'

pd.DataFrame.to_csv(data_contributions_merge, './data/top_coin_repo_contributions_entry' + str(today) + '.csv')
